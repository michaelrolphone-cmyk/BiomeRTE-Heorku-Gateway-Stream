import hashlib
import hmac
import json
import pathlib
import tempfile
import unittest
from unittest.mock import patch

from biomerte.heroku_stream_router import build_config, main
from biomerte.service_coordinator import coordinator_key
from biomerte.witness_deployment import (
    FORMAT, FILE_NAME, canonical_origin, derive_witness_secret,
    deployment_witness_config,
)

PARENT = "test-only-stream-parent-" + "x" * 40
TOKEN = coordinator_key(PARENT)
ORIGIN = "https://gateway.example"
IDENTITY = "biomerte-services"


class WitnessDeploymentTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = pathlib.Path(self.temporary.name)
        self.path = self.directory / FILE_NAME
        self.public = dict(format=FORMAT, origin=ORIGIN, witness_id=IDENTITY, lease_s=120)
        (self.directory / "controllers.json").write_text(json.dumps({
            "authorities": ["a" * 64], "inspect_url": "https://gateway.example/inspect"}))
        self.environ = {"STREAM_ROUTER_SECRET": PARENT}

    def write(self, value=None):
        self.path.write_text(json.dumps(self.public if value is None else value))

    def config(self, environ=None):
        return build_config(self.environ if environ is None else environ,
                            deployment_dir=self.directory)

    def test_missing_file_and_no_witness_settings_preserve_legacy(self):
        result = self.config()
        self.assertFalse(any(key.startswith("durable_witness_") for key in result))
        self.assertEqual({"a" * 64: PARENT}, result["secrets"])

    def test_pinned_public_file_derives_exact_separate_credential(self):
        self.write()
        result = self.config()
        message = b"biomerte/durable-witness/deployment/v1\x00https://gateway.example\x00biomerte-services"
        expected = hmac.new(TOKEN.encode("ascii"), message, hashlib.sha256).hexdigest()
        self.assertEqual(expected, result["durable_witness_secret"])
        self.assertEqual(ORIGIN, result["durable_witness_origin"])
        self.assertEqual(IDENTITY, result["durable_witness_id"])
        self.assertEqual(120, result["durable_witness_lease_s"])
        self.assertNotEqual(TOKEN, expected)
        self.assertNotEqual(PARENT, expected)
        self.assertNotIn(TOKEN, self.path.read_text())
        self.assertNotIn(expected, self.path.read_text())

    def test_derivation_is_bound_to_parent_origin_and_identity(self):
        original = derive_witness_secret(TOKEN, ORIGIN, IDENTITY)
        variants = (
            derive_witness_secret("a" * 64, ORIGIN, IDENTITY),
            derive_witness_secret(TOKEN, "https://another.example", IDENTITY),
            derive_witness_secret(TOKEN, ORIGIN, "another-witness"),
        )
        self.assertRegex(original, r"^[0-9a-f]{64}$")
        self.assertEqual(4, len(set((original,) + variants)))
        with self.assertRaises(ValueError):
            derive_witness_secret(PARENT, ORIGIN, IDENTITY)

    def test_explicit_separate_secret_overrides_derivation(self):
        self.write()
        explicit = "test-only-explicit-witness-" + "z" * 40
        result = self.config(dict(self.environ, DURABLE_WITNESS_SECRET=explicit))
        self.assertEqual(explicit, result["durable_witness_secret"])
        for credential in ("", "short", PARENT, TOKEN):
            with self.subTest(credential_length=len(credential)):
                with self.assertRaises(ValueError):
                    self.config(dict(self.environ, DURABLE_WITNESS_SECRET=credential))

    def test_environment_identity_or_lease_conflict_is_fatal(self):
        self.write()
        for setting in (
                {"DURABLE_WITNESS_ORIGIN": "https://another.example"},
                {"DURABLE_WITNESS_ID": "another-witness"},
                {"DURABLE_WITNESS_LEASE_S": "121"}):
            with self.subTest(setting=setting):
                with self.assertRaisesRegex(ValueError, "conflict"):
                    self.config(dict(self.environ, **setting))
        result = self.config(dict(self.environ, DURABLE_WITNESS_ORIGIN=ORIGIN,
            DURABLE_WITNESS_ID=IDENTITY, DURABLE_WITNESS_LEASE_S="120"))
        self.assertEqual(derive_witness_secret(TOKEN, ORIGIN, IDENTITY),
                         result["durable_witness_secret"])

    def test_invalid_present_file_cannot_fall_back_to_environment(self):
        env = dict(self.environ, DURABLE_WITNESS_ORIGIN=ORIGIN,
                   DURABLE_WITNESS_SECRET="test-only-override-" + "z" * 40)
        cases = [
            {}, dict(self.public, secret="must-not-be-in-repository"),
            dict(self.public, format="unknown"), dict(self.public, witness_id="bad/id"),
            dict(self.public, origin=ORIGIN + "/"), dict(self.public, lease_s=True),
            dict(self.public, lease_s=59), dict(self.public, lease_s=601),
            dict(self.public, lease_s=float("nan")),
        ]
        for value in cases:
            with self.subTest(fields=sorted(value)):
                self.write(value)
                with self.assertRaisesRegex(ValueError, "deployment configuration"):
                    self.config(env)
        for raw in (b"{", b"\xff", b" " * 16385,
                    b'{"format":"a","format":"b"}'):
            self.path.write_bytes(raw)
            with self.assertRaisesRegex(ValueError, "deployment configuration"):
                self.config(env)

    def test_origin_is_canonical_bare_https(self):
        self.assertEqual(ORIGIN, canonical_origin(ORIGIN))
        for origin in ("http://gateway.example", ORIGIN + "/", ORIGIN + "/path",
                       ORIGIN + "?q=1", ORIGIN + "#fragment",
                       "https://user:password@gateway.example",
                       "https://GATEWAY.example", "https://gateway.example:443",
                       "https://gateway.example:0", "https://gateway.example\n",
                       "https://", "https://gateway.example:bad"):
            with self.subTest(origin=origin):
                with self.assertRaises(ValueError):
                    canonical_origin(origin)

    def test_environment_only_keeps_explicit_credential_contract(self):
        explicit = "test-only-environment-witness-" + "z" * 40
        result = self.config(dict(self.environ, DURABLE_WITNESS_ORIGIN=ORIGIN,
                                  DURABLE_WITNESS_SECRET=explicit))
        self.assertEqual(explicit, result["durable_witness_secret"])
        self.assertEqual(120, result["durable_witness_lease_s"])
        for setting in ({"DURABLE_WITNESS_ORIGIN": ORIGIN},
                        {"DURABLE_WITNESS_SECRET": explicit},
                        {"DURABLE_WITNESS_ID": IDENTITY},
                        {"DURABLE_WITNESS_LEASE_S": "120"}):
            with self.subTest(keys=sorted(setting)):
                with self.assertRaises(ValueError):
                    self.config(dict(self.environ, **setting))

    def test_empty_or_invalid_explicit_public_settings_are_fatal(self):
        self.write()
        for setting in ({"DURABLE_WITNESS_ORIGIN": ""},
                        {"DURABLE_WITNESS_ID": ""},
                        {"DURABLE_WITNESS_LEASE_S": ""},
                        {"DURABLE_WITNESS_LEASE_S": "nan"},
                        {"DURABLE_WITNESS_LEASE_S": "inf"}):
            with self.subTest(keys=sorted(setting)):
                with self.assertRaises(ValueError):
                    self.config(dict(self.environ, **setting))

    def test_invalid_file_stops_main_before_server_construction(self):
        self.path.write_text("{}")
        with patch.dict("os.environ", self.environ, clear=True), \
                patch("biomerte.heroku_stream_router.REPOSITORY_ROOT", self.directory), \
                patch("biomerte.heroku_stream_router.serve") as serve, \
                patch("biomerte.heroku_stream_router.asyncio.run") as run:
            with self.assertRaises(SystemExit) as failure:
                main()
            self.assertNotIn(PARENT, str(failure.exception))
            self.assertNotIn(TOKEN, str(failure.exception))
            serve.assert_not_called()
            run.assert_not_called()


if __name__ == "__main__":
    unittest.main()
