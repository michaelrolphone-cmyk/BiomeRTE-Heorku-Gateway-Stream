import base64
import hashlib
import hmac
import json
import time
import unittest
import io
from unittest.mock import patch

from biomerte.stream_router import (
    Router, Fault, credential, service_authority_key, service_routes)
from biomerte.service_inspection import add_services


NATIVE, LOGICAL = "1" * 64, "2" * 64


class ServiceRouterTests(unittest.IsolatedAsyncioTestCase):
    async def test_native_config_adds_authenticated_service_inspection_without_changing_native_entries(self):
        secrets = {NATIVE: "native-secret"}
        validate = lambda sid: {"id": sid}
        validators = {NATIVE: validate}
        self.assertEqual((secrets, validators), add_services({}, secrets, validators))
        with self.assertRaises(ValueError):
            add_services({"service_authority_secret": "seed", "service_inspect_url": "http://bad"},
                         secrets, validators)
        mapped, routes = add_services({"service_authority_secret": "seed",
            "service_inspect_url": "https://gateway.example/stream/inspect"}, secrets, validators)
        self.assertIs(validate, routes[NATIVE])
        result = dict(id="c" * 32, authority=LOGICAL, state="open")
        response = io.BytesIO(json.dumps(result).encode())
        with patch("urllib.request.urlopen", return_value=response) as call:
            self.assertEqual(result, routes.get(LOGICAL)(result["id"]))
            request = call.call_args[0][0]
            body = json.loads(request.data)
            self.assertIs(type(body["timestamp"]), int)
            self.assertEqual(LOGICAL, body["authority"])
            signature = hmac.new(mapped[LOGICAL].encode(), request.data, hashlib.sha256).hexdigest()
            self.assertEqual("Router " + signature, request.headers["Authorization"])

    async def test_dynamic_logical_token_still_requires_current_inspection_and_native_path_is_preserved(self):
        inspected = []
        native = dict(id="a" * 32, authority=NATIVE, state="open")
        logical = dict(id="b" * 32, authority=LOGICAL, state="open")
        def inspect(authority, session):
            inspected.append((authority, session))
            if authority != LOGICAL or logical["state"] != "open":
                raise ConnectionError("Current logical authority unavailable")
            return dict(logical)
        secrets, validators = service_routes(
            {NATIVE: "native-secret"}, {NATIVE: lambda sid: dict(native)}, "service-seed", inspect)
        router = Router(secrets, validators)
        self.assertEqual("native-secret", secrets[NATIVE])
        self.assertEqual(native, await router.validate(native))
        self.assertEqual([], inspected)
        self.assertEqual(logical, await router.validate(logical))
        self.assertEqual([(LOGICAL, logical["id"])], inspected)
        token_grant = dict(logical, credential_expires_at=time.time()+30,
                           expires_at=time.time()+30, role="provider")
        raw = json.dumps(token_grant).encode()
        signature = hmac.new(service_authority_key("service-seed", LOGICAL).encode(),
                             raw, hashlib.sha256).hexdigest()
        token = base64.urlsafe_b64encode(raw).decode().rstrip("=") + "." + signature
        self.assertEqual(token_grant, credential(token, secrets))
        logical["state"] = "revoked"
        with self.assertRaises(ConnectionError):
            await router.validate(token_grant)
        with self.assertRaises(ConnectionError):
            await router.validate(dict(logical, authority="3" * 64))
        with self.assertRaises(Fault):
            credential(token + "wrong", secrets)
        # Dynamic lookups do not accumulate a journal of arbitrary IDs.
        self.assertEqual([NATIVE], list(secrets))


if __name__ == "__main__":
    unittest.main()
