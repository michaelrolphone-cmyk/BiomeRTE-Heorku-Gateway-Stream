import ast
import io
import json
import pathlib
import tempfile
import unittest
import urllib.error
import urllib.request
import uuid
from unittest.mock import Mock, patch

from biomerte.service_routing import ServiceOwnerResolver
from mac.rover_routes import ControllerRoutes, ResourceMoved


MAC = "1" * 64
PI = "2" * 64
LOGICAL = "3" * 64
CONTRACT = "test.stream:1"


class Reply(io.BytesIO):
    def __enter__(self):
        return self
    def __exit__(self, *args):
        self.close()


def runtime_class(name):
    # Execute the actual deployed routing adapter with explicit native
    # collaborators. Its native controller runtime is not vendored here.
    source = (pathlib.Path(__file__).parent.parent / "mac/rover_service.py").read_text()
    tree = ast.parse(source)
    node = next(item for item in tree.body if isinstance(item, ast.ClassDef) and item.name == name)
    namespace = dict(read_json=lambda path: {"node": "4" * 64, "site": "site"},
        ServiceOwnerResolver=ServiceOwnerResolver, PUBLIC="https://gateway.example",
        GATEWAY="https://site.example", urllib=urllib, json=json, uuid=uuid, pathlib=pathlib,
        ControllerRoutes=ControllerRoutes, sign=lambda path, envelope: "signature")
    exec(compile(ast.Module(body=[node], type_ignores=[]), "mac/rover_service.py", "exec"), namespace)
    return namespace[name]


def catalog(resource):
    return {"contracts": [{"id": CONTRACT, "implementations": [
        {"resource": resource, "state": "ready", "admitted": True}]}]}


class NativeApp:
    node = MAC
    config = {"site": "site"}
    def __init__(self):
        self.calls = []
        self.offer = "native.vision"
    def call(self, operation, arguments, request_id, **options):
        self.calls.append((operation, arguments, options))
        if operation == "cap.discover":
            return catalog(self.offer)
        return {"id": "native-lease", "state": "released"}


class ResolverTests(unittest.TestCase):
    def setUp(self):
        self.resolver = ServiceOwnerResolver("https://gateway.example")
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.home = pathlib.Path(self.directory.name)
        candidates = patch("mac.rover_routes.controller_candidates", return_value=(MAC, PI))
        candidates.start()
        self.addCleanup(candidates.stop)

    def test_only_unmanaged_404_permits_native_resolution(self):
        def error(code):
            return urllib.error.HTTPError(self.resolver.url, code, "test", {}, None)
        with patch("urllib.request.urlopen", side_effect=error(404)):
            self.assertIsNone(self.resolver("resource"))
        for status in (401, 409, 429, 500, 503):
            with patch("urllib.request.urlopen", side_effect=error(status)):
                with self.assertRaises(ConnectionError):
                    self.resolver("resource")

    def test_validates_logical_authority_and_effect_readiness(self):
        good = dict(authority=LOGICAL, effect_ready=True, state="active")
        for value in (dict(good, effect_ready=False), dict(good, state="awaiting_external_fence"),
                      dict(good, authority="bad"), []):
            with patch("urllib.request.urlopen", return_value=Reply(json.dumps(value).encode())):
                with self.assertRaises(ConnectionError):
                    self.resolver("resource")
        with patch("urllib.request.urlopen", return_value=Reply(json.dumps(good).encode())) as call:
            self.assertEqual(LOGICAL, self.resolver("resource"))
            self.assertEqual({"resource": "resource"}, json.loads(call.call_args[0][0].data))

    def test_logical_first_and_existing_native_and_logical_lease_origins_are_pinned(self):
        app = NativeApp()
        lookups, sent = [], []
        owner = [LOGICAL]
        def resolve(resource):
            lookups.append(resource)
            return owner[0]
        remote = runtime_class("RoutedRemote")(app, self.home, resolver=resolve)
        counter = [0]
        def transport(request, **options):
            envelope = json.loads(request.data)
            sent.append(envelope)
            if envelope["operation"] == "cap.discover":
                result = catalog("relay")
            else:
                counter[0] += 1
                result = {"id": "id" + str(counter[0]), "state": "released"}
            return Reply(json.dumps({"result": result}).encode())
        with patch("urllib.request.urlopen", side_effect=transport):
            lease = remote.call("cap.acquire", {"contract": CONTRACT, "resource": "relay"})
            self.assertEqual(LOGICAL, remote.leases[lease["id"]])
            owner[0] = None
            session = remote.call("stream.open", {"lease": lease["id"]})
            self.assertEqual(LOGICAL, remote.sessions[session["id"]])
            remote.call("stream.close", {"session": session["id"]})
            remote.call("cap.release", {"lease": lease["id"]})
            self.assertEqual(["relay"], lookups)
            self.assertTrue(all(envelope["controller"] == LOGICAL for envelope in sent))
            self.assertNotIn(lease["id"], remote.leases)
            self.assertNotIn(session["id"], remote.sessions)
            remote.leases["before-cutover"] = PI
            remote.call("cap.release", {"lease": "before-cutover"})
            self.assertEqual(PI, sent[-1]["controller"])
            remote.call("cap.acquire", {"contract": CONTRACT, "resource": "relay"})
            self.assertEqual(PI, sent[-1]["controller"])
        self.assertTrue(all(operation == "cap.discover" for operation, _, _ in app.calls))

    def test_unmanaged_local_resources_use_existing_native_offer_discovery(self):
        app = NativeApp()
        remote = runtime_class("RoutedRemote")(app, self.home, resolver=lambda resource: None)
        result = remote.call("cap.acquire", {"contract": CONTRACT, "resource": "native.vision"})
        self.assertEqual("native-lease", result["id"])
        self.assertEqual(["cap.discover", "cap.acquire"], [call[0] for call in app.calls])
        self.assertEqual(MAC, app.calls[-1][2]["controller"])

    def test_unavailable_managed_resource_never_queries_native_offers(self):
        app = NativeApp()
        def unavailable(resource):
            raise ConnectionError("Declared service unavailable")
        remote = runtime_class("RoutedRemote")(app, self.home, resolver=unavailable)
        with patch("urllib.request.urlopen", side_effect=AssertionError("No fallback")):
            with self.assertRaises(ConnectionError):
                remote.call("cap.acquire", {"contract": CONTRACT, "resource": "relay"})
        self.assertEqual([], app.calls)

    def test_pending_submission_retains_origin_across_lost_response_and_restart(self):
        app = NativeApp()
        remote = runtime_class("RoutedRemote")(app, self.home, resolver=lambda resource: LOGICAL)
        arguments = {"contract": CONTRACT, "resource": "relay", "operation": "stable-operation"}
        with patch("urllib.request.urlopen", side_effect=urllib.error.URLError("reply lost")):
            with self.assertRaises(OSError):
                remote.call("cap.submit", arguments)
        resolver = Mock(side_effect=AssertionError("Existing command must not resolve again"))
        restarted = runtime_class("RoutedRemote")(app, self.home, resolver=resolver)
        with patch("urllib.request.urlopen",
                   return_value=Reply(json.dumps({"result": {"state": "working"}}).encode())) as send:
            restarted.call("cap.status", {"operation": "stable-operation"})
            self.assertEqual(LOGICAL, json.loads(send.call_args[0][0].data)["controller"])
        resolver.assert_not_called()

    def test_existing_native_error_checks_keep_native_resource_move_behavior(self):
        app = NativeApp()
        resolver = Mock(side_effect=AssertionError("Existing lease must not use service resolver"))
        remote = runtime_class("RoutedRemote")(app, self.home, resolver=resolver)
        remote.leases["old"] = PI
        remote.lease_resources["old"] = (CONTRACT, "relay")
        app.offer = "relay"  # existing native move detector now sees local offer
        with patch("urllib.request.urlopen", side_effect=urllib.error.HTTPError(
                "https://site.example", 503, "offline", {}, None)):
            with self.assertRaises(ResourceMoved):
                remote.call("stream.open", {"lease": "old"})
        self.assertEqual(PI, remote.leases["old"])
        resolver.assert_not_called()

    def test_local_authority_uses_running_api_and_preserves_signed_controller_scope(self):
        local = runtime_class("LocalAuthority")("/native", "/caller")
        auth = {"actor": "5" * 64, "signature": "original"}
        with patch("urllib.request.urlopen",
                   return_value=Reply(json.dumps({"result": {"id": "lease"}}).encode())) as send:
            result = local.call("cap.acquire", {"resource": "native.vision"}, "request",
                                auth=auth, controller=MAC)
            request = send.call_args[0][0]
            self.assertEqual("http://127.0.0.1:8740/v1/call", request.full_url)
            envelope = json.loads(request.data)
            self.assertEqual(auth, envelope["auth"])
            self.assertEqual(MAC, envelope["controller"])
            self.assertEqual({"id": "lease"}, result)


if __name__ == "__main__":
    unittest.main()
