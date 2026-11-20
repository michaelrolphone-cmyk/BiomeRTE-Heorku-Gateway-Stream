"""Grant-fenced routing for a replicated whole-site controller authority."""
import asyncio
import copy
import unittest

from aiohttp import ClientSession, WSMsgType, web

from biomerte.service_coordinator import Coordinator, grant_binding, manifest_revision
from biomerte.service_transport import ServiceAuthorities
from test_service_coordinator import Clock, heartbeat
from test_service_activation import service_manifest, readiness

PI, MAC, LOGICAL, ACTOR = "1" * 64, "2" * 64, "3" * 64, "4" * 64
SECRET = "controller-routing-fixture-secret"


def controller_manifest(**changes):
    values = dict(id="runtime", authority={
        "kind": "controller", "id": LOGICAL, "resources": [], "providers": [],
        "controller_aliases": [PI, MAC]})
    values.update(changes)
    return service_manifest(**values)


class ControllerManifestTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.coordinator = Coordinator(SECRET, max_lease_ttl_s=10, clock=self.clock,
                                       reserved_authorities=[PI, MAC, "5" * 64])

    def configure(self, spec):
        spec["revision"] = manifest_revision(spec)
        return self.coordinator.configure(spec)

    def test_empty_dynamic_scopes_require_explicit_controller_kind(self):
        spec = controller_manifest()
        self.configure(spec)
        self.assertEqual([], self.coordinator.services["runtime"]["manifest"]["authority"]["providers"])
        for kind in (None, "resource"):
            with self.subTest(kind=kind):
                invalid = controller_manifest(id="other")
                invalid["authority"].pop("controller_aliases")
                invalid["authority"]["id"] = "6" * 64
                if kind is None:
                    invalid["authority"].pop("kind")
                else:
                    invalid["authority"]["kind"] = kind
                with self.assertRaises(ValueError):
                    self.configure(invalid)

    def test_aliases_must_be_reserved_native_identities(self):
        for aliases in ([], [PI, PI], [PI, "f" * 64], [PI, "not-a-fingerprint"]):
            with self.subTest(aliases=aliases):
                spec = controller_manifest()
                spec["authority"]["controller_aliases"] = aliases
                with self.assertRaises(ValueError):
                    self.configure(spec)

    def test_logical_identity_cannot_be_a_native_alias(self):
        spec = controller_manifest()
        spec["authority"]["id"] = PI
        with self.assertRaises(ValueError):
            self.configure(spec)

    def test_two_runtime_groups_cannot_claim_the_same_native_alias(self):
        self.configure(controller_manifest())
        other = controller_manifest(id="other", authority={
            "kind": "controller", "id": "6" * 64, "resources": [], "providers": [],
            "controller_aliases": [MAC, "5" * 64]})
        with self.assertRaises(ValueError):
            self.configure(other)
        other["authority"]["controller_aliases"] = ["5" * 64]
        self.configure(other)

    def test_controller_aliases_are_forbidden_on_physical_resource_authority(self):
        spec = service_manifest()
        spec["authority"]["controller_aliases"] = [PI]
        with self.assertRaises(ValueError):
            self.configure(spec)

    def test_controller_runtime_cannot_declare_stateless_storage(self):
        with self.assertRaises(ValueError):
            self.configure(controller_manifest(state_policy="stateless"))

    def test_unknown_authority_kind_is_not_silently_treated_as_physical(self):
        spec = controller_manifest()
        spec["authority"]["kind"] = "typo"
        with self.assertRaises(ValueError):
            self.configure(spec)


class ControllerRuntimeRoutingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = Clock()
        self.coordinator = Coordinator(SECRET, max_lease_ttl_s=10, clock=self.clock,
                                       reserved_authorities=[PI, MAC])
        self.spec = controller_manifest()
        self.clock.advance(10)
        self.grant = heartbeat(self.coordinator, self.spec, "pi")["assignments"][0]
        self.binding = grant_binding(self.grant)
        heartbeat(self.coordinator, self.spec, "mac")
        self.head = None
        self.coordinator.checkpoint_head = lambda sid: self.head if sid == "runtime" else None
        self.services = ServiceAuthorities(self.coordinator, SECRET, timeout=.3)
        app = web.Application()
        self.coordinator.mount(app)
        self.services.mount(app)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.addAsyncCleanup(self.runner.cleanup)
        self.addAsyncCleanup(self.services.close)
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.base = "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
        self.client = ClientSession()
        self.addAsyncCleanup(self.client.close)
        self.headers = {"Authorization": "Bearer " + self.coordinator.key}
        self.call_path = "/api/controller-runtimes/" + LOGICAL + "/call"

    async def socket(self, binding=None):
        binding = binding or self.binding
        socket = await self.client.ws_connect(self.base + "/service-authority/runtime/connect",
                                              headers=self.headers)
        self.addAsyncCleanup(socket.close)
        await socket.send_json({"type": "register", "binding": binding})
        self.assertEqual("registered", (await socket.receive_json(timeout=1))["type"])
        return socket

    def active(self, grant=None, worker="pi"):
        grant = grant or self.grant
        self.head = dict(service_id="runtime", sequence=1, digest="a" * 64,
                         binding=grant_binding(grant), boot_epoch=self.coordinator.boot_epoch)
        return heartbeat(self.coordinator, self.spec, worker, grant=grant,
                         healthy=True, **readiness(grant))["assignments"][0]

    def envelope(self, controller=PI, operation="assistant.status", **arguments):
        value = dict(site="test-site", realm="site", operation=operation,
                     arguments=arguments, id="original-request",
                     auth={"actor": ACTOR, "signature": "opaque-original-native-signature"})
        if controller is not None:
            value["controller"] = controller
        return value

    async def post(self, path, value, headers=None):
        return await self.client.post(self.base + path, json=value, headers=headers or {})

    async def answer(self, socket, request, result=None):
        await socket.send_json(dict(type=request["type"] + "_result", id=request["id"],
            binding=request["binding"], status=200,
            result=result if result is not None else {"state": "pause"}))

    async def forwarded(self, socket, envelope):
        pending = asyncio.create_task(self.post(self.call_path, envelope))
        request = await socket.receive_json(timeout=1)
        self.assertEqual(envelope, request["envelope"])
        self.assertFalse(request["preparation"])
        await self.answer(socket, request)
        response = await pending
        self.assertEqual(200, response.status)
        return await response.json()

    async def test_both_aliases_resolve_to_the_same_current_logical_authority(self):
        await self.socket()
        self.active()
        for alias in (PI, MAC):
            response = await self.post("/api/service-authorities/resolve", {"controller": alias})
            value = await response.json()
            self.assertEqual(200, response.status)
            self.assertEqual(LOGICAL, value["authority"])
            self.assertEqual(self.binding, value["binding"])
            self.assertEqual(self.call_path, value["call_path"])
            self.assertEqual("controller", value["authority_kind"])
            self.assertEqual([PI, MAC], value["controller_aliases"])

    async def test_alias_and_provider_envelopes_are_forwarded_without_rewriting(self):
        socket = await self.socket()
        self.active()
        for controller in (PI, MAC, LOGICAL, None):
            await self.forwarded(socket, self.envelope(controller, id="worker"))
        self.assertIsNone(self.services.scope(provider=ACTOR))
        self.assertEqual([], self.spec["authority"]["providers"])

    async def test_full_site_operations_and_dynamic_resources_reach_native_authorization(self):
        socket = await self.socket()
        self.active()
        for operation, arguments in (
                ("work.list", {}),
                ("process.start", {"name": "native-process", "inputs": {}}),
                ("cap.acquire", {"contract": "workspace.exec:1", "resource": "new.dynamic.resource", "task": "t"}),
                ("provider.poll", {"provider": ACTOR})):
            await self.forwarded(socket, self.envelope(None, operation, **arguments))

    async def test_ordered_original_request_identity_is_preserved(self):
        socket = await self.socket()
        self.active()
        envelope = self.envelope()
        envelope["id"] = {"stream": 3, "sequence": 9}
        await self.forwarded(socket, envelope)

    async def test_wrong_alias_local_realm_and_unsigned_body_are_rejected(self):
        await self.socket()
        self.active()
        invalid = []
        invalid.append(self.envelope("f" * 64))
        local = self.envelope()
        local["realm"] = "local"
        invalid.append(local)
        unsigned = self.envelope()
        unsigned.pop("auth")
        invalid.append(unsigned)
        changed = self.envelope()
        changed["authority_override"] = LOGICAL
        invalid.append(changed)
        ordered = self.envelope()
        ordered["id"] = {"stream": True, "sequence": 1}
        invalid.append(ordered)
        for value in invalid:
            response = await self.post(self.call_path, value)
            self.assertEqual(400, response.status)
            await response.read()
        self.assertEqual({}, self.services.pending)

    async def test_preparing_runtime_never_admits_ordinary_or_provider_calls(self):
        await self.socket()
        for envelope in (self.envelope(), self.envelope(None, "provider.poll", provider=ACTOR)):
            response = await self.post(self.call_path, envelope)
            self.assertEqual(503, response.status)
            await response.read()
        response = await self.post("/api/service-authorities/resolve", {"controller": PI})
        self.assertEqual(503, response.status)
        await response.read()

    async def test_managed_runtime_unavailability_does_not_fall_back_to_native(self):
        socket = await self.socket()
        self.active()
        await socket.close()
        for path, value in ((self.call_path, self.envelope()),
                            ("/api/service-authorities/resolve", {"controller": MAC})):
            response = await self.post(path, value)
            self.assertEqual(503, response.status)
            await response.read()
        self.clock.advance(600)
        response = await self.post("/api/service-authorities/resolve", {"controller": PI})
        self.assertEqual(503, response.status)
        await response.read()

    async def test_old_owner_reply_is_rejected_and_alias_follows_replacement(self):
        old = await self.socket()
        self.active()
        pending = asyncio.create_task(self.post(self.call_path, self.envelope()))
        request = await old.receive_json(timeout=1)
        self.clock.advance(6)
        new_grant = heartbeat(self.coordinator, self.spec, "mac")["assignments"][0]
        self.active(new_grant, "mac")
        await self.answer(old, request, {"state": "stale"})
        response = await pending
        self.assertEqual(503, response.status)
        await response.read()
        new = await self.socket(grant_binding(new_grant))
        response = await self.post("/api/service-authorities/resolve", {"controller": PI})
        self.assertEqual(grant_binding(new_grant), (await response.json())["binding"])
        await self.forwarded(new, self.envelope(MAC))
        resumed = await self.client.ws_connect(self.base + "/service-authority/runtime/connect",
                                               headers=self.headers)
        await resumed.send_json({"type": "register", "binding": self.binding})
        self.assertIn((await resumed.receive(timeout=1)).type, (WSMsgType.CLOSE, WSMsgType.CLOSED))
        await resumed.close()

    async def test_physical_endpoint_cannot_silently_adopt_runtime_semantics(self):
        await self.socket()
        self.active()
        response = await self.post("/api/service-authorities/call", self.envelope(LOGICAL))
        self.assertEqual(404, response.status)
        await response.read()
        response = await self.post("/api/controller-runtimes/" + "f" * 64 + "/call", self.envelope())
        self.assertEqual(404, response.status)
        await response.read()

    async def test_pairing_begin_and_finish_preserve_native_pending_only_protocol(self):
        socket = await self.socket()
        self.active()
        for operation, payload in (
                ("begin", {"public_key": "opaque-native-public-key", "label": "new provider"}),
                ("finish", {"proof": {"key": ACTOR, "signature": "signed-native-proof"}})):
            path = "/api/controller-runtimes/" + LOGICAL + "/pair/" + operation
            pending = asyncio.create_task(self.post(path, payload))
            request = await socket.receive_json(timeout=1)
            self.assertEqual("pair", request["type"])
            self.assertEqual(operation, request["operation"])
            self.assertEqual(payload, request["payload"])
            self.assertFalse(request["preparation"])
            await self.answer(socket, request, {"state": "pending"})
            self.assertEqual({"result": {"state": "pending"}}, await (await pending).json())
        for operation, payload in (("approve", {"key": ACTOR}),
                                   ("begin", {"public_key": "key", "label": "x", "approved": True})):
            response = await self.post("/api/controller-runtimes/" + LOGICAL + "/pair/" + operation, payload)
            self.assertEqual(400, response.status)
            await response.read()

    async def test_pairing_reply_from_expired_owner_is_discarded(self):
        socket = await self.socket()
        self.active()
        path = "/api/controller-runtimes/" + LOGICAL + "/pair/begin"
        pending = asyncio.create_task(self.post(path, {"public_key": "key", "label": "pending"}))
        request = await socket.receive_json(timeout=1)
        self.clock.advance(6)
        heartbeat(self.coordinator, self.spec, "mac")
        await self.answer(socket, request, {"challenge": "stale"})
        response = await pending
        self.assertEqual(503, response.status)
        await response.read()

    async def test_browser_origin_is_rejected_on_runtime_and_pairing_routes(self):
        for path, payload in ((self.call_path, self.envelope()),
            ("/api/controller-runtimes/" + LOGICAL + "/pair/begin", {"public_key": "key", "label": "x"})):
            response = await self.post(path, payload, {"Origin": "https://untrusted.example"})
            self.assertEqual(403, response.status)
            await response.read()


if __name__ == "__main__":
    unittest.main()
