import asyncio
import base64
import copy
import hashlib
import hmac
import json
import time
import unittest

from aiohttp import ClientSession, WSMsgType, web

from biomerte.authority_transport import Authorities
from biomerte.service_coordinator import Coordinator, grant_binding
from biomerte.service_transport import ServiceAuthorities, service_authority_key
from biomerte.checkpoint_transport import CheckpointWorkers
from test_service_coordinator import Clock, heartbeat
from test_service_activation import service_manifest, readiness, AUTHORITY, PROVIDER, RESOURCE


NATIVE = "1" * 64
CONSUMER = "2" * 64
SECRET = "test-gateway-shared-secret"


class ServiceTransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = Clock()
        self.coordinator = Coordinator(SECRET, max_lease_ttl_s=10, clock=self.clock,
                                       reserved_authorities=[NATIVE])
        self.spec = service_manifest()
        self.clock.advance(10)
        self.grant = heartbeat(self.coordinator, self.spec, "pi")["assignments"][0]
        self.binding = grant_binding(self.grant)
        heartbeat(self.coordinator, self.spec, "mac")
        self.services = ServiceAuthorities(self.coordinator, SECRET, timeout=.3)
        self.helpers = CheckpointWorkers(self.coordinator, timeout=.3)
        self.authorities = Authorities({NATIVE: SECRET}, services=self.services)
        app = web.Application()
        self.coordinator.mount(app)
        self.services.mount(app)
        self.helpers.mount(app)
        self.authorities.mount(app)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.addAsyncCleanup(self.runner.cleanup)
        self.addAsyncCleanup(self.services.close)
        self.addAsyncCleanup(self.helpers.close)
        self.addAsyncCleanup(self.authorities.close)
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.base = "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
        self.client = ClientSession()
        self.addAsyncCleanup(self.client.close)
        self.headers = {"Authorization": "Bearer " + self.coordinator.key}

    async def authority_socket(self, binding=None):
        binding = binding or self.binding
        socket = await self.client.ws_connect(self.base + "/service-authority/example/connect",
                                              headers=self.headers)
        self.addAsyncCleanup(socket.close)
        await socket.send_json({"type": "register", "binding": binding})
        message = await socket.receive_json(timeout=1)
        self.assertEqual("registered", message["type"])
        self.assertEqual(binding, message["binding"])
        return socket

    async def helper_socket(self, worker="mac", incarnation="boot1"):
        socket = await self.client.ws_connect(self.base + "/service-worker/" + worker + "/connect",
                                              headers=self.headers)
        self.addAsyncCleanup(socket.close)
        await socket.send_json(dict(type="register", worker_id=worker,
                                    worker_incarnation=incarnation))
        self.assertEqual("registered", (await socket.receive_json(timeout=1))["type"])
        return socket

    def active(self, grant=None, worker="pi"):
        grant = grant or self.grant
        self.helpers.heads["example"] = dict(service_id="example", sequence=1, digest="a" * 64,
            binding=grant_binding(grant), boot_epoch=self.coordinator.boot_epoch)
        return heartbeat(self.coordinator, self.spec, worker, grant=grant,
                         healthy=True, **readiness(grant))["assignments"][0]

    def envelope(self, operation="cap.acquire", *, provider=False, **arguments):
        value = dict(site="test-site", realm="site", operation=operation, arguments=arguments,
                     id="stable-request", auth=dict(actor=PROVIDER if provider else CONSUMER,
                                                   signature="signed-original-envelope"))
        if not provider:
            value["controller"] = AUTHORITY
        return value

    async def post(self, path, body, private=False):
        return await self.client.post(self.base + path, json=body,
                                      headers=self.headers if private else {})

    async def reply(self, socket, request, result=None):
        await socket.send_json(dict(type=request["type"] + "_result", id=request["id"],
            binding=request["binding"], status=200, result=result or {"id": "lease"}))

    async def test_preparation_only_for_provider_recovery_and_original_envelope_unchanged(self):
        socket = await self.authority_socket()
        for envelope in (self.envelope(resource=RESOURCE),
                         self.envelope("provider.claim", provider=True)):
            response = await self.post("/api/service-authorities/call", envelope)
            self.assertEqual(503, response.status)
            await response.read()
        envelope = self.envelope("provider.poll", provider=True, cursor="original")
        task = asyncio.create_task(self.post("/api/service-authorities/call", envelope))
        request = await socket.receive_json(timeout=1)
        self.assertEqual(envelope, request["envelope"])
        self.assertTrue(request["preparation"])
        self.assertEqual(self.binding, request["binding"])
        await self.reply(socket, request, {"fences": []})
        response = await task
        self.assertEqual(200, response.status)
        self.assertEqual({"result": {"fences": []}}, await response.json())
        response = await self.post("/api/service-authorities/resolve", {"provider": PROVIDER})
        self.assertFalse((await response.json())["effect_ready"])
        response = await self.post("/api/service-authorities/resolve", {"resource": RESOURCE})
        self.assertEqual(503, response.status)
        await response.read()
        self.active()
        task = asyncio.create_task(self.post("/api/service-authorities/call", envelope))
        request = await socket.receive_json(timeout=1)
        self.assertFalse(request["preparation"])
        await self.reply(socket, request)
        await (await task).read()

    async def test_resolver_distinguishes_unmanaged_and_unavailable_and_exact_scope(self):
        socket = await self.authority_socket()
        self.active()
        response = await self.post("/api/service-authorities/resolve", {"resource": RESOURCE})
        value = await response.json()
        self.assertEqual(200, response.status)
        self.assertEqual(AUTHORITY, value["authority"])
        self.assertEqual(self.binding, value["binding"])
        response = await self.post("/api/service-authorities/resolve", {"resource": "unmanaged"})
        self.assertEqual(404, response.status)
        await response.read()
        response = await self.post("/api/service-authorities/call", self.envelope(resource="unmanaged"))
        self.assertEqual(403, response.status)
        await response.read()
        await socket.close()
        response = await self.post("/api/service-authorities/resolve", {"resource": RESOURCE})
        self.assertEqual(503, response.status)
        await response.read()
        self.clock.advance(600)
        response = await self.post("/api/service-authorities/resolve", {"resource": RESOURCE})
        self.assertEqual(503, response.status)
        await response.read()

    async def test_late_old_owner_reply_is_discarded_and_resumed_socket_cannot_replace_owner(self):
        old = await self.authority_socket()
        self.active()
        task = asyncio.create_task(self.post("/api/service-authorities/call",
                                             self.envelope(resource=RESOURCE)))
        request = await old.receive_json(timeout=1)
        self.clock.advance(6)
        new_grant = heartbeat(self.coordinator, self.spec, "mac")["assignments"][0]
        self.active(new_grant, "mac")
        await self.reply(old, request, {"id": "stale-lease"})
        response = await task
        self.assertEqual(503, response.status)
        await response.read()
        new = await self.authority_socket(grant_binding(new_grant))
        resumed = await self.client.ws_connect(self.base + "/service-authority/example/connect",
                                               headers=self.headers)
        await resumed.send_json({"type": "register", "binding": self.binding})
        self.assertIn((await resumed.receive(timeout=1)).type, (WSMsgType.CLOSE, WSMsgType.CLOSED))
        await resumed.close()
        await old.close()
        self.assertEqual(grant_binding(new_grant), self.services.links["example"]["binding"])
        response = await self.post("/api/service-authorities/resolve", {"resource": RESOURCE})
        self.assertEqual(grant_binding(new_grant), (await response.json())["binding"])
        self.assertFalse(new.closed)

    async def test_reply_requires_exact_binding_and_coordinator_epoch(self):
        socket = await self.authority_socket()
        self.active()
        task = asyncio.create_task(self.post("/api/service-authorities/call",
                                             self.envelope(resource=RESOURCE)))
        request = await socket.receive_json(timeout=1)
        await socket.send_json(dict(type="call_result", id=request["id"], status=200,
            binding=dict(self.binding, manifest_revision="f" * 64), result={"id": "wrong"}))
        response = await task
        self.assertEqual(503, response.status)
        await response.read()
        # A changed coordinator epoch invalidates a previously registered socket.
        self.coordinator.boot_epoch = "restarted"
        response = await self.post("/api/service-authorities/call", self.envelope(resource=RESOURCE))
        self.assertEqual(503, response.status)
        await response.read()

    async def test_logical_authority_inspection_keeps_native_transport_independent(self):
        socket = await self.authority_socket()
        self.active()
        session = "3" * 32
        raw = json.dumps(dict(authority=AUTHORITY, session=session, timestamp=int(time.time()))).encode()
        signature = hmac.new(service_authority_key(SECRET, AUTHORITY).encode(), raw,
                             hashlib.sha256).hexdigest()
        task = asyncio.create_task(self.client.post(self.base + "/stream/inspect", data=raw,
                                                    headers={"Authorization": "Router " + signature}))
        request = await socket.receive_json(timeout=1)
        grant = dict(id=session, authority=AUTHORITY, state="open")
        await socket.send_json(dict(type="inspect_result", id=request["id"],
                                    binding=request["binding"], status=200, grant=grant))
        response = await task
        self.assertEqual(200, response.status)
        self.assertEqual(grant, await response.json())
        self.assertEqual({NATIVE: SECRET}, self.authorities.secrets)
        response = await self.client.post(self.base + "/stream/inspect", data=raw,
                                         headers={"Authorization": "Router wrong"})
        self.assertEqual(401, response.status)
        await response.read()

    async def test_preparation_validation_http_requires_exact_grant_and_private_auth(self):
        response = await self.post("/api/service-coordinator/validate-preparation", self.binding)
        self.assertEqual(401, response.status)
        await response.read()
        response = await self.post("/api/service-coordinator/validate-preparation", self.binding, True)
        self.assertTrue((await response.json())["valid"])
        response = await self.post("/api/service-coordinator/validate", self.binding, True)
        self.assertFalse((await response.json())["valid"])

    def checkpoint(self, sequence=1, digest="a" * 64):
        return dict(service_id="example", sequence=sequence, digest=digest,
                    package={"signed": "opaque", "service_id": "example"})

    def replicate(self, checkpoint=None, targets=None):
        return dict(binding=self.binding, checkpoint=checkpoint or self.checkpoint(),
                    local_durable=True, targets=targets if targets is not None else [])

    async def helper_reply(self, socket, request, *, checkpoint=None, durable=True):
        value = request["checkpoint"]
        identity = ({key: value[key] for key in ("service_id", "sequence", "digest")}
                    if value else None)
        await socket.send_json(dict(type="checkpoint_result", id=request["id"],
            worker_id=request["worker_id"], worker_incarnation=request["worker_incarnation"],
            boot_epoch=request["boot_epoch"], binding=request["binding"], status=200,
            checkpoint=checkpoint if checkpoint is not None else identity,
            result={"durable": durable}))

    async def test_checkpoint_replicates_to_standby_before_active_authority_exists(self):
        helper = await self.helper_socket()
        body = self.replicate(targets=[dict(worker_id="mac", worker_incarnation="boot1")])
        task = asyncio.create_task(self.post("/api/service-checkpoints/replicate", body, True))
        request = await helper.receive_json(timeout=1)
        self.assertEqual("checkpoint.put", request["type"])
        self.assertEqual(body["checkpoint"], request["checkpoint"])
        self.assertEqual(self.binding, request["binding"])
        self.assertEqual({}, self.services.links)
        await self.helper_reply(helper, request)
        response = await task
        value = await response.json()
        self.assertEqual(200, response.status)
        self.assertTrue(value["acknowledgements"][0]["durable"])
        self.assertEqual(1, value["head"]["sequence"])
        self.assertNotIn("package", self.helpers.head("example"))
        self.assertEqual({}, self.helpers.pending)

    async def test_checkpoint_high_water_is_monotonic_and_is_lost_on_restart(self):
        response = await self.post("/api/service-checkpoints/replicate",
                                   self.replicate(self.checkpoint(3)), True)
        self.assertEqual(200, response.status)
        await response.read()
        for checkpoint in (self.checkpoint(2), self.checkpoint(3, "b" * 64)):
            response = await self.post("/api/service-checkpoints/replicate",
                                       self.replicate(checkpoint), True)
            self.assertEqual(409, response.status)
            await response.read()
        response = await self.post("/api/service-checkpoints/status",
                                   {"service_id": "example", "workers": []}, True)
        value = await response.json()
        self.assertEqual(3, value["head"]["sequence"])
        self.assertEqual(self.binding["boot_epoch"], value["boot_epoch"])
        self.coordinator.boot_epoch = "new-boot"
        self.assertIsNone(self.helpers.head("example"))
        response = await self.post("/api/service-checkpoints/replicate", self.replicate(), True)
        self.assertEqual(409, response.status)
        await response.read()

    async def test_checkpoint_ack_must_be_durable_and_exact_digest(self):
        helper = await self.helper_socket()
        body = self.replicate(targets=[dict(worker_id="mac", worker_incarnation="boot1")])
        for wrong in ("digest", "durability"):
            task = asyncio.create_task(self.post("/api/service-checkpoints/replicate", body, True))
            request = await helper.receive_json(timeout=1)
            if wrong == "digest":
                await self.helper_reply(helper, request,
                    checkpoint=dict(service_id="example", sequence=1, digest="b" * 64))
            else:
                await self.helper_reply(helper, request, durable=False)
            response = await task
            self.assertEqual(503, response.status)
            await response.read()
            self.assertIsNone(self.helpers.head("example"))

    async def test_checkpoint_helper_survives_child_unhealthy_and_rejects_old_incarnation(self):
        heartbeat(self.coordinator, self.spec, "mac", healthy=False)
        helper = await self.helper_socket()
        worker = dict(worker_id="mac", worker_incarnation="boot1")
        task = asyncio.create_task(self.post("/api/service-checkpoints/status",
            {"service_id": "example", "workers": [worker]}, True))
        request = await helper.receive_json(timeout=1)
        self.assertIsNone(request["binding"])
        self.assertEqual("checkpoint.status", request["type"])
        await self.helper_reply(helper, request)
        response = await task
        self.assertEqual(200, (await response.json())["workers"][0]["status"])
        self.clock.advance(6)
        heartbeat(self.coordinator, self.spec, "mac", incarnation="boot2")
        new = await self.helper_socket("mac", "boot2")
        resumed = await self.client.ws_connect(self.base + "/service-worker/mac/connect",
                                               headers=self.headers)
        await resumed.send_json(dict(type="register", worker_id="mac", worker_incarnation="boot1"))
        self.assertIn((await resumed.receive(timeout=1)).type, (WSMsgType.CLOSE, WSMsgType.CLOSED))
        await resumed.close()
        await helper.close()
        self.assertEqual(("mac", "boot2"), self.helpers.links["mac"]["key"])
        self.assertFalse(new.closed)

    async def test_checkpoint_rejects_late_reply_after_owner_expiry(self):
        helper = await self.helper_socket()
        body = self.replicate(targets=[dict(worker_id="mac", worker_incarnation="boot1")])
        task = asyncio.create_task(self.post("/api/service-checkpoints/replicate", body, True))
        request = await helper.receive_json(timeout=1)
        self.clock.advance(6)
        heartbeat(self.coordinator, self.spec, "mac")
        await self.helper_reply(helper, request)
        response = await task
        self.assertEqual(503, response.status)
        await response.read()
        self.assertIsNone(self.helpers.head("example"))

    async def chunk_reply(self, socket, request, result):
        await socket.send_json(dict(type="checkpoint_result", id=request["id"],
            worker_id=request["worker_id"], worker_incarnation=request["worker_incarnation"],
            boot_epoch=request["boot_epoch"], binding=request["binding"], status=200,
            checkpoint=None, result=result))

    async def test_chunk_put_requires_exact_durable_ack_without_recording_a_head(self):
        helper = await self.helper_socket()
        raw = b"bounded fixture"
        key = hashlib.sha256(raw).hexdigest()
        body = dict(service_id="example", worker=dict(worker_id="mac", worker_incarnation="boot1"),
            binding=self.binding, chunk=dict(digest=key, data=base64.b64encode(raw).decode()))
        good = dict(digest=key, bytes=len(raw), durable=True)
        for result in (dict(good, digest="f" * 64), dict(good, bytes=len(raw)+1),
                       dict(good, durable=False), good):
            task = asyncio.create_task(self.post("/api/service-checkpoints/chunk-put", body, True))
            request = await helper.receive_json(timeout=1)
            self.assertEqual("checkpoint.chunk.put", request["type"])
            self.assertEqual(key, request["chunk"]["digest"])
            await self.chunk_reply(helper, request, result)
            response = await task
            self.assertEqual(200 if result == good else 503, response.status)
            await response.read()
            self.assertIsNone(self.helpers.head("example"))
            self.assertEqual({}, self.helpers.pending)

    async def test_chunk_reads_reject_corrupt_bytes_or_unrequested_missing_digests(self):
        helper = await self.helper_socket()
        raw = b"expected"
        key = hashlib.sha256(raw).hexdigest()
        common = dict(service_id="example", worker=dict(worker_id="mac", worker_incarnation="boot1"),
                      binding=self.binding)
        task = asyncio.create_task(self.post("/api/service-checkpoints/chunk-get",
            dict(common, chunk={"digest": key}), True))
        request = await helper.receive_json(timeout=1)
        await self.chunk_reply(helper, request, dict(digest=key, bytes=len(raw),
            data=base64.b64encode(b"tampered").decode()))
        response = await task
        self.assertEqual(503, response.status)
        await response.read()
        task = asyncio.create_task(self.post("/api/service-checkpoints/chunk-missing",
            dict(common, digests=[key]), True))
        request = await helper.receive_json(timeout=1)
        await self.chunk_reply(helper, request, {"missing": ["a" * 64]})
        response = await task
        self.assertEqual(503, response.status)
        await response.read()

    async def test_chunk_request_refuses_stale_grant_bad_digest_and_oversize(self):
        raw = b"x"
        body = dict(service_id="example", worker=dict(worker_id="mac", worker_incarnation="boot1"),
            binding=dict(self.binding, grant_token="stale"), chunk=dict(
                digest=hashlib.sha256(raw).hexdigest(), data=base64.b64encode(raw).decode()))
        response = await self.post("/api/service-checkpoints/chunk-put", body, True)
        self.assertEqual(409, response.status)
        await response.read()
        body["binding"] = self.binding
        body["chunk"]["digest"] = "a" * 64
        response = await self.post("/api/service-checkpoints/chunk-put", body, True)
        self.assertEqual(400, response.status)
        await response.read()
        raw = b"x" * (256 * 1024 + 1)
        body["chunk"] = dict(digest=hashlib.sha256(raw).hexdigest(),
                             data=base64.b64encode(raw).decode())
        response = await self.post("/api/service-checkpoints/chunk-put", body, True)
        self.assertEqual(400, response.status)
        await response.read()
        self.assertEqual({}, self.helpers.pending)

    async def test_root_gc_notification_follows_witness_publication(self):
        helper = await self.helper_socket()
        checkpoint = self.checkpoint()
        checkpoint["package"]["payload"] = {"state": {"format": "biomerte-controller-state/1"}}
        body = self.replicate(checkpoint, [dict(worker_id="mac", worker_incarnation="boot1")])
        task = asyncio.create_task(self.post("/api/service-checkpoints/replicate", body, True))
        request = await helper.receive_json(timeout=1)
        self.assertIsNone(self.helpers.head("example"))
        await self.helper_reply(helper, request)
        published = await helper.receive_json(timeout=1)
        self.assertEqual("checkpoint.published", published["type"])
        self.assertEqual(checkpoint["digest"], self.helpers.head("example")["digest"])
        await self.helper_reply(helper, published)
        response = await task
        self.assertEqual(200, response.status)
        await response.read()
        self.assertNotIn("package", self.helpers.head("example"))



if __name__ == "__main__":
    unittest.main()
