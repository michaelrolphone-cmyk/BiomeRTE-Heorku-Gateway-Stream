"""Independent checkpoint services share a supervisor, never a socket scope."""
import asyncio
import copy
import unittest

from aiohttp import ClientSession, WSMsgType, WSServerHandshakeError, web

from biomerte.checkpoint_transport import CheckpointWorkers
from biomerte.service_coordinator import Coordinator
from test_service_coordinator import Clock, heartbeat
from test_service_activation import service_manifest


class CheckpointServiceScopeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = Clock()
        self.coordinator = Coordinator("service-scope-fixture", clock=self.clock,
                                       max_lease_ttl_s=5)
        self.specs = {
            "relay": service_manifest(id="relay"),
            "controller": service_manifest(id="controller", authority={
                "id": "f" * 64, "resources": ["different.resource"], "providers": ["c" * 64]})}
        self.clock.advance(5)
        for spec in self.specs.values():
            heartbeat(self.coordinator, spec, "mac")
        self.helpers = CheckpointWorkers(self.coordinator, timeout=.15)
        app = web.Application()
        self.helpers.mount(app)
        self.runner = web.AppRunner(app, access_log=None)
        await self.runner.setup()
        self.addAsyncCleanup(self.runner.cleanup)
        self.addAsyncCleanup(self.helpers.close)
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.base = "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
        self.client = ClientSession()
        self.addAsyncCleanup(self.client.close)
        self.headers = {"Authorization": "Bearer " + self.coordinator.key}
        self.worker = dict(worker_id="mac", worker_incarnation="boot1")

    def reference(self, service):
        return dict(service_id=service, sequence=8 if service == "relay" else 2,
                    digest=("a" if service == "relay" else "b") * 64)

    def result(self, request, service, **changes):
        reply = dict(type="checkpoint_result", id=request["id"],
            worker_id=request["worker_id"], worker_incarnation=request["worker_incarnation"],
            boot_epoch=request["boot_epoch"], binding=request["binding"], status=200,
            service_id=service, checkpoint=self.reference(service),
            result={"durable": True, "retained": [self.reference(service)]})
        reply.update(changes)
        return reply

    async def socket(self, service=None, incarnation="boot1", legacy=False):
        socket = await self.client.ws_connect(self.base + "/service-worker/mac/connect",
                                              headers=self.headers)
        self.addAsyncCleanup(socket.close)
        value = dict(type="register", worker_id="mac", worker_incarnation=incarnation)
        if not legacy:
            value["service_id"] = service
        await socket.send_json(value)
        return socket

    async def scoped(self, service, incarnation="boot1"):
        socket = await self.socket(service, incarnation)
        ack = await socket.receive_json(timeout=1)
        self.assertEqual("registered", ack["type"])
        self.assertEqual(service, ack["service_id"])
        self.assertEqual(incarnation, ack["worker_incarnation"])
        return socket

    async def legacy(self, service="relay"):
        socket = await self.socket(legacy=True)
        probe = await socket.receive_json(timeout=1)
        self.assertEqual("checkpoint.status", probe["type"])
        self.assertNotIn("service_id", probe)
        reply = self.result(probe, service)
        reply.pop("service_id")
        await socket.send_json(reply)
        ack = await socket.receive_json(timeout=1)
        self.assertEqual(("registered", service), (ack["type"], ack["service_id"]))
        return socket

    async def closed(self, socket):
        message = await socket.receive(timeout=1)
        self.assertIn(message.type, (WSMsgType.CLOSE, WSMsgType.CLOSED))
        await socket.close()

    async def eventually(self, predicate):
        deadline = asyncio.get_running_loop().time() + 1
        while not predicate():
            if asyncio.get_running_loop().time() > deadline:
                self.fail("checkpoint connection did not settle")
            await asyncio.sleep(.005)

    async def status(self, service):
        return await self.client.post(self.base + "/api/service-checkpoints/status",
            headers=self.headers, json={"service_id": service, "workers": []})

    async def test_legacy_relay_and_scoped_controller_coexist_and_enumerate_exact_service(self):
        relay = await self.legacy()
        controller = await self.scoped("controller")
        self.assertEqual({("mac", "relay"), ("mac", "controller")}, set(self.helpers.links))
        for service, socket in (("relay", relay), ("controller", controller)):
            task = asyncio.create_task(self.status(service))
            request = await socket.receive_json(timeout=1)
            self.assertEqual(service, request["service_id"])
            reply = self.result(request, service)
            if service == "relay":
                reply.pop("service_id")
            await socket.send_json(reply)
            response = await task
            self.assertEqual(200, response.status)
            value = await response.json()
            self.assertEqual([self.worker], [
                {key: row[key] for key in self.worker} for row in value["workers"]])
            self.assertEqual(self.reference(service), value["workers"][0]["checkpoint"])
            self.assertEqual(200, value["workers"][0]["status"])

    async def test_scope_without_a_helper_has_no_fallback_to_a_sibling(self):
        await self.legacy()
        with self.assertRaises(web.HTTPServiceUnavailable):
            await self.helpers.rpc(self.worker, "checkpoint.status", "controller")
        response = await self.status("controller")
        self.assertEqual([], (await response.json())["workers"])
        self.assertEqual({}, self.helpers.pending)

    async def test_sibling_reply_cannot_complete_a_pending_rpc_even_with_exact_request_id(self):
        relay = await self.legacy()
        controller = await self.scoped("controller")
        task = asyncio.create_task(self.helpers.rpc(self.worker, "checkpoint.status", "controller"))
        request = await controller.receive_json(timeout=1)
        reply = self.result(request, "controller")
        await relay.send_json(reply)
        await asyncio.sleep(.02)
        self.assertFalse(task.done())
        for change in ({"service_id": "relay"}, {"worker_incarnation": "old"},
                       {"boot_epoch": "old"}, {"binding": {"wrong": True}}):
            await controller.send_json(dict(reply, **change))
        missing_scope = dict(reply)
        missing_scope.pop("service_id")
        await controller.send_json(missing_scope)
        await asyncio.sleep(.02)
        self.assertFalse(task.done())
        await controller.send_json(reply)
        self.assertEqual(self.reference("controller"), (await task)["checkpoint"])
        self.assertEqual({}, self.helpers.pending)

    async def test_same_scope_duplicate_cannot_replace_either_current_connection(self):
        relay = await self.legacy()
        controller = await self.scoped("controller")
        originals = dict(self.helpers.links)
        duplicate = await self.socket("controller")
        await self.closed(duplicate)
        duplicate = await self.socket(legacy=True)
        probe = await duplicate.receive_json(timeout=1)
        reply = self.result(probe, "relay")
        reply.pop("service_id")
        await duplicate.send_json(reply)
        await self.closed(duplicate)
        self.assertEqual(originals, self.helpers.links)
        self.assertFalse(relay.closed)
        self.assertFalse(controller.closed)

    async def test_disconnect_and_reconnect_retire_only_that_service_and_its_pending_rpc(self):
        relay = await self.legacy()
        controller = await self.scoped("controller")
        relay_link = self.helpers.links[("mac", "relay")]
        task = asyncio.create_task(self.helpers.rpc(self.worker, "checkpoint.status", "controller"))
        await controller.receive_json(timeout=1)
        await controller.close()
        with self.assertRaises(web.HTTPServiceUnavailable):
            await task
        await self.eventually(lambda: ("mac", "controller") not in self.helpers.links)
        replacement = await self.scoped("controller")
        self.assertIs(relay_link, self.helpers.links[("mac", "relay")])
        self.assertFalse(relay.closed)
        self.assertFalse(replacement.closed)
        self.assertEqual({}, self.helpers.pending)

    async def test_new_incarnation_replaces_each_scope_without_cross_scope_cleanup(self):
        old_relay = await self.legacy()
        old_controller = await self.scoped("controller")
        self.clock.advance(6)
        for spec in self.specs.values():
            heartbeat(self.coordinator, spec, "mac", incarnation="boot2")
        relay = await self.scoped("relay", "boot2")
        controller = await self.scoped("controller", "boot2")
        await old_relay.close()
        await old_controller.close()
        self.assertEqual({("mac", "relay"), ("mac", "controller")}, set(self.helpers.links))
        self.assertTrue(all(link["key"] == ("mac", "boot2") for link in self.helpers.links.values()))
        self.assertFalse(relay.closed)
        self.assertFalse(controller.closed)
        resumed = await self.socket("controller", "boot1")
        await self.closed(resumed)

    async def test_legacy_probe_rejects_missing_wrong_or_nondurable_correlations(self):
        mutations = [
            {"id": "foreign"}, {"worker_id": "pi"}, {"worker_incarnation": "old"},
            {"boot_epoch": "old"}, {"binding": {"wrong": True}}, {"status": True},
            {"status": 503}, {"result": {"durable": False}}, {"checkpoint": None},
            {"checkpoint": {"service_id": "relay", "sequence": True, "digest": "a" * 64}},
            {"checkpoint": {"service_id": "relay", "sequence": 1, "digest": "bad"}},
            {"checkpoint": {"service_id": "unknown", "sequence": 1, "digest": "a" * 64}},
            {"service_id": "controller"}]
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                socket = await self.socket(legacy=True)
                probe = await socket.receive_json(timeout=1)
                reply = self.result(probe, "relay")
                reply.pop("service_id")
                reply.update(mutation)
                await socket.send_json(reply)
                await self.closed(socket)
                self.assertEqual({}, self.helpers.links)
        socket = await self.socket(legacy=True)
        probe = await socket.receive_json(timeout=1)
        reply = self.result(probe, "relay")
        reply.pop("binding")
        await socket.send_json(reply)
        await self.closed(socket)

    async def test_probe_cannot_be_satisfied_by_an_existing_sibling_connection(self):
        controller = await self.scoped("controller")
        legacy = await self.socket(legacy=True)
        probe = await legacy.receive_json(timeout=1)
        await controller.send_json(self.result(probe, "relay"))
        await self.closed(legacy)
        self.assertEqual({("mac", "controller")}, set(self.helpers.links))

    async def test_probe_rechecks_current_incarnation_and_epoch_after_the_round_trip(self):
        for changed in ("incarnation", "epoch"):
            with self.subTest(changed=changed):
                socket = await self.socket(legacy=True)
                probe = await socket.receive_json(timeout=1)
                if changed == "incarnation":
                    self.clock.advance(6)
                    for spec in self.specs.values():
                        heartbeat(self.coordinator, spec, "mac", incarnation="new")
                else:
                    self.coordinator.boot_epoch = "different-epoch"
                await socket.send_json(self.result(probe, "relay"))
                await self.closed(socket)
                self.assertEqual({}, self.helpers.links)
                if changed == "incarnation":
                    self.clock.advance(6)
                    for spec in self.specs.values():
                        heartbeat(self.coordinator, spec, "mac")

    async def test_legacy_wrong_service_checkpoint_or_retained_reply_is_never_returned(self):
        relay = await self.legacy()
        for change in ({"checkpoint": self.reference("controller")},
                       {"result": {"durable": True, "retained": [self.reference("controller")]}},
                       {"service_id": "controller"}):
            task = asyncio.create_task(self.helpers.rpc(self.worker, "checkpoint.status", "relay"))
            request = await relay.receive_json(timeout=1)
            reply = self.result(request, "relay")
            reply.pop("service_id")
            reply.update(change)
            await relay.send_json(reply)
            with self.assertRaises(web.HTTPServiceUnavailable):
                await task
        self.assertEqual({}, self.helpers.pending)

    async def test_explicit_scope_requires_current_configured_service_and_authentication(self):
        for scope in ("missing", None, "", [], {}):
            with self.subTest(scope=scope):
                socket = await self.socket(scope)
                await self.closed(socket)
        with self.assertRaises(WSServerHandshakeError):
            await self.client.ws_connect(self.base + "/service-worker/mac/connect",
                                          headers={"Authorization": "Bearer invalid"})
        self.assertEqual({}, self.helpers.links)


if __name__ == "__main__":
    unittest.main()
