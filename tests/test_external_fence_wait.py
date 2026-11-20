"""Bounded, opt-in waiting for a physical fence with independent live evidence."""
import asyncio
import copy
import unittest

from aiohttp import ClientSession, web

from biomerte.checkpoint_transport import CheckpointWorkers
from biomerte.service_coordinator import Coordinator, grant_binding, manifest_revision
from biomerte.service_transport import ServiceAuthorities
from test_service_activation import service_manifest, readiness
from test_service_coordinator import Clock, heartbeat


def waiting_manifest(enabled=True, **changes):
    activation = dict(protocol="biomerte-service-adapter/1",
                      binding="test-authority", wait_for_external_fence=enabled)
    return service_manifest(activation=activation, **changes)


class ExternalFenceWaitTests(unittest.TestCase):
    def setUp(self):
        self.prepare()

    def prepare(self, enabled=True, **changes):
        self.clock = Clock()
        self.coordinator = Coordinator("secret", max_lease_ttl_s=10, clock=self.clock)
        self.spec = waiting_manifest(enabled, **changes)
        self.clock.advance(10)
        self.grant = heartbeat(self.coordinator, self.spec, "pi")["assignments"][0]
        self.binding = grant_binding(self.grant)
        self.head = dict(service_id="example", sequence=1, digest="a" * 64,
                         binding=self.binding, boot_epoch=self.coordinator.boot_epoch)
        self.coordinator.checkpoint_head = lambda sid: self.head
        self.live = True
        self.coordinator.preparation_connected = lambda b: self.live and b == self.binding
        self.proof = readiness(self.grant)
        self.proof.pop("fence_ack")
        self.report()
        # Registration is allowed to sustain a wait only after a new durable
        # fence-intent checkpoint follows the acknowledged prepared baseline.
        self.head = dict(self.head, sequence=2, digest="b" * 64)
        self.report()

    def report(self, **changes):
        fields = dict(self.proof)
        fields.update(changes)
        result = heartbeat(self.coordinator, self.spec, "pi", grant=self.grant, **fields)
        if result["assignments"]:
            self.grant = result["assignments"][0]
        return result

    def wait_beyond_startup(self):
        for unused in range(8):
            self.clock.advance(2)
            result = self.report()
            self.assertEqual(1, len(result["assignments"]))
            self.assertEqual(self.binding, grant_binding(self.grant))

    def test_same_binding_and_checkpoint_survive_repeated_grace_windows(self):
        self.wait_beyond_startup()
        self.assertEqual("waiting_for_external_fence", self.grant["state"])
        self.assertEqual(2, self.head["sequence"])
        self.assertTrue(self.grant["state_ready"])
        self.assertFalse(self.grant["effect_ready"])
        self.assertTrue(self.coordinator.validate_preparation(self.binding)["valid"])
        self.assertFalse(self.coordinator.validate(self.binding)["valid"])
        self.assertIsNone(self.coordinator.current_grant("example"))

    def test_default_policy_still_times_out_despite_live_link_and_state(self):
        self.prepare(enabled=False)
        for unused in range(3):
            self.clock.advance(2)
            self.report()
        self.clock.advance(2)
        self.assertEqual([], self.report()["assignments"])
        self.assertFalse(self.coordinator.validate_preparation(self.binding)["valid"])

    def test_stalled_before_registration_keeps_the_original_startup_deadline(self):
        self.live = False
        for unused in range(3):
            self.clock.advance(2)
            self.assertEqual(1, len(self.report()["assignments"]))
        self.clock.advance(2)
        self.assertEqual([], self.report()["assignments"])
        self.assertEqual("startup_health_timeout",
                         self.coordinator.status()["services"]["example"]["state"])

    def test_wait_requires_current_post_baseline_head_and_live_exact_link(self):
        for missing in ("link", "head", "new_intent", "epoch", "binding", "state_proof"):
            with self.subTest(missing=missing):
                self.prepare()
                self.wait_beyond_startup()
                fields = {}
                if missing == "link":
                    self.live = False
                elif missing == "head":
                    self.head = None
                elif missing == "new_intent":
                    self.head = dict(self.head, sequence=1, digest="a" * 64)
                elif missing == "epoch":
                    self.head = dict(self.head, boot_epoch="previous-witness")
                elif missing == "binding":
                    self.head = dict(self.head, binding=dict(self.binding, grant_token="old"))
                else:
                    fields["state_ready"] = False
                self.assertEqual([], self.report(**fields)["assignments"])
                self.assertFalse(self.coordinator.validate_preparation(self.binding)["valid"])

    def test_running_preparation_is_unknown_readiness_but_process_failure_revokes(self):
        # Runner _health reports None for a running unactivated child even
        # when its effect-readiness HTTP health is503. False means stopped
        # process, or failed readiness after activation; do not reinterpret it.
        self.wait_beyond_startup()
        self.assertEqual([], self.report(healthy=False)["assignments"])
        self.assertEqual("service_unhealthy",
                         self.coordinator.status()["services"]["example"]["state"])
        self.assertFalse(self.coordinator.validate_preparation(self.binding)["valid"])

    def test_worker_loss_drains_before_lease_expiry_then_allows_fresh_owner(self):
        self.prepare(worker_ttl_s=3, lease_ttl_s=10)
        self.wait_beyond_startup()
        self.clock.advance(4)
        result = heartbeat(self.coordinator, self.spec, "mac")
        self.assertEqual([], result["assignments"])
        self.assertFalse(self.coordinator.validate_preparation(self.binding)["valid"])
        self.clock.advance(7)
        new = heartbeat(self.coordinator, self.spec, "mac")["assignments"][0]
        self.assertNotEqual(self.binding, grant_binding(new))
        self.assertFalse(new["state_ready"])
        self.assertFalse(new["effect_ready"])

    def test_status_and_live_socket_never_renew_an_expired_lease(self):
        self.wait_beyond_startup()
        self.clock.advance(6)
        self.coordinator.status()
        self.assertFalse(self.coordinator.validate_preparation(self.binding)["valid"])
        new = heartbeat(self.coordinator, self.spec, "mac")["assignments"][0]
        self.assertNotEqual(self.binding["grant_token"], new["grant_token"])
        self.assertFalse(new["effect_ready"])

    def test_revision_release_and_witness_restart_do_not_preserve_wait(self):
        for boundary in ("revision", "release", "restart"):
            with self.subTest(boundary=boundary):
                self.prepare()
                self.wait_beyond_startup()
                if boundary == "revision":
                    revised = copy.deepcopy(self.spec)
                    revised["environment"]["MODE"] = "new-image-config"
                    revised["revision"] = manifest_revision(revised)
                    self.coordinator.configure(revised, self.spec["revision"])
                    self.assertFalse(self.coordinator.validate_preparation(self.binding)["valid"])
                elif boundary == "release":
                    self.coordinator.release(self.binding)
                    self.assertFalse(self.coordinator.validate_preparation(self.binding)["valid"])
                else:
                    restarted = Coordinator("secret", max_lease_ttl_s=10, clock=self.clock)
                    restarted.checkpoint_head = lambda sid: self.head
                    restarted.preparation_connected = lambda b: True
                    self.clock.advance(10)
                    new = heartbeat(restarted, self.spec, "mac", **self.proof)["assignments"][0]
                    self.assertNotEqual(self.binding["boot_epoch"], new["boot_epoch"])
                    self.assertFalse(restarted.validate_preparation(self.binding)["valid"])
                    self.assertFalse(new["state_ready"])
                    self.assertFalse(new["effect_ready"])

    def test_real_fence_after_long_wait_allows_activation_but_not_false_health(self):
        self.wait_beyond_startup()
        ready = readiness(self.grant)
        result = self.report(**ready)
        self.assertEqual(1, len(result["assignments"]))
        self.assertEqual("starting", self.grant["state"])
        self.assertTrue(self.grant["effect_ready"])
        self.assertTrue(self.coordinator.validate(self.binding)["valid"])
        self.clock.advance(2)
        self.report(healthy=True, **ready)
        self.assertEqual("active", self.grant["state"])
        self.assertEqual([], self.report(healthy=False, **ready)["assignments"])

    def test_activation_after_wait_still_has_a_bounded_health_window(self):
        self.wait_beyond_startup()
        ready = readiness(self.grant)
        self.report(**ready)
        for unused in range(3):
            self.clock.advance(2)
            self.assertEqual(1, len(self.report(**ready)["assignments"]))
        self.clock.advance(2)
        self.assertEqual([], self.report(**ready)["assignments"])
        self.assertEqual("startup_health_timeout",
                         self.coordinator.status()["services"]["example"]["state"])

    def test_wait_option_requires_boolean_and_scoped_authority(self):
        for value in ("true", 1, None):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    self.coordinator.configure(waiting_manifest(value))
        with self.assertRaises(ValueError):
            self.coordinator.configure(waiting_manifest(authority=None))


class ExternalFenceWaitTransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = Clock()
        self.coordinator = Coordinator("secret", max_lease_ttl_s=10, clock=self.clock)
        self.services = ServiceAuthorities(self.coordinator, "secret", timeout=.3, heartbeat=.04)
        self.helpers = CheckpointWorkers(self.coordinator, timeout=.3)
        self.spec = waiting_manifest()
        self.clock.advance(10)
        self.grant = heartbeat(self.coordinator, self.spec, "pi")["assignments"][0]
        self.binding = grant_binding(self.grant)
        self.helpers.heads["example"] = dict(service_id="example", sequence=1, digest="a" * 64,
            binding=self.binding, boot_epoch=self.coordinator.boot_epoch)
        self.proof = readiness(self.grant)
        self.proof.pop("fence_ack")
        heartbeat(self.coordinator, self.spec, "pi", grant=self.grant, **self.proof)
        self.helpers.heads["example"] = dict(self.helpers.heads["example"],
                                             sequence=2, digest="b" * 64)
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

    async def connect(self, autoping=True):
        socket = await self.client.ws_connect(self.base + "/service-authority/example/connect",
            headers={"Authorization": "Bearer " + self.coordinator.key}, autoping=autoping)
        self.addAsyncCleanup(socket.close)
        await socket.send_json(dict(type="register", binding=self.binding))
        reply = await socket.receive_json(timeout=1)
        self.assertEqual("registered", reply["type"])
        self.assertTrue(self.services.preparation_connected(self.binding))
        return socket

    def renew(self):
        return heartbeat(self.coordinator, self.spec, "pi", grant=self.grant, **self.proof)

    async def test_real_registered_socket_sustains_wait_and_close_revokes_it(self):
        socket = await self.connect()
        for unused in range(5):
            self.clock.advance(2)
            grant = self.renew()["assignments"][0]
            self.assertEqual("waiting_for_external_fence", grant["state"])
            self.assertFalse(grant["effect_ready"])
        self.assertFalse(self.services.preparation_connected(
            dict(self.binding, grant_token="different")))
        await socket.close()
        for unused in range(50):
            if not self.services.preparation_connected(self.binding):
                break
            await asyncio.sleep(.01)
        self.assertFalse(self.services.preparation_connected(self.binding))
        self.assertEqual([], self.renew()["assignments"])

    async def test_no_pong_closes_socket_even_while_worker_keeps_renewing(self):
        socket = await self.connect(autoping=False)
        for unused in range(5):
            self.clock.advance(2)
            self.assertEqual(1, len(self.renew()["assignments"]))
        for unused in range(100):
            if not self.services.preparation_connected(self.binding):
                break
            await asyncio.sleep(.01)
        self.assertFalse(self.services.preparation_connected(self.binding))
        self.assertEqual([], self.renew()["assignments"])
        self.assertFalse(self.coordinator.validate_preparation(self.binding)["valid"])

    async def test_live_socket_cannot_mask_explicit_process_failure(self):
        await self.connect()
        result = heartbeat(self.coordinator, self.spec, "pi", grant=self.grant,
                           healthy=False, **self.proof)
        self.assertEqual([], result["assignments"])
        self.assertFalse(self.coordinator.validate_preparation(self.binding)["valid"])


if __name__ == "__main__":
    unittest.main()
