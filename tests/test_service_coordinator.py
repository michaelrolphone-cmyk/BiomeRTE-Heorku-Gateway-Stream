import asyncio
import copy
from unittest import mock
import unittest

from aiohttp import ClientSession, web

from biomerte.service_coordinator import (
    Conflict, Coordinator, coordinator_key, manifest_revision)


class Clock:
    def __init__(self):
        self.value = 0.0
    def __call__(self):
        return self.value
    def advance(self, seconds):
        self.value += seconds


def manifest(**changes):
    value = dict(id="example", images={"linux/arm64": "sha256:" + "a" * 64,
                                     "linux/arm/v7": "registry/service@sha256:" + "b" * 64},
                 effect_policy="stateless", state_policy="stateless",
                 lease_ttl_s=5, worker_ttl_s=6, startup_grace_s=8,
                 preferred_workers=["pi", "mac"], command=["python", "-m", "example"],
                 environment={"MODE": "worker"})
    value.update(changes)
    value["revision"] = manifest_revision(value)
    return value


def heartbeat(coordinator, spec, worker="mac", incarnation="boot1",
              grant=None, lease_scoped=True, **changes):
    platform = "linux/arm/v7" if worker == "pi" else "linux/arm64"
    row = dict(manifest=copy.deepcopy(spec), manifest_revision=spec["revision"],
               platform=platform, image_ref=spec["images"][platform],
               prepared=True, dependencies_ready=True, healthy=None)
    if grant:
        row.update(grant_token=grant["grant_token"], boot_epoch=grant["boot_epoch"])
    row.update(changes)
    return coordinator.heartbeat(dict(worker_id=worker,
        worker_incarnation=incarnation, services=[row], lease_scoped=lease_scoped))


class CoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.coordinator = Coordinator("test-secret", max_lease_ttl_s=10, clock=self.clock)
        self.spec = manifest()

    def ready(self, worker="pi", spec=None):
        self.clock.advance(10)
        result = heartbeat(self.coordinator, spec or self.spec, worker)
        self.assertEqual({}, result["errors"])
        return result["assignments"][0]

    def test_capability_demand_starts_and_drains_exact_lease_generation(self):
        spec = manifest(capability_lease={"controller": "a" * 64, "provider": "b" * 64,
                                        "contract": "workspace.status:1", "resource": "worker"},
                        health={"command": ["ready"]}, state_policy="lease_scoped")
        self.clock.advance(10)
        self.assertEqual([], heartbeat(self.coordinator, spec)["assignments"])
        old = heartbeat(self.coordinator, spec, "pi", lease_scoped=False)
        self.assertEqual([], old["manifests"])
        self.assertEqual([], old["assignments"])
        self.assertIn("lease-scoped", old["errors"][spec["id"]])
        self.coordinator.record_capability_demand(spec["id"], spec["revision"], ("c" * 64, 1), 16)
        grant = heartbeat(self.coordinator, spec)["assignments"][0]
        self.assertEqual("starting", grant["state"])
        self.assertFalse(self.coordinator.validate(grant)["valid"])
        self.assertEqual("starting", heartbeat(self.coordinator, spec, healthy=True)["assignments"][0]["state"])
        resumed = heartbeat(self.coordinator, spec, grant=grant, healthy=True)["assignments"][0]
        self.assertEqual(grant["grant_token"], resumed["grant_token"])
        self.assertEqual("active", resumed["state"])
        self.assertTrue(self.coordinator.validate(grant)["valid"])

        self.coordinator.record_capability_demand(spec["id"], spec["revision"], None, 16)
        self.assertFalse(self.coordinator.validate(grant)["valid"])
        self.coordinator.record_capability_demand(spec["id"], spec["revision"], ("c" * 64, 2), 17)
        self.assertEqual([], heartbeat(self.coordinator, spec, "pi")["assignments"])
        self.clock.advance(5)
        successor = heartbeat(self.coordinator, spec, "pi")["assignments"][0]
        self.assertNotEqual(grant["grant_token"], successor["grant_token"])
        self.assertEqual(2, successor["lease_ttl_s"])
        self.clock.advance(2)
        self.assertEqual([], heartbeat(self.coordinator, spec, "pi", grant=successor)["assignments"])

    def test_capability_demand_is_not_restored_after_coordinator_restart(self):
        spec = manifest(capability_lease={"controller": "a" * 64, "provider": "b" * 64,
                                        "contract": "workspace.status:1", "resource": "worker"},
                        health={"command": ["ready"]}, state_policy="lease_scoped")
        self.clock.advance(10)
        heartbeat(self.coordinator, spec)
        self.coordinator.record_capability_demand(spec["id"], spec["revision"], ("c" * 64, 1), 16)
        grant = heartbeat(self.coordinator, spec)["assignments"][0]
        restarted = Coordinator("test-secret", max_lease_ttl_s=10, clock=self.clock)
        self.clock.advance(10)
        self.assertEqual([], heartbeat(restarted, spec, grant=grant)["assignments"])
        self.assertFalse(restarted.validate(grant)["valid"])

    def test_boot_grace_collects_candidates_and_preferred_first_placement(self):
        self.assertEqual([], heartbeat(self.coordinator, self.spec, "mac")["assignments"])
        self.clock.advance(5)
        heartbeat(self.coordinator, self.spec, "pi")
        self.clock.advance(5)
        result = heartbeat(self.coordinator, self.spec, "mac")
        self.assertEqual([], result["assignments"])
        self.assertEqual("pi", result["statuses"]["example"]["owner"]["worker_id"])
        grant = heartbeat(self.coordinator, self.spec, "pi")["assignments"][0]
        self.assertTrue(self.coordinator.validate(grant)["valid"])

    def test_primary_loss_transfers_after_old_lease_and_liveness_expire(self):
        old = self.ready()
        heartbeat(self.coordinator, self.spec, "mac")
        self.clock.advance(4)
        heartbeat(self.coordinator, self.spec, "mac")
        self.assertTrue(self.coordinator.validate(old)["valid"])
        self.clock.advance(2)
        new = heartbeat(self.coordinator, self.spec, "mac")["assignments"][0]
        self.assertNotEqual(old["grant_token"], new["grant_token"])
        self.assertFalse(self.coordinator.validate(old)["valid"])
        self.assertTrue(self.coordinator.validate(new)["valid"])

    def test_unhealthy_owner_drains_without_early_replacement(self):
        old = self.ready()
        heartbeat(self.coordinator, self.spec, "mac")
        self.clock.advance(1)
        result = heartbeat(self.coordinator, self.spec, "pi", grant=old, healthy=False)
        self.assertEqual([], result["assignments"])
        self.assertFalse(self.coordinator.validate(old)["valid"])
        self.assertEqual([], heartbeat(self.coordinator, self.spec, "mac")["assignments"])
        self.clock.advance(4)
        new = heartbeat(self.coordinator, self.spec, "mac")["assignments"][0]
        self.assertEqual("mac", new["worker_id"])

    def test_partition_resume_cannot_revive_stale_token_or_preempt_healthy_owner(self):
        old = self.ready()
        self.clock.advance(6)
        new = heartbeat(self.coordinator, self.spec, "mac")["assignments"][0]
        result = heartbeat(self.coordinator, self.spec, "pi", grant=old, healthy=True)
        self.assertEqual([], result["assignments"])
        self.assertFalse(self.coordinator.validate(old)["valid"])
        self.assertTrue(self.coordinator.validate(new)["valid"])
        self.assertEqual("mac", self.coordinator.status()["services"]["example"]["owner"]["worker_id"])

    def test_restart_invalidates_boot_and_waits_full_configured_bound(self):
        old = self.ready()
        restarted = Coordinator("test-secret", max_lease_ttl_s=10, clock=self.clock)
        self.assertFalse(restarted.validate(old)["valid"])
        self.assertEqual([], heartbeat(restarted, self.spec, "mac")["assignments"])
        self.clock.advance(9)
        self.assertEqual([], heartbeat(restarted, self.spec, "mac")["assignments"])
        self.clock.advance(1)
        new = heartbeat(restarted, self.spec, "mac")["assignments"][0]
        self.assertNotEqual(old["boot_epoch"], new["boot_epoch"])

    def test_stale_token_and_wrong_incarnation_cannot_renew(self):
        grant = self.ready()
        self.clock.advance(2)
        wrong = dict(grant, grant_token="stale")
        result = heartbeat(self.coordinator, self.spec, "pi", grant=wrong, healthy=True)
        self.assertEqual(3, result["assignments"][0]["lease_ttl_s"])
        self.assertFalse(self.coordinator.validate(dict(grant, worker_incarnation="boot2"))["valid"])
        self.clock.advance(1)
        renewed = heartbeat(self.coordinator, self.spec, "pi", grant=grant, healthy=True)
        self.assertEqual(5, renewed["assignments"][0]["lease_ttl_s"])

    def test_missing_wrong_image_and_dependency_failures_are_ineligible(self):
        self.clock.advance(10)
        for changes in ({"prepared": False},
                        {"image_ref": "sha256:" + "c" * 64},
                        {"dependencies_ready": False},
                        {"platform": "linux/amd64"}):
            with self.subTest(changes=changes):
                result = heartbeat(self.coordinator, self.spec, **changes)
                self.assertEqual([], result["assignments"])
                candidate = result["statuses"]["example"]["candidates"][0]
                self.assertIsNotNone(candidate["reason"])

    def test_revision_binds_full_spec_and_replicas_cannot_overwrite(self):
        self.ready()
        changed = manifest(environment={"MODE": "different"})
        result = heartbeat(self.coordinator, changed, "mac")
        self.assertIn("manifest_conflict", result["errors"]["example"])
        self.assertEqual(self.spec["revision"],
                         self.coordinator.status()["services"]["example"]["manifest_revision"])
        tampered = copy.deepcopy(self.spec)
        tampered["command"] = ["different"]
        result = heartbeat(self.coordinator, tampered, "mac")
        self.assertIn("complete spec", next(iter(result["errors"].values())))

    def test_explicit_rollout_uses_cas_and_waits_old_grant_expiry(self):
        old = self.ready()
        changed = manifest(command=["new-revision"])
        with self.assertRaises(Conflict):
            self.coordinator.configure(changed, expected_revision="stale")
        self.coordinator.configure(changed, expected_revision=self.spec["revision"])
        self.assertFalse(self.coordinator.validate(old)["valid"])
        self.assertEqual([], heartbeat(self.coordinator, changed, "mac")["assignments"])
        self.clock.advance(5)
        new = heartbeat(self.coordinator, changed, "mac")["assignments"][0]
        self.assertEqual(changed["revision"], new["manifest_revision"])

    def test_release_does_not_reassign_before_expiry(self):
        grant = self.ready()
        heartbeat(self.coordinator, self.spec, "mac")
        self.coordinator.release(grant)
        self.assertFalse(self.coordinator.validate(grant)["valid"])
        self.assertEqual([], heartbeat(self.coordinator, self.spec, "mac")["assignments"])
        self.clock.advance(5)
        new = heartbeat(self.coordinator, self.spec, "mac")["assignments"][0]
        self.assertEqual("mac", new["worker_id"])
        with self.assertRaises(Conflict):
            self.coordinator.release(grant)

    def test_startup_health_grace_is_bounded_even_with_heartbeats(self):
        spec = manifest(startup_grace_s=3)
        grant = self.ready(spec=spec)
        self.clock.advance(2)
        result = heartbeat(self.coordinator, spec, "pi", grant=grant, healthy=None)
        self.assertEqual(1, len(result["assignments"]))
        self.clock.advance(1)
        result = heartbeat(self.coordinator, spec, "pi", grant=grant, healthy=None)
        self.assertEqual([], result["assignments"])
        self.assertFalse(self.coordinator.validate(grant)["valid"])

    def test_external_fence_ack_is_bound_to_grant_and_required_before_effects(self):
        spec = manifest(effect_policy="externally_fenced", fence_hook=["real-fence-adapter"])
        grant = self.ready(spec=spec)
        self.assertFalse(grant["effect_ready"])
        self.assertFalse(self.coordinator.validate(grant)["valid"])
        wrong = dict(fenced=True, boot_epoch=grant["boot_epoch"], grant_token="old")
        result = heartbeat(self.coordinator, spec, "pi", grant=grant, fence_ack=wrong)
        self.assertFalse(result["assignments"][0]["effect_ready"])
        ack = dict(fenced=True, boot_epoch=grant["boot_epoch"], grant_token=grant["grant_token"])
        result = heartbeat(self.coordinator, spec, "pi", grant=grant, fence_ack=ack, healthy=None)
        self.assertEqual("starting", result["assignments"][0]["state"])
        self.assertTrue(result["assignments"][0]["effect_ready"])
        self.assertTrue(self.coordinator.validate(grant)["valid"])
        # Accepted fence attestation stays bound to this grant, not every poll.
        active = heartbeat(self.coordinator, spec, "pi", grant=grant, healthy=True)
        self.assertEqual("active", active["assignments"][0]["state"])
        self.assertTrue(self.coordinator.validate(grant)["valid"])

    def test_policy_labels_do_not_install_fencing_or_migrate_local_state(self):
        for spec in (manifest(effect_policy="watchdog"),
                     manifest(effect_policy="gateway_fenced"),
                     manifest(state_policy="local")):
            with self.subTest(spec=spec):
                result = heartbeat(self.coordinator, spec)
                self.assertTrue(result["errors"])
                self.assertEqual([], result["assignments"])


    def test_catalog_distributes_manifests_without_manual_peer_copy(self):
        heartbeat(self.coordinator, self.spec, "pi")
        response = self.coordinator.heartbeat(dict(worker_id="mac",
            worker_incarnation="boot1", services=[]))
        self.assertEqual([self.spec], response["manifests"])
        self.assertEqual([self.spec], self.coordinator.catalog()["manifests"])

    def test_conflicting_durable_replicas_during_boot_require_explicit_resolution(self):
        heartbeat(self.coordinator, self.spec, "pi")
        other = manifest(command=["different-durable-replica"])
        heartbeat(self.coordinator, other, "mac")
        self.assertEqual([], self.coordinator.catalog()["manifests"])
        self.assertEqual(2, len(self.coordinator.catalog()["conflicts"]["example"]))
        self.clock.advance(10)
        self.assertEqual([], heartbeat(self.coordinator, self.spec, "pi")["assignments"])
        with self.assertRaises(Conflict):
            self.coordinator.configure(self.spec)
        self.coordinator.configure(other, expected_revision=self.spec["revision"])
        result = heartbeat(self.coordinator, other, "mac")
        self.assertEqual(1, len(result["assignments"]))

    def test_one_invalid_service_does_not_expire_other_service_heartbeat(self):
        grant = self.ready()
        other = manifest(id="other")
        rows = []
        for spec in (self.spec, other):
            rows.append(dict(manifest=spec, manifest_revision=spec["revision"],
                platform="linux/arm/v7", image_ref=spec["images"]["linux/arm/v7"],
                prepared=True, dependencies_ready=True, healthy=True,
                boot_epoch=grant["boot_epoch"], grant_token=grant["grant_token"]))
        rows[1]["manifest"] = dict(other, revision="invalid")
        self.clock.advance(2)
        result = self.coordinator.heartbeat(dict(worker_id="pi",
            worker_incarnation="boot1", services=rows))
        self.assertTrue(result["errors"])
        self.assertEqual(5, result["assignments"][0]["lease_ttl_s"])

    def test_expired_owner_is_not_regranted_from_old_health(self):
        spec = manifest(worker_ttl_s=20)
        old = self.ready(spec=spec)
        heartbeat(self.coordinator, spec, "mac")
        self.clock.advance(5)
        new = heartbeat(self.coordinator, spec, "mac")["assignments"][0]
        self.assertEqual("mac", new["worker_id"])
        self.assertFalse(self.coordinator.validate(old)["valid"])

    def test_configured_revision_proves_explicit_selection_only_for_current_boot(self):
        heartbeat(self.coordinator, self.spec, "pi")
        self.assertEqual({}, self.coordinator.catalog()["configured_revisions"])
        self.coordinator.configure(self.spec)
        self.assertEqual({"example": self.spec["revision"]},
                         self.coordinator.catalog()["configured_revisions"])
        restarted = Coordinator("test-secret", clock=self.clock)
        heartbeat(restarted, self.spec, "pi")
        self.assertEqual({}, restarted.catalog()["configured_revisions"])

    def test_late_unknown_replica_revision_blocks_but_selected_rollout_does_not(self):
        old = self.ready()
        changed = manifest(command=["later-replica"])
        result = heartbeat(self.coordinator, changed, "mac")
        self.assertEqual("replica_revision_conflict", result["statuses"]["example"]["state"])
        self.assertFalse(self.coordinator.validate(old)["valid"])
        self.coordinator.configure(changed, expected_revision=self.spec["revision"])
        result = heartbeat(self.coordinator, self.spec, "pi")
        self.assertEqual([], result["statuses"]["example"]["conflicting_revisions"])
        self.assertEqual({"example": changed["revision"]}, result["configured_revisions"])

    def test_max_lease_bound_and_idle_retention(self):
        with self.assertRaises(ValueError):
            self.coordinator.configure(manifest(lease_ttl_s=11))
        grant = self.ready()
        self.clock.advance(301)
        self.assertEqual({}, self.coordinator.status()["services"])
        self.assertFalse(self.coordinator.validate(grant)["valid"])


class DemandWatcherTests(unittest.IsolatedAsyncioTestCase):
    async def test_pooled_slots_share_one_read_and_grants_follow_their_own_slot(self):
        from biomerte import service_demand
        from types import SimpleNamespace
        clock = Clock()
        coordinator = Coordinator("pool", clock=clock, max_lease_ttl_s=10)
        specs = []
        for slot in (0, 1):
            spec = manifest(id="pool-" + str(slot), state_policy="lease_scoped",
                            capability_lease=dict(controller="a"*64, provider="b"*64,
                                                  resource="pool", contract="job:1", slot=slot),
                            health={"command": ["ready"]})
            coordinator.configure(spec)
            specs.append(spec)
        clock.advance(10)
        read = mock.AsyncMock(return_value={"status": 200, "result": {"slots": {
            "0": None, "1": {"id": "c"*64, "generation": 3}}}})
        authorities = SimpleNamespace(scope=lambda **unused: ("link", {"kind": "controller"}), rpc=read)
        await service_demand.observe(coordinator, authorities, specs)
        read.assert_awaited_once()
        self.assertEqual([0, 1], read.await_args.args[1]["scope"]["slots"])
        self.assertFalse(heartbeat(coordinator, specs[0])["assignments"])
        grant = heartbeat(coordinator, specs[1])["assignments"][0]
        self.assertEqual({"id": "c"*64, "generation": 3}, grant["capability_demand"])

    async def test_slow_or_failed_controller_does_not_block_other_observations(self):
        from biomerte import service_demand
        coordinator = Coordinator("watch-test")
        for name in ("slow", "fast"):
            item = manifest(id=name, state_policy="lease_scoped", health={"command": ["true"]},
                            capability_lease=dict(controller="a"*64, provider="b"*64,
                                                  resource=name, contract="job:1"))
            coordinator.configure(item)
        blocked, cancelled, refreshed = asyncio.Event(), asyncio.Event(), asyncio.Event()
        calls = {"slow": 0, "fast": 0}

        async def observe(unused_coordinator, unused_authorities, group):
            name = group[0]["id"]
            calls[name] += 1
            if name == "slow":
                blocked.set()
                try:
                    await asyncio.Event().wait()
                finally:
                    cancelled.set()
            elif calls[name] == 1:
                raise ConnectionResetError("controller disconnected")
            else:
                refreshed.set()

        with mock.patch.object(service_demand, "observe", observe), \
                mock.patch.object(service_demand, "POLL_INTERVAL_S", .01):
            watcher = asyncio.create_task(service_demand.watch(coordinator, None))
            try:
                await asyncio.wait_for(blocked.wait(), 1)
                await asyncio.wait_for(refreshed.wait(), 1)
                self.assertEqual(1, calls["slow"])
                del coordinator.services["slow"]
                await asyncio.wait_for(cancelled.wait(), 1)
                self.assertFalse(watcher.done())
            finally:
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)


class CoordinatorHttpTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = Clock()
        self.coordinator = Coordinator("test-secret", max_lease_ttl_s=10, clock=self.clock)
        app = web.Application()
        self.coordinator.mount(app)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        self.addAsyncCleanup(self.runner.cleanup)
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        self.base = "http://127.0.0.1:" + str(site._server.sockets[0].getsockname()[1])
        self.client = ClientSession()
        self.addAsyncCleanup(self.client.close)
        self.headers = {"Authorization": "Bearer " + coordinator_key("test-secret")}

    async def test_authenticated_status_and_validation_do_not_expose_tokens(self):
        path = self.base + "/api/service-coordinator/status"
        async with self.client.get(path) as response:
            self.assertEqual(401, response.status)
        async with self.client.get(path, headers=self.headers) as response:
            self.assertEqual(200, response.status)
            self.assertNotIn("grant_token", await response.text())
        async with self.client.get(path, headers=dict(self.headers, Origin="https://example.test")) as response:
            self.assertEqual(403, response.status)
        async with self.client.post(self.base + "/api/service-coordinator/validate",
                                    headers=self.headers, json={"service_id": "absent"}) as response:
            self.assertFalse((await response.json())["valid"])

    async def test_http_configure_and_conflict(self):
        path = self.base + "/api/service-coordinator/configure"
        async with self.client.post(path, headers=self.headers, json={"manifest": manifest()}) as response:
            self.assertEqual(200, response.status)
        async with self.client.post(path, headers=self.headers,
                                    json={"manifest": manifest(command=["changed"])}) as response:
            self.assertEqual(409, response.status)


if __name__ == "__main__":
    unittest.main()
