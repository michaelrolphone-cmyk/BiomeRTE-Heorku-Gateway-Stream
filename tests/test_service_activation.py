import copy
import unittest

from biomerte.service_coordinator import (
    Coordinator, grant_binding, manifest_revision, ADAPTER_PROTOCOL)
from test_service_coordinator import Clock, manifest, heartbeat


AUTHORITY = "d" * 64
PROVIDER = "e" * 64
RESOURCE = "test.relay"


def service_manifest(**changes):
    values = dict(effect_policy="externally_fenced", state_policy="externally_managed",
        activation={"protocol": ADAPTER_PROTOCOL, "binding": "test-authority"},
        authority={"id": AUTHORITY, "resources": [RESOURCE], "providers": [PROVIDER]})
    values.update(changes)
    return manifest(**values)


def readiness(grant, checkpoint="a" * 64):
    binding = grant_binding(grant)
    return dict(state_ready=True,
        state_proof=dict(protocol=ADAPTER_PROTOCOL, binding=binding,
            checkpoint=checkpoint, fresh=True, custody_complete=True, ready=True),
        fence_ack=dict(fenced=True, boot_epoch=binding["boot_epoch"],
            grant_token=binding["grant_token"], binding=binding, checkpoint=checkpoint))


class ActivationTests(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.coordinator = Coordinator("secret", max_lease_ttl_s=10, clock=self.clock)
        self.spec = service_manifest()
        self.clock.advance(10)
        self.grant = heartbeat(self.coordinator, self.spec, "pi")["assignments"][0]
        self.binding = grant_binding(self.grant)
        self.head = dict(service_id="example", sequence=1, digest="a" * 64,
                         binding=self.binding, boot_epoch=self.coordinator.boot_epoch)
        self.coordinator.checkpoint_head = lambda sid: self.head

    def test_provisional_placement_permits_preparation_but_never_effects(self):
        self.assertEqual("awaiting_state_preparation", self.grant["state"])
        self.assertFalse(self.grant["effect_ready"])
        self.assertFalse(self.grant["state_ready"])
        self.assertTrue(self.coordinator.validate_preparation(self.binding)["valid"])
        self.assertFalse(self.coordinator.validate(self.binding)["valid"])
        self.assertIsNone(self.coordinator.current_grant("example"))

    def test_assignment_explicitly_acknowledges_preparation_before_fence(self):
        proof = readiness(self.grant)
        proof.pop("fence_ack")
        grant = heartbeat(self.coordinator, self.spec, "pi", grant=self.grant,
                          **proof)["assignments"][0]
        self.assertTrue(grant["state_ready"])
        self.assertFalse(grant["effect_ready"])
        self.assertEqual("awaiting_external_fence", grant["state"])

    def test_preparation_requires_every_exact_binding_field(self):
        for key in self.binding:
            with self.subTest(key=key):
                missing = dict(self.binding)
                missing.pop(key)
                self.assertFalse(self.coordinator.validate_preparation(missing)["valid"])
                self.assertFalse(self.coordinator.validate_preparation(
                    dict(self.binding, **{key: "wrong"}))["valid"])
        self.assertFalse(self.coordinator.validate_preparation(self.grant)["valid"])
        self.coordinator.release(self.binding)
        self.assertFalse(self.coordinator.validate_preparation(self.binding)["valid"])

    def test_proof_and_fence_must_bind_same_placement_and_checkpoint(self):
        valid = readiness(self.grant)
        bad_cases = []
        for flag in ("fresh", "custody_complete", "ready"):
            bad = copy.deepcopy(valid)
            bad["state_proof"][flag] = False
            bad_cases.append(bad)
        for target in ("state_proof", "fence_ack"):
            for key in self.binding:
                bad = copy.deepcopy(valid)
                bad[target]["binding"][key] = "wrong"
                bad_cases.append(bad)
        bad = copy.deepcopy(valid)
        bad["fence_ack"]["checkpoint"] = "b" * 64
        bad_cases.append(bad)
        for bad in bad_cases:
            heartbeat(self.coordinator, self.spec, "pi", grant=self.grant, **bad)
            self.assertFalse(self.coordinator.validate(self.binding)["valid"])
        ready = heartbeat(self.coordinator, self.spec, "pi", grant=self.grant, **valid)["assignments"][0]
        self.assertEqual("starting", ready["state"])
        self.assertTrue(ready["effect_ready"])
        self.assertTrue(self.coordinator.validate(self.binding)["valid"])
        missing_revision = dict(self.binding)
        missing_revision.pop("manifest_revision")
        self.assertFalse(self.coordinator.validate(missing_revision)["valid"])
        self.assertFalse(self.coordinator.validate(ready)["valid"])
        # Successful cached fencing cannot mask a later missing state proof.
        heartbeat(self.coordinator, self.spec, "pi", grant=self.grant)
        self.assertFalse(self.coordinator.validate(self.binding)["valid"])

    def test_expiry_and_restart_never_reuse_old_state_proof(self):
        old_proof = readiness(self.grant)
        self.clock.advance(6)
        new = heartbeat(self.coordinator, self.spec, "mac")["assignments"][0]
        self.assertFalse(self.coordinator.validate_preparation(self.binding)["valid"])
        heartbeat(self.coordinator, self.spec, "mac", grant=new, **old_proof)
        self.assertFalse(self.coordinator.validate(new)["valid"])
        reboot = Coordinator("secret", max_lease_ttl_s=10, clock=self.clock)
        heartbeat(reboot, self.spec, "mac", **old_proof)
        self.clock.advance(10)
        next_grant = heartbeat(reboot, self.spec, "mac", **old_proof)["assignments"][0]
        self.assertFalse(reboot.validate(next_grant)["valid"])
        self.assertNotEqual(self.binding["boot_epoch"], next_grant["boot_epoch"])

    def test_preparation_requires_published_current_head_then_retains_baseline_as_live_state_advances(self):
        prepared = readiness(self.grant)
        self.head = None
        heartbeat(self.coordinator, self.spec, "pi", grant=self.grant, **prepared)
        self.assertFalse(self.coordinator.validate(self.binding)["valid"])
        self.head = dict(service_id="example", sequence=1, digest="b" * 64,
                         binding=self.binding, boot_epoch=self.coordinator.boot_epoch)
        self.assertFalse(self.coordinator.validate(self.binding)["valid"])
        self.head["digest"] = "a" * 64
        self.assertTrue(self.coordinator.validate(self.binding)["valid"])
        self.head = dict(self.head, sequence=2, digest="b" * 64)
        self.assertTrue(self.coordinator.validate(self.binding)["valid"])
        self.head = None
        self.assertFalse(self.coordinator.validate(self.binding)["valid"])

    def test_native_authority_and_duplicate_scope_cannot_be_shadowed(self):
        coordinator = Coordinator("secret", reserved_authorities=[AUTHORITY])
        with self.assertRaises(ValueError):
            coordinator.configure(self.spec)
        other = service_manifest(id="other")
        with self.assertRaises(ValueError):
            self.coordinator.configure(other)
        other["authority"]["id"] = "f" * 64
        other["authority"]["resources"] = ["another"]
        other["revision"] = manifest_revision(other)
        with self.assertRaises(ValueError):
            self.coordinator.configure(other)  # same provider still overlaps

    def test_managed_scope_survives_missing_workers_and_no_checkpoint_proves_freshness(self):
        heartbeat(self.coordinator, self.spec, "mac",
                  checkpoint={"sequence": 99, "digest": "a" * 64})
        self.clock.advance(600)
        self.assertIn("example", self.coordinator.status()["services"])
        self.assertIsNone(self.coordinator.current_grant("example"))
        new = heartbeat(self.coordinator, self.spec, "mac",
                        checkpoint={"sequence": 99, "digest": "a" * 64})["assignments"][0]
        self.assertFalse(new["effect_ready"])


if __name__ == "__main__":
    unittest.main()
