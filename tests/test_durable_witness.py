import asyncio
import copy
import unittest

from biomerte.durable_witness import DurableWitness, WitnessError, PROTOCOL
from biomerte.service_coordinator import Coordinator
from test_service_coordinator import Clock, manifest, heartbeat

SCOPE = "d" * 64


def packet(sequence, digest, parent=""):
    return {"digest": digest, "payload": {"service_id": "example",
        "scope_digest": SCOPE, "sequence": sequence, "previous_digest": parent}}


class Metadata:
    """HTTP-independent CAS fixture; real SQL is covered by the gateway tests."""
    def __init__(self, clock):
        self.clock, self.lease, self.head = clock, None, None
        self.fail_after_commit = self.fail_heads = False
        self.response_latency = 0

    async def request(self, operation, **body):
        now = self.clock()
        if operation == "acquire":
            if self.lease and self.lease["expires_at"] > now and self.lease["boot_id"] != body["boot_id"]:
                raise WitnessError(409)
            if self.lease is None or self.lease["expires_at"] <= now:
                epoch = 1 if self.lease is None else self.lease["epoch"] + 1
                self.lease = dict(witness="biomerte-services", epoch=epoch, boot_id=body["boot_id"],
                    expires_at=now + body["lease_s"], ready_at=now + body["max_grant_s"],
                    max_grant_s=body["max_grant_s"])
            value = dict(self.lease, server_now=now)
            self.clock.advance(self.response_latency)
            return dict(protocol=PROTOCOL, lease=value)
        owner = body["owner"]
        if (not self.lease or self.lease["expires_at"] <= now or
                owner != {key: self.lease[key] for key in ("boot_id", "epoch")}):
            raise WitnessError(409)
        if operation == "renew":
            self.lease["expires_at"] = now + body["lease_s"]
            return dict(protocol=PROTOCOL, lease=dict(self.lease, server_now=now))
        if operation == "heads":
            if self.fail_heads:
                raise asyncio.TimeoutError()
            return dict(protocol=PROTOCOL, heads=[copy.deepcopy(self.head)] if self.head else [], next=None)
        if operation == "commit":
            candidate = dict(body["checkpoint"], service_id=body["service_id"], scope_digest=body["scope_digest"])
            same = self.head == candidate
            if self.head and not same and (
                    body["expected"] != {key: self.head[key] for key in ("sequence", "digest")} or
                    candidate["sequence"] <= self.head["sequence"] or
                    candidate["scope_digest"] != self.head["scope_digest"] or
                    body["previous_digest"] != self.head["digest"]):
                raise WitnessError(409, {"head": copy.deepcopy(self.head)})
            self.head = candidate
            if self.fail_after_commit:
                self.fail_after_commit = False
                raise asyncio.TimeoutError()
            return dict(protocol=PROTOCOL, head=copy.deepcopy(self.head), durable=True)
        raise AssertionError(operation)


class DurableWitnessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clock = Clock()
        self.coordinator = Coordinator("coordinator", max_lease_ttl_s=10, clock=self.clock)
        self.metadata = Metadata(self.clock)
        self.witness = self.client(self.coordinator)
        await self.witness.refresh()
        self.clock.advance(10)

    def client(self, coordinator):
        client = DurableWitness(None, coordinator, "https://gateway.test", "w" * 48, lease_s=40)
        client.request = self.metadata.request
        return client

    async def test_lost_cas_response_blocks_replacement_until_exact_retry_is_reconciled(self):
        await self.witness.commit(packet(0, "a" * 64))
        self.metadata.fail_after_commit = True
        with self.assertRaises(asyncio.TimeoutError):
            await self.witness.commit(packet(1, "b" * 64, "a" * 64))
        self.assertTrue(self.coordinator.publication_pending("example"))
        self.assertEqual("a" * 64, self.witness.head("example")["digest"])
        await self.witness.commit(packet(1, "b" * 64, "a" * 64))
        self.assertFalse(self.coordinator.publication_pending("example"))
        self.assertEqual("b" * 64, self.witness.head("example")["digest"])

    async def test_restart_loads_exact_durable_anchor_without_any_worker_assertion(self):
        await self.witness.commit(packet(0, "a" * 64))
        old_epoch = self.coordinator.boot_epoch
        self.clock.advance(41)
        restarted = Coordinator("coordinator", max_lease_ttl_s=10, clock=self.clock)
        survivor = self.client(restarted)
        await survivor.refresh()
        anchor = survivor.head("example")
        self.assertEqual("a" * 64, anchor["digest"])
        self.assertTrue(anchor["durable_witness"])
        self.assertNotEqual(old_epoch, anchor["boot_epoch"])
        self.assertGreaterEqual(restarted.boot_until - self.clock(), 10)
        self.assertIsNone(self.witness.head("example"))

    async def test_incomplete_head_load_never_enables_a_lease_on_retry(self):
        self.clock.advance(41)
        restarted = Coordinator("coordinator", max_lease_ttl_s=10, clock=self.clock)
        client = self.client(restarted)
        self.metadata.fail_heads = True
        with self.assertRaises(asyncio.TimeoutError):
            await client.refresh()
        self.assertFalse(client.current())
        self.assertIsNone(client.loaded_epoch)
        self.metadata.fail_heads = False
        await client.refresh()
        self.assertTrue(client.current())
        self.assertEqual(client.epoch, client.loaded_epoch)

    async def test_grant_lifetime_is_capped_by_durable_lease_and_old_owner_cannot_extend_it(self):
        self.clock.advance(27)
        spec = manifest()
        first = heartbeat(self.coordinator, spec, "pi")["assignments"][0]
        self.assertLessEqual(first["lease_ttl_s"], 3)
        self.clock.advance(2)
        renewed = heartbeat(self.coordinator, spec, "pi", grant=first, healthy=True)["assignments"][0]
        self.assertLessEqual(renewed["lease_ttl_s"], 1)
        self.clock.advance(1)
        self.assertFalse(self.coordinator.validate(renewed)["valid"])
        self.assertEqual([], heartbeat(self.coordinator, spec, "mac")["assignments"])

    async def test_unknown_cas_prevents_new_grant_without_extending_the_expired_owner(self):
        spec = manifest()
        old = heartbeat(self.coordinator, spec, "pi")["assignments"][0]
        self.witness.pending["example"] = {"checkpoint": {"sequence": 1, "digest": "b" * 64}}
        self.clock.advance(6)
        result = heartbeat(self.coordinator, spec, "mac")
        self.assertEqual([], result["assignments"])
        self.assertFalse(self.coordinator.validate(old)["valid"])
        self.assertEqual("checkpoint_publication_reconciliation", result["statuses"]["example"]["state"])
        self.witness.pending.clear()
        self.assertEqual("mac", heartbeat(self.coordinator, spec, "mac")["assignments"][0]["worker_id"])

    async def test_wrong_parent_cannot_rebase_stale_controller_state_onto_newer_head(self):
        await self.witness.commit(packet(0, "a" * 64))
        await self.witness.commit(packet(1, "b" * 64, "a" * 64))
        with self.assertRaises(WitnessError):
            await self.witness.commit(packet(2, "c" * 64, "a" * 64))
        self.assertEqual("b" * 64, self.metadata.head["digest"])

    async def test_explicit_lease_refusal_immediately_fences_cached_authority(self):
        self.assertTrue(self.witness.current())
        self.metadata.lease["boot_id"] = "replacement-witness"
        with self.assertRaises(WitnessError):
            await self.witness.refresh()
        self.assertFalse(self.witness.current())
        self.assertEqual(0, self.coordinator.witness_deadline())

    async def test_network_latency_never_extends_witness_authority(self):
        self.clock.advance(41)
        restarted = Coordinator("coordinator", max_lease_ttl_s=10, clock=self.clock)
        client = self.client(restarted)
        self.metadata.response_latency = 5
        started = self.clock()
        await client.refresh()
        self.assertEqual(started + 40, client.deadline)
        self.assertEqual(35, client.deadline - self.clock())


if __name__ == "__main__":
    unittest.main()
