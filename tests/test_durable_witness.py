import asyncio
import copy
import unittest
from unittest.mock import AsyncMock, Mock

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
        if operation == "release":
            self.lease["expires_at"] = 0
            return dict(protocol=PROTOCOL, released=True)
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

    async def test_compact_and_full_publications_share_the_same_lineage_and_retry(self):
        await self.witness.commit(packet(1, "a" * 64))
        target = dict(service_id="example", sequence=2, digest="b"*64, scope_digest=SCOPE)
        with self.assertRaises(WitnessError):
            await self.witness.commit_reference(target, "0" * 64)
        with self.assertRaises(WitnessError):
            await self.witness.commit_reference(dict(target, scope_digest="0"*64), "a"*64)
        self.metadata.fail_after_commit = True
        with self.assertRaises(asyncio.TimeoutError):
            await self.witness.commit_reference(target, "a"*64)
        self.assertEqual(target, await self.witness.commit(packet(2, "b"*64, "a"*64)))
        self.assertEqual(target, self.metadata.head)

    async def test_http_request_identifies_client_and_preserves_authentication(self):
        answer = dict(protocol=PROTOCOL, heads=[], next=None)
        response = Mock(status=200)
        response.json = AsyncMock(return_value=answer)
        context = AsyncMock()
        context.__aenter__.return_value = response
        session = Mock()
        session.post.return_value = context
        client = DurableWitness(session, self.coordinator, "https://gateway.test",
                                "w" * 48, lease_s=40)
        owner = dict(boot_id="probe-owner-0000000000000000", epoch=1)

        self.assertEqual(answer, await client.request("heads", owner=owner))

        session.post.assert_called_once()
        args, kwargs = session.post.call_args
        self.assertEqual(("https://gateway.test/internal/service-witness/v1",), args)
        self.assertEqual({"Authorization": "Bearer " + "w" * 48,
                          "User-Agent": "BioMRT-Durable-Witness/1"}, kwargs["headers"])
        self.assertEqual(dict(protocol=PROTOCOL, operation="heads",
                              witness="biomerte-services", owner=owner), kwargs["json"])
        self.assertFalse(kwargs["allow_redirects"])
        self.assertEqual(client.timeout, kwargs["timeout"].total)

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

    async def test_clean_exit_retires_boot_but_keeps_checkpoint_and_full_grant_grace(self):
        await self.witness.commit(packet(0, "a" * 64))
        await self.witness.close()
        self.assertFalse(self.witness.current())
        restarted = Coordinator("coordinator", max_lease_ttl_s=10, clock=self.clock)
        survivor = self.client(restarted)
        await survivor.refresh()
        self.assertEqual(survivor.head("example")["digest"], "a" * 64)
        self.assertGreaterEqual(restarted.boot_until - self.clock(), 10)
        with self.assertRaises(WitnessError):
            await self.witness.refresh()

    async def test_unknown_release_response_keeps_the_previous_expiry_protection(self):
        request = self.witness.request
        async def unavailable(operation, **kwargs):
            if operation == "release":
                raise asyncio.TimeoutError()
            return await request(operation, **kwargs)
        self.witness.request = unavailable
        await self.witness.close()
        successor = self.client(Coordinator("coordinator", max_lease_ttl_s=10, clock=self.clock))
        with self.assertRaises(WitnessError):
            await successor.refresh()
        self.clock.advance(41)
        await successor.refresh()
        self.assertTrue(successor.current())

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

    async def test_explicit_commit_refusal_immediately_fences_cached_authority(self):
        self.assertTrue(self.witness.current())
        self.metadata.lease["boot_id"] = "replacement-witness"
        with self.assertRaises(WitnessError):
            await self.witness.commit(packet(0, "a" * 64))
        self.assertFalse(self.witness.current())
        self.assertEqual(0, self.coordinator.witness_deadline())
        self.assertTrue(self.coordinator.publication_pending("example"))

    async def test_checkpoint_conflict_preserves_lease_and_adopts_exact_newer_head(self):
        await self.witness.commit(packet(0, "a" * 64))
        self.metadata.head = dict(service_id="example", scope_digest=SCOPE,
                                  sequence=2, digest="c" * 64)
        deadline = self.witness.deadline
        with self.assertRaises(WitnessError):
            await self.witness.commit(packet(1, "b" * 64, "a" * 64))
        self.assertTrue(self.witness.current())
        self.assertEqual(deadline, self.witness.deadline)
        self.assertEqual("c" * 64, self.witness.head("example")["digest"])
        self.assertFalse(self.coordinator.publication_pending("example"))

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
