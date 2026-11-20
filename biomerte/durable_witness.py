"""Durable latest-reference witness backed by authenticated Sites metadata.

The store contains no checkpoint payloads or execution history. A publication
whose response is unknown blocks replacement grants until the exact CAS is
reconciled or a newer durable epoch fences the old request.
"""
import asyncio
import contextlib
import math
import re
import uuid
from urllib.parse import urlsplit

from aiohttp import ClientTimeout

PROTOCOL = "biomerte-durable-witness/1"
HASH = re.compile("[0-9a-f]{64}")


class WitnessError(ConnectionError):
    def __init__(self, status=503, value=None):
        super().__init__("Durable witness unavailable" if status != 409 else "Durable witness conflict")
        self.status, self.value = status, value or {}


def exact_head(value):
    if (not isinstance(value, dict) or not isinstance(value.get("service_id"), str)
            or not isinstance(value.get("scope_digest"), str)
            or HASH.fullmatch(value["scope_digest"]) is None
            or type(value.get("sequence")) is not int or value["sequence"] < 0
            or not isinstance(value.get("digest"), str) or HASH.fullmatch(value["digest"]) is None):
        raise WitnessError()
    return {key: value[key] for key in ("service_id", "scope_digest", "sequence", "digest")}


class DurableWitness:
    def __init__(self, session, coordinator, origin, secret, *,
                 witness_id="biomerte-services", lease_s=120, timeout=10):
        parts = urlsplit(origin)
        if (parts.scheme != "https" or not parts.netloc or parts.username or parts.password
                or parts.path not in ("", "/") or parts.query or parts.fragment):
            raise ValueError("HTTPS durable witness origin required")
        if not isinstance(secret, str) or len(secret) < 32:
            raise ValueError("Separate durable witness credential required")
        if (type(lease_s) not in (int, float) or not math.isfinite(lease_s)
                or not 2 * coordinator.max_lease_ttl_s <= lease_s <= 600):
            raise ValueError("Witness lease must cover twice the maximum grant interval")
        if not isinstance(witness_id, str) or re.fullmatch("[A-Za-z0-9_.:-]{1,128}", witness_id) is None:
            raise ValueError("Exact durable witness identity required")
        self.session, self.coordinator = session, coordinator
        self.url, self.secret = origin.rstrip("/") + "/internal/service-witness/v1", secret
        self.witness_id, self.lease_s, self.timeout = witness_id, lease_s, timeout
        self.boot_id, self.epoch, self.deadline = uuid.uuid4().hex, None, 0
        self.loaded_epoch = None
        self.heads, self.pending = {}, {}
        self.lock, self.task = asyncio.Lock(), None
        coordinator.witness_deadline = lambda: self.deadline
        coordinator.publication_pending = lambda service_id: service_id in self.pending

    def current(self):
        return self.epoch is not None and self.coordinator.clock() < self.deadline

    def owner(self):
        return {"boot_id": self.boot_id, "epoch": self.epoch}

    async def request(self, operation, **body):
        async with self.session.post(self.url, json=dict(
                protocol=PROTOCOL, operation=operation, witness=self.witness_id, **body),
                headers={"Authorization": "Bearer " + self.secret},
                timeout=ClientTimeout(total=self.timeout), allow_redirects=False) as response:
            try:
                value = await response.json()
            except (ValueError, TypeError):
                raise WitnessError(response.status)
            if response.status != 200:
                raise WitnessError(response.status, value if isinstance(value, dict) else None)
            if not isinstance(value, dict) or value.get("protocol") != PROTOCOL:
                raise WitnessError()
            return value

    async def _load_heads(self):
        values, cursor = {}, ""
        while True:
            response = await self.request("heads", owner=self.owner(), cursor=cursor)
            rows = response.get("heads")
            if not isinstance(rows, list) or len(rows) > 64:
                raise WitnessError()
            previous = cursor
            for raw in rows:
                head = exact_head(raw)
                if head["service_id"] <= previous or head["service_id"] in values:
                    raise WitnessError()
                previous = head["service_id"]
                values[head["service_id"]] = head
            next_cursor = response.get("next")
            if next_cursor is None:
                return values
            if not rows or next_cursor != previous:
                raise WitnessError()
            cursor = next_cursor

    async def _lease(self, acquiring):
        started = self.coordinator.clock()
        operation = "acquire" if acquiring else "renew"
        body = dict(boot_id=self.boot_id, max_grant_s=self.coordinator.max_lease_ttl_s) if acquiring else dict(owner=self.owner())
        try:
            response = await self.request(operation, lease_s=self.lease_s, **body)
        except WitnessError as exc:
            if exc.status == 409:
                self.deadline = 0  # explicit epoch refusal overrides cached time
            raise
        lease = response.get("lease")
        if (not isinstance(lease, dict) or lease.get("witness") != self.witness_id
                or lease.get("boot_id") != self.boot_id or type(lease.get("epoch")) is not int
                or lease["epoch"] < 1 or any(type(lease.get(key)) not in (int, float)
                    or not math.isfinite(lease[key])
                    for key in ("server_now", "expires_at", "ready_at", "max_grant_s"))
                or lease["max_grant_s"] != self.coordinator.max_lease_ttl_s):
            raise WitnessError()
        remaining = lease["expires_at"] - lease["server_now"]
        deadline = started + remaining  # response latency never extends authority
        if not 0 < remaining <= self.lease_s or self.coordinator.clock() >= deadline:
            raise WitnessError()
        new_epoch = lease["epoch"] != self.loaded_epoch
        if new_epoch:
            self.deadline = 0
            self.epoch = lease["epoch"]
            heads = await self._load_heads()
            if self.coordinator.clock() >= deadline:
                raise WitnessError()
            self.heads = heads
            self.loaded_epoch = self.epoch
            self.pending.clear()  # old delayed mutations are fenced by D1's new epoch
            self.coordinator.boot_epoch = self.boot_id + "-" + str(self.epoch)
            self.coordinator.boot_until = self.coordinator.clock() + max(
                0, lease["ready_at"] - lease["server_now"])
            for item in self.coordinator.services.values():
                item["grant"] = None
                item["reason"] = "restart_grace"
        self.deadline = deadline

    def head(self, service_id):
        value = self.heads.get(service_id)
        if not self.current() or value is None:
            return None
        return dict(value, boot_epoch=self.coordinator.boot_epoch, binding=None,
                    durable_witness=True, witness_epoch=self.epoch)

    async def _commit_pending(self, service_id):
        candidate = self.pending[service_id]
        epoch = self.epoch
        try:
            response = await self.request("commit", owner=self.owner(), **candidate)
        except WitnessError as exc:
            # A returned higher/conflicting reference makes every delayed copy
            # of this same CAS permanently inadmissible. Other unknown outcomes
            # retain the publication barrier, including a timed-out HTTP write.
            raw = exc.value.get("head") if exc.status == 409 else None
            if exc.status == 409:
                if not isinstance(raw, dict):
                    self.deadline = 0  # owner/epoch/quarantine refusal is definitive
                else:
                    try:
                        head = exact_head(raw)
                    except WitnessError:
                        self.deadline = 0
                        raise
                    if head["service_id"] != service_id:
                        self.deadline = 0
                        raise
                    target = candidate["checkpoint"]
                    if (head["scope_digest"] != candidate["scope_digest"] or
                            head["sequence"] >= target["sequence"]):
                        self.heads[service_id] = head
                        self.pending.pop(service_id, None)
            raise
        head = exact_head(response.get("head"))
        expected = dict(candidate["checkpoint"], service_id=service_id,
                        scope_digest=candidate["scope_digest"])
        if (response.get("durable") is not True or head != expected
                or epoch != self.epoch or not self.current()):
            raise WitnessError()
        self.heads[service_id] = head
        self.pending.pop(service_id, None)
        return head

    async def commit(self, packet):
        payload = packet.get("payload", {})
        service_id = payload.get("service_id")
        target = exact_head(dict(service_id=service_id, scope_digest=payload.get("scope_digest"),
                                 sequence=payload.get("sequence"), digest=packet.get("digest")))
        async with self.lock:
            if not self.current():
                raise WitnessError()
            previous = self.heads.get(service_id)
            same = previous == target
            if previous and not same and (
                    previous["scope_digest"] != target["scope_digest"]
                    or target["sequence"] <= previous["sequence"]
                    or payload.get("previous_digest") != previous["digest"]):
                raise WitnessError(409, {"head": previous})
            expected = ({key: previous[key] for key in ("sequence", "digest")} if previous else None)
            candidate = dict(service_id=service_id, scope_digest=target["scope_digest"],
                checkpoint={key: target[key] for key in ("sequence", "digest")},
                expected=expected, previous_digest=payload.get("previous_digest", ""))
            retained = self.pending.get(service_id)
            if retained is not None:
                if retained["checkpoint"] != candidate["checkpoint"] or retained["scope_digest"] != candidate["scope_digest"]:
                    raise WitnessError()
            else:
                self.pending[service_id] = candidate
            return await self._commit_pending(service_id)

    async def refresh(self):
        async with self.lock:
            await self._lease(not self.current())
            for service_id in list(self.pending):
                await self._commit_pending(service_id)

    async def loop(self):
        while True:
            try:
                await self.refresh()
            except asyncio.CancelledError:
                raise
            except Exception:
                # No exception/body/credential logging. The deadline and pending
                # barriers preserve authority while the transport recovers.
                pass
            await asyncio.sleep(min(2, self.lease_s / 4))

    def start(self):
        self.task = asyncio.create_task(self.loop())

    async def close(self):
        if self.task:
            self.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self.task
        self.deadline = 0
