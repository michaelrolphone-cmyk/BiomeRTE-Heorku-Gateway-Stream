"""In-flight checkpoint relay to independent per-service, per-worker helpers.

The gateway holds no checkpoint journal or authoritative freshness claim.
Helpers authenticate the package, retain high water locally, and fsync before
acknowledging. A coordinator boot change invalidates every in-flight receipt.
"""
import asyncio
import base64
import hashlib
import json
import re
import uuid

from aiohttp import web, WSMsgType

from .service_transport import authorize_coordinator, REPLY_STATUSES
from .durable_witness import WitnessError


PROTOCOL = "biomerte-service-checkpoint/1"
BLOCK_BYTES = 256 * 1024


def chunk_digest(value):
    if not isinstance(value, str) or re.fullmatch("[0-9a-f]{64}", value) is None:
        raise ValueError("Exact controller block digest required")
    return value


def chunk_payload(value, data=False):
    if not isinstance(value, dict):
        raise ValueError("Controller block descriptor required")
    key = chunk_digest(value.get("digest"))
    result = {"digest": key}
    if data:
        encoded = value.get("data")
        if not isinstance(encoded, str) or len(encoded) > ((BLOCK_BYTES + 2) // 3) * 4:
            raise ValueError("Bounded controller block required")
        raw = base64.b64decode(encoded, validate=True)
        if not 0 < len(raw) <= BLOCK_BYTES or hashlib.sha256(raw).hexdigest() != key:
            raise ValueError("Controller block digest mismatch")
        result.update(data=encoded, bytes=len(raw))
    return result


def chunk_keys(value):
    if not isinstance(value, list) or len(value) > 256:
        raise ValueError("At most 256 controller block digests required")
    if len({chunk_digest(key) for key in value}) != len(value):
        raise ValueError("Duplicate controller block digest")
    return value


def checkpoint_identity(value, service_id):
    if (not isinstance(value, dict) or value.get("service_id") != service_id or
            type(value.get("sequence")) is not int or value["sequence"] < 0 or
            not isinstance(value.get("digest"), str) or
            not re.fullmatch("[0-9a-f]{64}", value["digest"])):
        raise ValueError("Exact service, nonnegative sequence and checkpoint digest required")
    return {key: value[key] for key in ("service_id", "sequence", "digest")}


def worker_identity(value):
    if (not isinstance(value, dict) or
            set(value) != {"worker_id", "worker_incarnation"} or
            any(not isinstance(v, str) or not v for v in value.values())):
        raise ValueError("Exact worker identity required")
    return value["worker_id"], value["worker_incarnation"]


class CheckpointWorkers:
    def __init__(self, coordinator, *, limit=64, timeout=10, durable_witness=None):
        self.coordinator = coordinator
        self.durable_witness = durable_witness
        self.links = {}
        self.pending = {}
        self.closing = set()
        self.limit = limit
        self.timeout = timeout
        self.heads = {}
        self.head_epoch = coordinator.boot_epoch
        coordinator.checkpoint_head = self.head

    def head(self, service_id):
        if self.head_epoch != self.coordinator.boot_epoch:
            self.heads.clear()
            self.head_epoch = self.coordinator.boot_epoch
        value = self.heads.get(service_id)
        anchor = self.durable_witness.head(service_id) if self.durable_witness else None
        if anchor and (value is None or anchor["sequence"] > value["sequence"] or
                       anchor["digest"] != value["digest"]):
            return anchor
        return value

    def _check_head(self, identity):
        previous = self.head(identity["service_id"])
        if previous and (identity["sequence"] < previous["sequence"] or
                (identity["sequence"] == previous["sequence"] and
                 identity["digest"] != previous["digest"])):
            raise web.HTTPConflict(text="Checkpoint is older than or conflicts with current high water")

    def _current(self, key, link, service_id=None):
        scope = link["service_id"]
        return ((service_id is None or service_id == scope) and
                self.links.get((key[0], scope)) is link and not link["socket"].closed and
                link["boot_epoch"] == self.coordinator.boot_epoch and
                self.coordinator.worker_current(*key, service_id=scope))

    async def _registration(self, socket, worker_id):
        """Bind a socket once. Legacy helpers prove their own nonempty scope."""
        value = await socket.receive_json()
        if not isinstance(value, dict) or value.get("type") != "register":
            raise ValueError("Worker registration required")
        key = worker_identity({k: value.get(k) for k in ("worker_id", "worker_incarnation")})
        if key[0] != worker_id or not self.coordinator.worker_current(*key):
            raise ValueError("Current supervisor incarnation required")
        epoch = self.coordinator.boot_epoch
        legacy = "service_id" not in value
        if legacy:
            # The deployed v1 helper reads requests before its registration ACK.
            # This probe is read only and stays on this exact authenticated
            # socket; no ordinary pending RPC can satisfy it or infer a scope.
            identity = uuid.uuid4().hex
            await socket.send_json(dict(type="checkpoint.status", id=identity,
                worker_id=key[0], worker_incarnation=key[1], boot_epoch=epoch,
                binding=None, checkpoint=None))
            reply = await socket.receive_json()
            if (not isinstance(reply, dict) or reply.get("type") != "checkpoint_result" or
                    reply.get("id") != identity or reply.get("worker_id") != key[0] or
                    reply.get("worker_incarnation") != key[1] or
                    reply.get("boot_epoch") != epoch or "binding" not in reply or
                    reply["binding"] is not None or type(reply.get("status")) is not int or
                    reply["status"] != 200 or not isinstance(reply.get("result"), dict) or
                    reply["result"].get("durable") is not True):
                raise ValueError("Exact durable legacy helper scope proof required")
            checkpoint = reply.get("checkpoint")
            service_id = checkpoint.get("service_id") if isinstance(checkpoint, dict) else None
            checkpoint_identity(checkpoint, service_id)
            if "service_id" in reply and reply["service_id"] != service_id:
                raise ValueError("Legacy helper scope proof disagrees")
        else:
            service_id = value["service_id"]
        if (not isinstance(service_id, str) or not service_id or
                service_id not in self.coordinator.services or
                epoch != self.coordinator.boot_epoch or
                not self.coordinator.worker_current(*key, service_id=service_id)):
            raise ValueError("Current configured service supervisor required")
        return dict(socket=socket, key=key, service_id=service_id,
                    boot_epoch=epoch, legacy=legacy)

    async def connect(self, request):
        authorize_coordinator(request, self.coordinator)
        worker_id = request.match_info["worker_id"]
        socket = web.WebSocketResponse(heartbeat=15, max_msg_size=0)
        await socket.prepare(request)
        link = None
        try:
            candidate = await asyncio.wait_for(
                self._registration(socket, worker_id), self.timeout)
            key, service_id = candidate["key"], candidate["service_id"]
            slot = (worker_id, service_id)
            # No await between the last liveness/epoch check and insertion.
            if (candidate["boot_epoch"] != self.coordinator.boot_epoch or
                    not self.coordinator.worker_current(*key, service_id=service_id)):
                raise ValueError("Helper registration changed")
            old = self.links.get(slot)
            if old and self._current(old["key"], old):
                raise ValueError("Current helper already connected for this service")
            link = candidate
            self.links[slot] = link
            if old:
                closing = asyncio.create_task(old["socket"].close(
                    code=1008, message=b"Worker incarnation superseded"))
                self.closing.add(closing)
                closing.add_done_callback(self.closing.discard)
            await socket.send_json(dict(type="registered", protocol=PROTOCOL,
                worker_id=key[0], worker_incarnation=key[1], service_id=service_id,
                boot_epoch=link["boot_epoch"]))
            async for message in socket:
                if message.type != WSMsgType.TEXT:
                    continue
                try:
                    reply = json.loads(message.data)
                    if not isinstance(reply, dict):
                        continue
                    pending = self.pending.get(reply.get("id"))
                    if (not pending or pending["link"] is not link or
                            pending["future"].done() or reply.get("type") != "checkpoint_result" or
                            reply.get("worker_id") != key[0] or
                            reply.get("worker_incarnation") != key[1] or
                            reply.get("boot_epoch") != pending["boot_epoch"] or
                            "binding" not in reply or reply["binding"] != pending["binding"] or
                            pending["service_id"] != service_id or
                            (not link["legacy"] and reply.get("service_id") != service_id) or
                            ("service_id" in reply and reply["service_id"] != service_id) or
                            type(reply.get("status")) is not int or
                            reply["status"] not in REPLY_STATUSES):
                        continue
                    if reply["status"] == 200:
                        checkpoint = reply.get("checkpoint")
                        if checkpoint is not None:
                            checkpoint_identity(checkpoint, pending["service_id"])
                        if (pending["checkpoint"] is not None and
                                checkpoint != pending["checkpoint"]):
                            continue
                        if (pending["operation"] in ("checkpoint.put", "checkpoint.published") and
                                (not isinstance(reply.get("result"), dict) or
                                 reply["result"].get("durable") is not True)):
                            continue
                        result = reply.get("result")
                        if isinstance(result, dict) and "retained" in result:
                            if not isinstance(result["retained"], list):
                                continue
                            for retained in result["retained"]:
                                checkpoint_identity(retained, service_id)
                        if pending["operation"].startswith("checkpoint.chunk."):
                            if checkpoint is not None or not isinstance(result, dict):
                                continue
                            expected_chunk = pending.get("chunk")
                            if pending["operation"] == "checkpoint.chunk.put":
                                if (type(result.get("bytes")) is not int or
                                        result.get("digest") != expected_chunk["digest"] or
                                        result.get("bytes") != expected_chunk["bytes"] or
                                        result.get("durable") is not True):
                                    continue
                            elif pending["operation"] == "checkpoint.chunk.get":
                                actual_chunk = chunk_payload(result, data=True)
                                if (type(result.get("bytes")) is not int or
                                        actual_chunk["digest"] != expected_chunk["digest"] or
                                        result.get("bytes") != actual_chunk["bytes"]):
                                    continue
                            else:
                                missing = chunk_keys(result.get("missing"))
                                if not set(missing).issubset(pending["digests"]):
                                    continue
                    pending["future"].set_result(reply)
                except (ValueError, TypeError):
                    continue
        except (ValueError, TypeError, asyncio.TimeoutError, ConnectionError):
            await socket.close(code=1008, message=b"Invalid checkpoint helper registration")
        finally:
            if link is not None:
                slot = (worker_id, link["service_id"])
                if self.links.get(slot) is link:
                    self.links.pop(slot, None)
                for pending in list(self.pending.values()):
                    if pending["link"] is link and not pending["future"].done():
                        pending["future"].set_exception(ConnectionError("Checkpoint helper disconnected"))
        return socket

    async def rpc(self, worker, operation, service_id, *, binding=None, checkpoint=None, chunk=None, digests=None):
        key = worker_identity(worker)
        link = self.links.get((key[0], service_id))
        if not link or link["key"] != key or not self._current(key, link, service_id):
            raise web.HTTPServiceUnavailable(text="Current checkpoint helper unavailable")
        if len(self.pending) >= self.limit:
            raise web.HTTPTooManyRequests()
        epoch = self.coordinator.boot_epoch
        identity = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        expected = checkpoint_identity(checkpoint, service_id) if checkpoint is not None else None
        self.pending[identity] = dict(link=link, future=future, boot_epoch=epoch,
            binding=binding, checkpoint=expected, operation=operation, service_id=service_id,
            chunk=chunk, digests=digests)
        try:
            await link["socket"].send_json(dict(type=operation, id=identity,
                worker_id=key[0], worker_incarnation=key[1], boot_epoch=epoch,
                service_id=service_id, binding=binding, checkpoint=checkpoint,
                chunk=chunk, digests=digests))
            reply = await asyncio.wait_for(future, self.timeout)
            if (epoch != self.coordinator.boot_epoch or not self._current(key, link, service_id) or
                    (binding is not None and
                     not self.coordinator.validate_preparation(binding)["valid"])):
                raise web.HTTPServiceUnavailable(text="Checkpoint placement changed during request")
            return reply
        except (asyncio.TimeoutError, ConnectionError, RuntimeError):
            raise web.HTTPServiceUnavailable(text="Checkpoint helper unavailable")
        finally:
            self.pending.pop(identity, None)

    async def request(self, request):
        authorize_coordinator(request, self.coordinator)
        try:
            body = await request.json()
            if not isinstance(body, dict):
                raise ValueError("JSON object required")
            operation = request.match_info["operation"]
            if operation == "replicate":
                binding = body.get("binding")
                if not self.coordinator.validate_preparation(binding)["valid"]:
                    raise web.HTTPConflict(text="Current complete preparation grant required")
                service_id = binding["service_id"]
                checkpoint = body.get("checkpoint")
                identity = checkpoint_identity(checkpoint, service_id)
                if not isinstance(checkpoint.get("package"), dict):
                    raise ValueError("Signed scoped checkpoint package required")
                targets = body.get("targets")
                if not isinstance(targets, list):
                    raise ValueError("targets must list exact eligible standby identities")
                if body.get("local_durable") is not True:
                    raise ValueError("Current owner local fsync acknowledgement required")
                self._check_head(identity)
                keys = [worker_identity(target) for target in targets]
                if (len(set(keys)) != len(keys) or
                        any(key[0] == binding["worker_id"] for key in keys)):
                    raise ValueError("Replica targets must be distinct standby workers")
                replies = await asyncio.gather(*(self.rpc(target, "checkpoint.put", service_id,
                    binding=binding, checkpoint=checkpoint) for target in targets))
                if not self.coordinator.validate_preparation(binding)["valid"]:
                    raise web.HTTPConflict(text="Checkpoint grant changed")
                self._check_head(identity)
                acknowledgements = []
                for target, reply in zip(targets, replies):
                    if reply["status"] != 200:
                        return web.json_response({"error": "Replica rejected checkpoint"},
                                                 status=reply["status"])
                    acknowledgements.append(dict(target, checkpoint=identity, durable=True))
                if self.durable_witness:
                    try:
                        await self.durable_witness.commit(checkpoint["package"])
                    except (WitnessError, asyncio.TimeoutError, OSError):
                        raise web.HTTPServiceUnavailable(text="Durable checkpoint witness unavailable")
                    if self.coordinator.preparation_binding(binding, self.coordinator.clock()) is None:
                        raise web.HTTPConflict(text="Checkpoint grant changed during durable publication")
                # The witness may have advanced to a head with no in-memory binding.
                # Reconcile only after publishing its exact same-grant binding in
                # this event-loop turn, with no intervening await.
                self.head(service_id)
                previous = self.heads.get(service_id)
                self.heads[service_id] = dict(identity, binding=dict(binding),
                                              boot_epoch=self.coordinator.boot_epoch)
                if self.durable_witness and not self.coordinator.validate_preparation(binding)["valid"]:
                    if previous is None:
                        self.heads.pop(service_id, None)
                    else:
                        self.heads[service_id] = previous
                    raise web.HTTPConflict(text="Checkpoint grant changed during durable publication")
                if checkpoint["package"].get("payload", {}).get("state", {}).get("format") == "biomerte-controller-state/1":
                    # This authenticated notification only permits local payload
                    # GC after head publication. It is not another commit vote.
                    await asyncio.gather(*(self.rpc(target, "checkpoint.published", service_id,
                        binding=binding, checkpoint=identity) for target in targets),
                        return_exceptions=True)
                    if (self.coordinator.boot_epoch != self.head_epoch or
                            not self.coordinator.validate_preparation(binding)["valid"]):
                        raise web.HTTPServiceUnavailable(text="Checkpoint publication epoch changed")
                return web.json_response(dict(protocol=PROTOCOL, binding=binding,
                    checkpoint=identity, head=self.heads[service_id], acknowledgements=acknowledgements),
                    headers={"Cache-Control": "no-store"})
            service_id = body.get("service_id")
            if not isinstance(service_id, str) or not service_id:
                raise ValueError("service_id is required")
            if service_id not in self.coordinator.services:
                raise web.HTTPNotFound()
            if operation in ("chunk-put", "chunk-get", "chunk-missing"):
                binding = body.get("binding")
                if (not isinstance(binding, dict) or binding.get("service_id") != service_id or
                        not self.coordinator.validate_preparation(binding)["valid"]):
                    raise web.HTTPConflict(text="Current complete preparation grant required")
                chunk, digests = None, None
                if operation == "chunk-missing":
                    digests = chunk_keys(body.get("digests"))
                else:
                    chunk = chunk_payload(body.get("chunk"), data=operation == "chunk-put")
                reply = await self.rpc(body.get("worker"),
                    "checkpoint." + operation.replace("-", "."), service_id,
                    binding=binding, chunk=chunk, digests=digests)
                return web.json_response(reply, status=reply["status"],
                                         headers={"Cache-Control": "no-store"})
            if operation == "get":
                identity = checkpoint_identity(body.get("checkpoint"), service_id)
                reply = await self.rpc(body.get("worker"), "checkpoint.get", service_id,
                                       checkpoint=identity)
                return web.json_response(reply, status=reply["status"],
                                         headers={"Cache-Control": "no-store"})
            workers = body.get("workers")
            if not isinstance(workers, list):
                raise ValueError("workers must be a list of exact supervisor identities")
            if not workers:
                workers = [dict(worker_id=link["key"][0], worker_incarnation=link["key"][1])
                    for link in self.links.values()
                    if self._current(link["key"], link, service_id)]
            if len({worker_identity(worker) for worker in workers}) != len(workers):
                raise ValueError("Duplicate checkpoint helper")
            epoch = self.coordinator.boot_epoch
            replies = await asyncio.gather(*(self.rpc(worker, "checkpoint.status", service_id)
                for worker in workers), return_exceptions=True)
            if epoch != self.coordinator.boot_epoch:
                raise web.HTTPServiceUnavailable(text="Coordinator restarted")
            values = []
            for worker, reply in zip(workers, replies):
                if isinstance(reply, BaseException):
                    values.append(dict(worker, status=503))
                else:
                    values.append(dict(worker, status=reply["status"],
                        checkpoint=reply.get("checkpoint"), result=reply.get("result")))
            return web.json_response(dict(protocol=PROTOCOL, boot_epoch=epoch,
                service_id=service_id, head=self.head(service_id), workers=values), headers={"Cache-Control": "no-store"})
        except (ValueError, TypeError, KeyError) as error:
            raise web.HTTPBadRequest(text=str(error))

    async def close(self):
        await asyncio.gather(*(link["socket"].close(code=1001, message=b"Gateway stopped")
                               for link in list(self.links.values())), return_exceptions=True)
        await asyncio.gather(*list(self.closing), return_exceptions=True)

    def mount(self, app):
        app.router.add_get("/service-worker/{worker_id}/connect", self.connect)
        app.router.add_post("/api/service-checkpoints/{operation:replicate|get|status|chunk-put|chunk-get|chunk-missing}", self.request)
