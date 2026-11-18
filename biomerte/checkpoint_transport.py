"""In-flight checkpoint relay to independent per-worker Docker helpers.

The gateway holds no checkpoint journal or authoritative freshness claim.
Helpers authenticate the package, retain high water locally, and fsync before
acknowledging. A coordinator boot change invalidates every in-flight receipt.
"""
import asyncio
import json
import re
import uuid

from aiohttp import web, WSMsgType

from .service_transport import authorize_coordinator, REPLY_STATUSES


PROTOCOL = "biomerte-service-checkpoint/1"


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
    def __init__(self, coordinator, *, limit=64, timeout=10):
        self.coordinator = coordinator
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
        return self.heads.get(service_id)

    def _check_head(self, identity):
        previous = self.head(identity["service_id"])
        if previous and (identity["sequence"] < previous["sequence"] or
                (identity["sequence"] == previous["sequence"] and
                 identity["digest"] != previous["digest"])):
            raise web.HTTPConflict(text="Checkpoint is older than or conflicts with current high water")

    def _current(self, key, link, service_id=None):
        return (self.links.get(key[0]) is link and not link["socket"].closed and
                self.coordinator.worker_current(*key, service_id=service_id))

    async def connect(self, request):
        authorize_coordinator(request, self.coordinator)
        worker_id = request.match_info["worker_id"]
        socket = web.WebSocketResponse(heartbeat=15, max_msg_size=0)
        await socket.prepare(request)
        link = None
        try:
            value = await socket.receive_json(timeout=self.timeout)
            if not isinstance(value, dict) or value.get("type") != "register":
                raise ValueError("Worker registration required")
            key = worker_identity({k: value.get(k) for k in ("worker_id", "worker_incarnation")})
            if key[0] != worker_id or not self.coordinator.worker_current(*key):
                raise ValueError("Current supervisor incarnation required")
            old = self.links.get(worker_id)
            if old and self._current(old["key"], old):
                raise ValueError("Current helper already connected")
            link = dict(socket=socket, key=key)
            self.links[worker_id] = link
            if old:
                closing = asyncio.create_task(old["socket"].close(
                    code=1008, message=b"Worker incarnation superseded"))
                self.closing.add(closing)
                closing.add_done_callback(self.closing.discard)
            await socket.send_json(dict(type="registered", protocol=PROTOCOL,
                worker_id=key[0], worker_incarnation=key[1],
                boot_epoch=self.coordinator.boot_epoch))
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
                            reply.get("binding") != pending["binding"] or
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
                        if (pending["operation"] == "checkpoint.put" and
                                (not isinstance(reply.get("result"), dict) or
                                 reply["result"].get("durable") is not True)):
                            continue
                    pending["future"].set_result(reply)
                except (ValueError, TypeError):
                    continue
        except (ValueError, TypeError, asyncio.TimeoutError, ConnectionError):
            await socket.close(code=1008, message=b"Invalid checkpoint helper registration")
        finally:
            if link is not None:
                if self.links.get(worker_id) is link:
                    self.links.pop(worker_id, None)
                for pending in list(self.pending.values()):
                    if pending["link"] is link and not pending["future"].done():
                        pending["future"].set_exception(ConnectionError("Checkpoint helper disconnected"))
        return socket

    async def rpc(self, worker, operation, service_id, *, binding=None, checkpoint=None):
        key = worker_identity(worker)
        link = self.links.get(key[0])
        if not link or link["key"] != key or not self._current(key, link, service_id):
            raise web.HTTPServiceUnavailable(text="Current checkpoint helper unavailable")
        if len(self.pending) >= self.limit:
            raise web.HTTPTooManyRequests()
        epoch = self.coordinator.boot_epoch
        identity = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        expected = checkpoint_identity(checkpoint, service_id) if checkpoint is not None else None
        self.pending[identity] = dict(link=link, future=future, boot_epoch=epoch,
            binding=binding, checkpoint=expected, operation=operation, service_id=service_id)
        try:
            await link["socket"].send_json(dict(type=operation, id=identity,
                worker_id=key[0], worker_incarnation=key[1], boot_epoch=epoch,
                service_id=service_id, binding=binding, checkpoint=checkpoint))
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
                self.heads[service_id] = dict(identity, binding=dict(binding),
                                              boot_epoch=self.coordinator.boot_epoch)
                return web.json_response(dict(protocol=PROTOCOL, binding=binding,
                    checkpoint=identity, head=self.heads[service_id], acknowledgements=acknowledgements),
                    headers={"Cache-Control": "no-store"})
            service_id = body.get("service_id")
            if not isinstance(service_id, str) or not service_id:
                raise ValueError("service_id is required")
            if service_id not in self.coordinator.services:
                raise web.HTTPNotFound()
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
        app.router.add_post("/api/service-checkpoints/{operation:replicate|get|status}", self.request)
