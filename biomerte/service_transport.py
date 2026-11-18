"""Ephemeral, grant-bound forwarding to portable logical authorities.

Signed envelopes are relayed without modification. The service verifies the
original actor signature and resource scope. Every response is admitted again
after await, so a resumed old process cannot answer for a newer placement.
"""
import asyncio
import hmac
import json
import re
import uuid

from aiohttp import web, WSMsgType

from .service_coordinator import exact_binding, grant_binding
from .stream_router import service_authority_key


PROTOCOL = "biomerte-service-authority/1"
PREPARATION_OPERATIONS = frozenset((
    "cap.offer", "provider.poll", "cap.fenced", "provider.result", "cap.status"))
REPLY_STATUSES = frozenset((200, 400, 401, 403, 404, 409, 429, 503))



def authorize_coordinator(request, coordinator):
    if request.headers.get("Origin"):
        raise web.HTTPForbidden()
    if not hmac.compare_digest(request.headers.get("Authorization", ""),
                               "Bearer " + coordinator.key):
        raise web.HTTPUnauthorized()


class ServiceAuthorities:
    def __init__(self, coordinator, secret, *, limit=64, timeout=5):
        self.coordinator = coordinator
        self.secret = secret
        self.links = {}
        self.pending = {}
        self.closing = set()
        self.limit = limit
        self.timeout = timeout

    def scopes(self):
        self.coordinator.status()
        return {sid: item["manifest"]["authority"]
                for sid, item in self.coordinator.services.items()
                if item["manifest"].get("authority")}

    def scope(self, *, resource=None, provider=None, authority=None):
        for sid, scope in self.scopes().items():
            if ((resource is not None and resource in scope["resources"]) or
                    (provider is not None and provider in scope["providers"]) or
                    (authority is not None and authority == scope["id"])):
                return sid, scope
        return None

    def inspection_secret(self, authority):
        return (service_authority_key(self.secret, authority)
                if self.scope(authority=authority) else None)

    def _unknown(self):
        if self.coordinator.clock() < self.coordinator.boot_until:
            raise web.HTTPServiceUnavailable(text="Service discovery restart grace")
        raise web.HTTPNotFound(text="Scope is not managed by a portable service")

    def _admitted(self, service_id, link, preparation):
        if self.links.get(service_id) is not link or link["socket"].closed:
            return False
        binding = link["binding"]
        if not self.coordinator.validate_preparation(binding)["valid"]:
            return False
        return preparation or self.coordinator.validate(binding)["valid"]

    async def connect(self, request):
        authorize_coordinator(request, self.coordinator)
        service_id = request.match_info["service_id"]
        if service_id not in self.scopes():
            self._unknown()
        socket = web.WebSocketResponse(heartbeat=15, max_msg_size=0)
        await socket.prepare(request)
        link = None
        try:
            registration = await socket.receive_json(timeout=self.timeout)
            binding = registration.get("binding") if isinstance(registration, dict) else None
            if (not isinstance(registration, dict) or registration.get("type") != "register" or
                    not isinstance(binding, dict) or binding.get("service_id") != service_id or
                    not self.coordinator.validate_preparation(binding)["valid"]):
                await socket.close(code=1008, message=b"Current complete grant required")
                return socket
            old = self.links.get(service_id)
            if old and self._admitted(service_id, old, True):
                await socket.close(code=1008, message=b"Current authority already connected")
                return socket
            link = dict(socket=socket, binding=dict(binding))
            self.links[service_id] = link
            if old:
                closing = asyncio.create_task(old["socket"].close(
                    code=1008, message=b"Superseded service grant"))
                self.closing.add(closing)
                closing.add_done_callback(self.closing.discard)
            grant = self.coordinator.current_grant(service_id, preparation=True)
            if not grant or not exact_binding(binding, grant):
                await socket.close(code=1008, message=b"Service grant expired during registration")
                return socket
            scope = self.scopes()[service_id]
            await socket.send_json(dict(type="registered", protocol=PROTOCOL,
                binding=binding, authority=scope["id"], lease_ttl_s=grant["lease_ttl_s"]))
            async for message in socket:
                if message.type != WSMsgType.TEXT:
                    continue
                try:
                    value = json.loads(message.data)
                    if not isinstance(value, dict):
                        continue
                    pending = self.pending.get(value.get("id"))
                    if (not pending or pending["link"] is not link or
                            value.get("type") != pending["reply_type"] or
                            pending["future"].done() or
                            not exact_binding(value.get("binding"), binding) or
                            type(value.get("status")) is not int or
                            value["status"] not in REPLY_STATUSES):
                        continue
                    if value["status"] == 200 and pending["reply_type"] == "inspect_result":
                        result = value.get("grant")
                        if (not isinstance(result, dict) or
                                result.get("authority") != scope["id"] or
                                result.get("id") != pending["session"]):
                            continue
                    pending["future"].set_result(value)
                except (ValueError, TypeError):
                    continue
        except (asyncio.TimeoutError, ValueError, TypeError, ConnectionError):
            await socket.close(code=1008, message=b"Invalid service registration")
        finally:
            if link is not None:
                if self.links.get(service_id) is link:
                    self.links.pop(service_id, None)
                for pending in list(self.pending.values()):
                    if pending["link"] is link and not pending["future"].done():
                        pending["future"].set_exception(ConnectionError("Authority disconnected"))
        return socket

    async def rpc(self, service_id, payload, *, preparation=False):
        link = self.links.get(service_id)
        if not link or not self._admitted(service_id, link, preparation):
            raise web.HTTPServiceUnavailable(text="Service authority is unavailable")
        if len(self.pending) >= self.limit:
            raise web.HTTPTooManyRequests()
        identity = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[identity] = dict(link=link, future=future,
            reply_type=payload["type"] + "_result", session=payload.get("session"))
        try:
            await link["socket"].send_json(dict(payload, id=identity,
                binding=link["binding"], preparation=preparation))
            value = await asyncio.wait_for(future, self.timeout)
            if not self._admitted(service_id, link, preparation):
                raise web.HTTPServiceUnavailable(text="Service placement changed during request")
            return value
        except (asyncio.TimeoutError, ConnectionError, RuntimeError):
            raise web.HTTPServiceUnavailable(text="Service authority is unavailable")
        finally:
            self.pending.pop(identity, None)

    async def inspect(self, authority, session):
        found = self.scope(authority=authority)
        if not found:
            self._unknown()
        return await self.rpc(found[0], dict(type="inspect", authority=authority, session=session))

    async def resolve(self, request):
        if request.headers.get("Origin"):
            raise web.HTTPForbidden()
        try:
            value = await request.json()
            if (not isinstance(value, dict) or len(value) != 1 or
                    not set(value) <= {"resource", "provider", "authority"} or
                    not all(isinstance(v, str) and v for v in value.values())):
                raise ValueError()
        except (ValueError, TypeError):
            raise web.HTTPBadRequest(text="One exact scope selector is required")
        found = self.scope(**value)
        if not found:
            self._unknown()
        service_id, scope = found
        preparation = "provider" in value
        grant = self.coordinator.current_grant(service_id, preparation=preparation)
        link = self.links.get(service_id)
        if not grant or not link or not self._admitted(service_id, link, preparation):
            raise web.HTTPServiceUnavailable(text="Managed service is unavailable")
        return web.json_response(dict(service_id=service_id, authority=scope["id"],
            binding=grant_binding(grant), lease_ttl_s=grant["lease_ttl_s"],
            state=grant["state"], effect_ready=grant["effect_ready"]),
            headers={"Cache-Control": "no-store"})

    async def call(self, request):
        if request.headers.get("Origin"):
            raise web.HTTPForbidden()
        try:
            envelope = await request.json()
            if not isinstance(envelope, dict):
                raise ValueError()
            auth = envelope.get("auth")
            operation = envelope.get("operation")
            arguments = envelope.get("arguments")
            if (not isinstance(auth, dict) or
                    not isinstance(auth.get("actor"), str) or
                    not re.fullmatch("[a-f0-9]{64}", auth["actor"]) or
                    not isinstance(auth.get("signature"), str) or not auth["signature"] or
                    not isinstance(operation, str) or not isinstance(arguments, dict) or
                    not isinstance(envelope.get("id"), str) or not envelope["id"]):
                raise ValueError()
        except (ValueError, TypeError):
            raise web.HTTPBadRequest(text="Original signed envelope required")
        controller = envelope.get("controller")
        found = (self.scope(authority=controller) if controller is not None else
                 self.scope(provider=auth["actor"]))
        if not found:
            self._unknown()
        service_id, scope = found
        if ("resource" in arguments and arguments["resource"] not in scope["resources"]):
            raise web.HTTPForbidden(text="Resource is outside the logical authority")
        provider = auth["actor"] in scope["providers"]
        if controller is None and not provider:
            raise web.HTTPForbidden(text="Logical authority is required")
        # Recovery RPCs can reach an inert authority only for declared providers.
        # The adapter restricts poll output/results to fencing and reconciliation.
        preparation = (provider and operation in PREPARATION_OPERATIONS and
                       self.coordinator.current_grant(service_id) is None)
        value = await self.rpc(service_id, dict(type="call", envelope=envelope),
                               preparation=preparation)
        if value["status"] != 200:
            return web.json_response({"error": value.get("error", "Service rejected request")},
                                     status=value["status"])
        return web.json_response({"result": value.get("result")},
                                 headers={"Cache-Control": "no-store"})

    async def close(self):
        await asyncio.gather(*(item["socket"].close(code=1001, message=b"Gateway stopped")
                               for item in list(self.links.values())), return_exceptions=True)
        await asyncio.gather(*list(self.closing), return_exceptions=True)

    def mount(self, app):
        app.router.add_get("/service-authority/{service_id}/connect", self.connect)
        app.router.add_post("/api/service-authorities/resolve", self.resolve)
        app.router.add_post("/api/service-authorities/call", self.call)
