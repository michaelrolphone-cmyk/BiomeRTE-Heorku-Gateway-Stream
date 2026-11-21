"""Ephemeral, grant-bound forwarding to portable logical authorities.

Signed envelopes are relayed without modification. The service verifies the
original actor signature and resource scope. Every response is admitted again
after await, so a resumed old process cannot answer for a newer placement.
"""
import asyncio
import hmac
import json
import math
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
    def __init__(self, coordinator, secret, *, limit=64, timeout=5, rpc_timeout=20,
                 heartbeat=15):
        self.coordinator = coordinator
        self.secret = secret
        self.links = {}
        self.pending = {}
        self.closing = set()
        self.limit = limit
        self.timeout = timeout
        if (type(rpc_timeout) not in (int, float) or not math.isfinite(rpc_timeout)
                or rpc_timeout <= 0):
            raise ValueError("Positive authority RPC timeout required")
        self.rpc_timeout = rpc_timeout
        if (type(heartbeat) not in (int, float) or not math.isfinite(heartbeat)
                or heartbeat <= 0):
            raise ValueError("Positive authority heartbeat required")
        self.heartbeat = heartbeat
        coordinator.preparation_connected = self.preparation_connected
        coordinator.authority_transport_status = self.timeout_status

    def timeout_status(self):
        return dict(registration_timeout_s=self.timeout, rpc_timeout_s=self.rpc_timeout)

    def preparation_connected(self, binding):
        """Observe this exact registered process without recursive admission.

        Aiohttp's existing ping/pong heartbeat closes an unresponsive socket.
        Coordinator checks still own the grant, worker and checkpoint gates.
        """
        link = self.links.get(binding["service_id"])
        return bool(link and link.get("registered") and not link["socket"].closed
                    and exact_binding(link["binding"], binding))

    def scopes(self):
        self.coordinator.status()
        return {sid: item["manifest"]["authority"]
                for sid, item in self.coordinator.services.items()
                if item["manifest"].get("authority")}

    def scope(self, *, resource=None, provider=None, authority=None, controller=None):
        for sid, scope in self.scopes().items():
            if ((resource is not None and resource in scope["resources"]) or
                    (provider is not None and provider in scope["providers"]) or
                    (authority is not None and authority == scope["id"]) or
                    (controller is not None and scope.get("kind") == "controller"
                     and controller in scope["controller_aliases"])):
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
        socket = web.WebSocketResponse(heartbeat=self.heartbeat, max_msg_size=0)
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
            link = dict(socket=socket, binding=dict(binding), registered=False)
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
            link["registered"] = True
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
                                result.get("authority") != pending["authority"] or
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
            reply_type=payload["type"] + "_result", session=payload.get("session"),
            authority=payload.get("authority"))
        try:
            await link["socket"].send_json(dict(payload, id=identity,
                binding=link["binding"], preparation=preparation))
            value = await asyncio.wait_for(future, self.rpc_timeout)
            if not self._admitted(service_id, link, preparation):
                raise web.HTTPServiceUnavailable(text="Service placement changed during request")
            return value
        except (asyncio.TimeoutError, ConnectionError, RuntimeError):
            raise web.HTTPServiceUnavailable(text="Service authority is unavailable")
        finally:
            self.pending.pop(identity, None)

    async def inspect(self, authority, session):
        found = self.scope(authority=authority) or self.scope(controller=authority)
        if not found:
            self._unknown()
        return await self.rpc(found[0], dict(type="inspect", authority=authority, session=session))

    async def resolve(self, request):
        if request.headers.get("Origin"):
            raise web.HTTPForbidden()
        try:
            value = await request.json()
            if (not isinstance(value, dict) or len(value) != 1 or
                    not set(value) <= {"resource", "provider", "authority", "controller"} or
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
        result = dict(service_id=service_id, authority=scope["id"],
            binding=grant_binding(grant), lease_ttl_s=grant["lease_ttl_s"],
            state=grant["state"], effect_ready=grant["effect_ready"])
        if scope.get("kind") == "controller":
            result.update(authority_kind="controller",
                controller_aliases=list(scope["controller_aliases"]),
                call_path="/api/controller-runtimes/" + scope["id"] + "/call")
        return web.json_response(result, headers={"Cache-Control": "no-store"})

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
        if scope.get("kind") == "controller":
            raise web.HTTPNotFound(text="Controller runtime requires its exact authority route")
        if ("resource" in arguments and arguments["resource"] not in scope["resources"]):
            raise web.HTTPForbidden(text="Resource is outside the logical authority")
        provider = auth["actor"] in scope["providers"]
        if controller is None and not provider:
            raise web.HTTPForbidden(text="Logical authority is required")
        # Recovery RPCs can reach an inert authority only for declared providers.
        # The adapter restricts poll output/results to fencing and reconciliation.
        recovery = provider and operation in PREPARATION_OPERATIONS
        attachment = (provider and operation == "stream.attach" and
                      arguments.get("role") == "provider" and
                      isinstance(arguments.get("session"), str))
        active = self.coordinator.current_grant(service_id) if recovery or attachment else None
        if attachment and active is None:
            preparing = self.coordinator.current_grant(service_id, preparation=True)
            if preparing and preparing["state"] == "waiting_for_external_fence":
                # The new authority must establish a physical fence before
                # any stream can attach. Tell the provider to discard its
                # old attachment and return to the signed inbox for the fence.
                return web.json_response(
                    {"error": "Superseded stream attachment; poll for the new physical fence"},
                    status=409, headers={"Cache-Control": "no-store"})
        preparation = recovery and active is None
        value = await self.rpc(service_id, dict(type="call", envelope=envelope),
                               preparation=preparation)
        if value["status"] != 200:
            return web.json_response({"error": value.get("error", "Service rejected request")},
                                     status=value["status"])
        return web.json_response({"result": value.get("result")},
                                 headers={"Cache-Control": "no-store"})

    def _controller_scope(self, authority):
        found = self.scope(authority=authority)
        if not found or found[1].get("kind") != "controller":
            self._unknown()
        return found

    @staticmethod
    def _runtime_envelope(envelope, scope):
        if (not isinstance(envelope, dict) or
                set(envelope) - {"site", "realm", "operation", "arguments", "id", "auth", "controller"}):
            raise ValueError("Original signed site envelope required")
        auth = envelope.get("auth")
        if (not isinstance(auth, dict) or not isinstance(auth.get("actor"), str)
                or not re.fullmatch("[a-f0-9]{64}", auth["actor"])
                or not isinstance(auth.get("signature"), str) or not auth["signature"]
                or envelope.get("realm") != "site"
                or not isinstance(envelope.get("site"), str) or not envelope["site"]
                or not isinstance(envelope.get("operation"), str) or not envelope["operation"]
                or not isinstance(envelope.get("arguments"), dict)):
            raise ValueError("Original signed site envelope required")
        identity = envelope.get("id")
        if isinstance(identity, str):
            if not identity:
                raise ValueError("Original request identity required")
        elif (not isinstance(identity, dict) or set(identity) != {"stream", "sequence"}
                or type(identity["stream"]) is not int or not 0 <= identity["stream"] < 16
                or type(identity["sequence"]) is not int
                or not 1 <= identity["sequence"] <= 9223372036854775807):
            raise ValueError("Original request identity required")
        controller = envelope.get("controller")
        if controller is not None and controller not in [scope["id"]] + scope["controller_aliases"]:
            raise ValueError("Signed controller is outside this runtime authority")

    async def controller_call(self, request):
        if request.headers.get("Origin"):
            raise web.HTTPForbidden()
        service_id, scope = self._controller_scope(request.match_info["authority"])
        try:
            envelope = await request.json()
            self._runtime_envelope(envelope, scope)
        except (ValueError, TypeError):
            raise web.HTTPBadRequest(text="Exact original signed runtime envelope required")
        # The URL binds the logical authority for providers whose original
        # signed envelope has no controller field. Do not infer ownership from a
        # dynamic provider list or rewrite the signed body to add a selector.
        # Native peer authorization remains the authority's responsibility.
        value = await self.rpc(service_id, dict(type="call", envelope=envelope))
        if value["status"] != 200:
            return web.json_response({"error": value.get("error", "Runtime rejected request")},
                                     status=value["status"], headers={"Cache-Control": "no-store"})
        return web.json_response({"result": value.get("result")},
                                 headers={"Cache-Control": "no-store"})

    async def controller_pair(self, request):
        if request.headers.get("Origin"):
            raise web.HTTPForbidden()
        service_id, unused = self._controller_scope(request.match_info["authority"])
        operation = request.match_info["operation"]
        try:
            payload = await request.json()
            if not isinstance(payload, dict):
                raise ValueError()
            if operation == "begin":
                if (set(payload) != {"public_key", "label"}
                        or not isinstance(payload["public_key"], str)
                        or not 1 <= len(payload["public_key"]) <= 16384
                        or not isinstance(payload["label"], str)
                        or not 1 <= len(payload["label"]) <= 512):
                    raise ValueError()
            elif operation == "finish":
                proof = payload.get("proof")
                if (set(payload) != {"proof"} or not isinstance(proof, dict)
                        or set(proof) != {"key", "signature"}
                        or not isinstance(proof["key"], str)
                        or not re.fullmatch("[a-f0-9]{64}", proof["key"])
                        or not isinstance(proof["signature"], str)
                        or not 1 <= len(proof["signature"]) <= 16384):
                    raise ValueError()
            else:
                raise ValueError()
        except (ValueError, TypeError):
            raise web.HTTPBadRequest(text="Only native pairing begin or finish is permitted")
        # Public pairing can submit a candidate or prove possession. Approval
        # remains an authenticated operator call; this route never grants it.
        value = await self.rpc(service_id, dict(type="pair", operation=operation, payload=payload))
        if value["status"] != 200:
            return web.json_response({"error": value.get("error", "Pairing rejected")},
                                     status=value["status"], headers={"Cache-Control": "no-store"})
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
        app.router.add_post("/api/controller-runtimes/{authority}/call", self.controller_call)
        app.router.add_post("/api/controller-runtimes/{authority}/pair/{operation}", self.controller_pair)
