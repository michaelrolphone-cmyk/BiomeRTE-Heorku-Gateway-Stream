"""Read exact lease demand over the existing managed controller connection."""
import asyncio
import logging

from aiohttp import web

from .service_coordinator import REVISION

LOG = logging.getLogger(__name__)
OBSERVATION_TTL_S = 6
POLL_INTERVAL_S = 1


async def observe(coordinator, authorities, manifest):
    scope = manifest["capability_lease"]
    found = authorities.scope(authority=scope["controller"])
    if not found or found[1].get("kind") != "controller":
        return
    started = coordinator.clock()
    arguments = {key: scope[key] for key in ("provider", "resource", "contract")}
    try:
        reply = await asyncio.wait_for(
            authorities.rpc(found[0], dict(type="capability_demand", scope=arguments)),
            timeout=OBSERVATION_TTL_S - 1)
    except (OSError, asyncio.TimeoutError, web.HTTPException):
        return  # Last observed permission can only last until its original deadline.
    if not isinstance(reply, dict):
        return
    result = reply.get("result")
    if reply.get("status") != 200 or not isinstance(result, dict) or "lease" not in result:
        return
    lease = result["lease"]
    binding = None
    if lease is not None:
        if not isinstance(lease, dict):
            return
        lease_id, generation = lease.get("id"), lease.get("generation")
        if (not isinstance(lease_id, str) or not REVISION.fullmatch(lease_id) or
                type(generation) is not int or generation < 1):
            return
        binding = (lease_id, generation)
    coordinator.record_capability_demand(manifest["id"], manifest["revision"],
                                         binding, started + OBSERVATION_TTL_S)


async def watch(coordinator, authorities):
    pending = {}
    try:
        while True:
            current = {(m["id"], m["revision"]): m for item in coordinator.services.values()
                       for m in (item["manifest"],) if m.get("capability_lease")}
            for key, task in list(pending.items()):
                if key not in current:
                    task.cancel()
                if task.done():
                    del pending[key]
                    if not task.cancelled() and task.exception() is not None:
                        LOG.warning("Lease observation for %s failed (%s)", key[0],
                                    type(task.exception()).__name__)
            for key, manifest in current.items():
                if key not in pending:
                    pending[key] = asyncio.create_task(observe(coordinator, authorities, manifest))
            await asyncio.sleep(POLL_INTERVAL_S)
    finally:
        for task in pending.values():
            task.cancel()
        await asyncio.gather(*pending.values(), return_exceptions=True)
