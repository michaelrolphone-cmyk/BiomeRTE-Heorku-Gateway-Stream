"""Read exact lease demand over the existing managed controller connection."""
import asyncio

from aiohttp import web

from .service_coordinator import REVISION

OBSERVATION_TTL_S = 6


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
    except (asyncio.TimeoutError, web.HTTPException):
        return  # Last observed permission can only last until its original deadline.
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
    while True:
        manifests = [item["manifest"] for item in coordinator.services.values()
                     if item["manifest"].get("capability_lease")]
        if manifests:
            await asyncio.gather(*(observe(coordinator, authorities, m) for m in manifests))
        await asyncio.sleep(1)
