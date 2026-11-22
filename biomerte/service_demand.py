"""Read exact lease demand over the existing managed controller connection."""
import asyncio
import logging

from aiohttp import web

from .service_coordinator import REVISION

LOG = logging.getLogger(__name__)
POLL_INTERVAL_S = 1


async def observe(coordinator, authorities, manifests):
    if isinstance(manifests, dict):
        manifests = [manifests]
    scope = manifests[0]["capability_lease"]
    found = authorities.scope(authority=scope["controller"])
    if not found or found[1].get("kind") != "controller":
        return
    started = coordinator.clock()
    # The authorization budget begins before the RPC, so transport latency is charged.
    # Each pooled slot keeps the lifetime declared by its own service manifest.
    lifetimes = {m["id"]: min(m.get("lease_ttl_s", 10), coordinator.max_lease_ttl_s)
                 for m in manifests}
    arguments = {key: scope[key] for key in ("provider", "resource", "contract")}
    pooled = "slot" in scope
    if pooled:
        arguments["slots"] = sorted({m["capability_lease"]["slot"] for m in manifests})
    try:
        reply = await asyncio.wait_for(
            authorities.rpc(found[0], dict(type="capability_demand", scope=arguments)),
            timeout=max(lifetimes.values()))
    except (OSError, asyncio.TimeoutError, web.HTTPException):
        return
    if not isinstance(reply, dict) or reply.get("status") != 200:
        return
    result = reply.get("result")
    if not isinstance(result, dict):
        return
    for manifest in manifests:
        values = result.get("slots") if pooled else result
        key = str(manifest["capability_lease"]["slot"]) if pooled else "lease"
        if not isinstance(values, dict) or key not in values:
            continue
        lease = values[key]
        binding = None
        if lease is not None:
            if not isinstance(lease, dict):
                continue
            lease_id, generation = lease.get("id"), lease.get("generation")
            if (not isinstance(lease_id, str) or not REVISION.fullmatch(lease_id) or
                    type(generation) is not int or generation < 1):
                continue
            binding = (lease_id, generation)
        coordinator.record_capability_demand(manifest["id"], manifest["revision"],
                                             binding, started + lifetimes[manifest["id"]])


async def watch(coordinator, authorities):
    pending = {}
    try:
        while True:
            groups = {}
            for item in coordinator.services.values():
                manifest = item["manifest"]
                scope = manifest.get("capability_lease")
                if scope:
                    identity = tuple(scope[k] for k in ("controller", "provider", "resource", "contract"))
                    groups.setdefault((identity, "slot" in scope), []).append(manifest)
            current = {tuple(sorted((m["id"], m["revision"]) for m in group)): group
                       for group in groups.values()}
            for key, task in list(pending.items()):
                if key not in current:
                    task.cancel()
                if task.done():
                    del pending[key]
                    if not task.cancelled() and task.exception() is not None:
                        LOG.warning("Lease observation for %s failed (%s)", key[0],
                                    type(task.exception()).__name__)
            for key, group in current.items():
                if key not in pending:
                    pending[key] = asyncio.create_task(observe(coordinator, authorities, group))
            await asyncio.sleep(POLL_INTERVAL_S)
    finally:
        for task in pending.values():
            task.cancel()
        await asyncio.gather(*pending.values(), return_exceptions=True)
