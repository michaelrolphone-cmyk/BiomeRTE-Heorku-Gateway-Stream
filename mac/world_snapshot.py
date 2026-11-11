"""Read a single signed, revision-pinned atlas page off the event loop."""
import asyncio
import pathlib
from aiohttp import web

async def world_snapshot(body):
    if set(body) - {'world', 'cursor'} or body.get('world') is not True:
        raise web.HTTPBadRequest(text='Invalid world map request')
    cursor = body.get('cursor')
    if cursor is not None and (not isinstance(cursor, str) or not cursor or len(cursor)>30000):
        raise web.HTTPBadRequest(text='Invalid world map cursor')
    # Uses the same authenticated local native service as local map snapshots.
    # No browser lease, controller dispatch, or checkpoint copying is involved.
    from biome_provider import VisionClient
    client=VisionClient(pathlib.Path.home()/'.local/share/biome-robot-vision/vision.sock',timeout=10)
    inputs={'limit':500}
    if cursor is not None:inputs['cursor']=cursor
    try:
        data=await asyncio.to_thread(client.query,'robot.world.read',inputs)
    except Exception:
        raise web.HTTPServiceUnavailable(text='World map page unavailable')
    return web.json_response({'type':'world_snapshot','data':data})
