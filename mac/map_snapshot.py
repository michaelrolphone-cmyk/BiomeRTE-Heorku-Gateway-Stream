"""Complete revision-pinned local map pages, without controller jobs."""
import asyncio,json
from aiohttp import web

def reader(client):
    async def snapshot(map_id, revision):
        if not isinstance(map_id,str) or type(revision) is not int or revision<0:
            raise web.HTTPBadRequest()
        entities, cursor, seen, metadata = [], None, set(), None
        while True:
            inputs={'map_id':map_id,'revision':revision,'limit':500}
            if cursor is not None:
                inputs['cursor']=cursor
            page=await asyncio.to_thread(client.query,'robot.map.read',inputs)
            current=page['map']
            if current.get('map_id',current.get('id'))!=map_id or current['revision']!=revision:
                raise web.HTTPConflict(text='Map snapshot revision changed')
            metadata=current
            entities.extend(page['entities'])
            cursor=page.get('next_cursor')
            if not cursor:
                break
            key=json.dumps(cursor,sort_keys=True)
            if key in seen:
                raise web.HTTPConflict(text='Repeated map cursor')
            seen.add(key)
        return dict(type='snapshot',map_id=map_id,revision=revision,complete=True,
                    data=dict(map=metadata,entities=entities,next_cursor=None))
    return snapshot
