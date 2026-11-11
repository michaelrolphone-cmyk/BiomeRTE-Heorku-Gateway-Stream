"""Independent camera LED transport; never holds or changes drive output leases."""
import asyncio
import os
from aiohttp import ClientSession, ClientTimeout, web

COLORS = {'red': [255, 0, 0], 'green': [0, 255, 0],
          'blue': [0, 0, 255], 'white': [255, 255, 255]}


class CameraLight:
    def __init__(self, url=None):
        self.url = url or os.environ.get('ROVER_CAMERA_LIGHT_URL', 'http://192.168.1.45/api/light')
        self.lock = asyncio.Lock()

    async def request(self, body):
        if not isinstance(body, dict) or set(body) - {'led'}:
            raise ValueError('Expected an LED state or empty state request')
        update = body.get('led')
        if update is not None:
            if (not isinstance(update, dict) or set(update) != {'on', 'brightness', 'color'} or
                    type(update['on']) is not bool or type(update['brightness']) is not int or
                    not 0 <= update['brightness'] <= 100 or update['color'] not in COLORS):
                raise ValueError('Invalid LED power, brightness or color')
            update = dict(on=update['on'], brightness=round(update['brightness'] * 255 / 100),
                          rgb=COLORS[update['color']])
        async with self.lock:
            async with ClientSession(timeout=ClientTimeout(total=4)) as session:
                options = {'json': update} if update is not None else {}
                async with session.request('POST' if update is not None else 'GET', self.url, **options) as response:
                    data = await response.json()
                    if response.status != 200:
                        raise RuntimeError('Camera light command rejected (HTTP %s)' % response.status)
        if not data.get('ready') or not data.get('command_state_known'):
            raise RuntimeError('Camera LED output is unconfirmed')
        color = next((name for name, rgb in COLORS.items() if rgb == data.get('rgb')), 'white')
        return {'led': dict(on=data['on'], brightness=round(data['brightness'] * 100 / 255), color=color),
                'hardware_verified': False, 'optical_readback': False}

    async def handle(self, request):
        try:
            return web.json_response(await self.request(await request.json()))
        except ValueError as error:
            return web.json_response({'error': {'code': 'INVALID_LED', 'message': str(error)}}, status=400)
        except Exception:
            return web.json_response({'error': {'code': 'LED_UNAVAILABLE',
                'message': 'Camera LED state is unconfirmed. Read state before retrying.'}}, status=503)
