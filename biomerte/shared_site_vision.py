"""Share a read-only SiteBridge feed after two origin subscribers reach the same observation."""
import asyncio
from collections import deque

from aiohttp import web, WSMessage, WSMsgType

from .rover_transport import LIVE_HEARTBEAT_SECONDS, _PublicVision, _VisionObservations, _offer_vision, _vision_observation


def closed(code, reason):
    return WSMessage(WSMsgType.CLOSE, code, reason)


class _Viewer:
    """A read-only socket backed by this viewer's ordered, bounded mailbox."""

    def __init__(self, cursor, attached):
        self.joined = cursor
        self.attached = attached
        self.anchor = None
        self.origin = None
        self.queue = deque()
        self.ready = asyncio.Event()
        self.terminal = False
        self.new_group = False
        self.close_code = 1000

    def offer(self, message, observation=None):
        if self.terminal:
            return
        if not _offer_vision(self.queue, message, observation, new_group=self.new_group):
            # A peer that exceeds capacity for reliable data must reconnect.
            # Other viewers continue on the common source.
            self.queue.clear()
            self.queue.append(closed(1013, 'Vision receiver stalled; reconnect'))
            self.terminal = True
        else:
            self.terminal = message.type == WSMsgType.CLOSE
        self.new_group = False
        self.ready.set()

    def attach(self):
        self.attached = True
        self.anchor = None
        # Preserve the matched record once, even when the next shared record
        # immediately replaces the same observation type.
        self.new_group = True

    async def receive(self):
        while not self.queue:
            self.ready.clear()
            await self.ready.wait()
        first = self.queue[0]
        if isinstance(first, _VisionObservations):
            message = first.take()
            if not first.latest:
                self.queue.popleft()
            return message
        return self.queue.popleft()

    # SiteBridge's vision route discards every inbound application message.
    # Protocol PING/PONG and each viewer's close stay on its own public socket.
    async def send_bytes(self, unused):
        pass

    send_str = send_bytes

    async def close(self, **unused):
        pass


class SharedSiteVision:
    def __init__(self, ingress, bridge):
        self.ingress = ingress
        self.bridge = bridge
        self.members = set()
        self.ended = False
        self.origin = None
        self.serial = 0
        self.history = deque()
        self.history_bytes = 0

    def publish(self, message, observation):
        self.serial += 1
        if any(not peer.attached for peer in self.members):
            size = len(message.data.encode()) if message.type == WSMsgType.TEXT else len(message.data)
            self.history.append((self.serial, message, observation, size))
            self.history_bytes += size
            # Only a contiguous suffix can be replayed after a matched anchor.
            while len(self.history) > 32 or self.history_bytes > 16 * 65536:
                self.history_bytes -= self.history.popleft()[3]
        else:
            self.history.clear()
            self.history_bytes = 0
        for peer in self.members:
            if peer.attached:
                peer.offer(message, observation)
            elif observation is not None and peer.anchor == message.data:
                peer.attach()
                peer.origin.cancel()

    def from_origin(self, peer, message, observation):
        peer.offer(message, observation)
        peer.anchor = message.data if observation is not None else None
        if self.ended or peer.anchor is None:
            return
        matches = [position for position, (serial, previous, known, _) in enumerate(self.history)
                   if serial > peer.joined and known is not None and previous.data == peer.anchor]
        if len(matches) == 1:
            peer.attach()
            for _, later, later_observation, _ in list(self.history)[matches[0] + 1:]:
                peer.offer(later, later_observation)

    async def read(self, peer, opened):
        reader = asyncio.current_task()
        terminal = closed(1011, 'Rover vision disconnected')
        try:
            notice = {'type': 'open', 'path': '/api/live/vision'}
            async with self.ingress.open_live_lane(self.bridge, notice, shared=True) as upstream:
                opened.set_result(None)
                while True:
                    message = await upstream.receive()
                    if message.type not in (WSMsgType.TEXT, WSMsgType.BINARY):
                        if message.type == WSMsgType.CLOSE:
                            terminal = message
                        elif message.type == WSMsgType.CLOSED:
                            terminal = closed(upstream.close_code or 1000, '')
                        break
                    observation = _vision_observation(message)
                    if reader is self.origin:
                        self.publish(message, observation)
                    else:
                        self.from_origin(peer, message, observation)
                        if peer.attached or peer.terminal:
                            break
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if not opened.done():
                opened.set_result(error)
        finally:
            if not opened.done():
                opened.set_result(ConnectionError('Rover vision disconnected'))
            if reader is self.origin:
                self.ended = True
                for member in self.members:
                    if member.attached:
                        member.offer(terminal)
            elif peer is not None and not peer.attached:
                peer.offer(terminal)

    async def serve(self, request, maximum):
        common = self.origin is None
        peer = _Viewer(self.serial, common)
        self.members.add(peer)
        self.ingress.vision_viewers.add(peer)
        opened = asyncio.get_running_loop().create_future()
        task = asyncio.create_task(self.read(None if common else peer, opened))
        if common:
            self.origin = task
        else:
            peer.origin = task
        socket = None
        try:
            failure = await asyncio.shield(opened)
            if failure is not None:
                raise failure
            socket = web.WebSocketResponse(heartbeat=LIVE_HEARTBEAT_SECONDS, autoping=False,
                                           max_msg_size=maximum, writer_limit=65536)
            await socket.prepare(request)
            self.ingress.active.add(socket)
            await _PublicVision(socket).relay(peer, buffered=True)
            return socket
        finally:
            if socket is not None:
                self.ingress.active.discard(socket)
            self.members.discard(peer)
            self.ingress.vision_viewers.discard(peer)
            tasks = []
            if peer.origin is not None:
                tasks.append(peer.origin)
            if not self.members:
                self.ended = True
                tasks.append(self.origin)
                if self.ingress.site_vision is self:
                    self.ingress.site_vision = None
            elif not self.ended and not any(member.attached for member in self.members):
                # The first browser can leave before the others rendezvous.
                # Keep a remaining viewer's own origin as their common source.
                next_owner = next((member for member in self.members
                                   if member.origin is not None and not member.origin.done()), None)
                if next_owner is not None:
                    tasks.append(self.origin)
                    self.origin, next_owner.origin = next_owner.origin, None
                    self.history.clear()
                    self.history_bytes = 0
                    for member in self.members:
                        member.anchor = None
                        member.joined = self.serial
                    next_owner.attach()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
