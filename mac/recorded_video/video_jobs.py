"""Durable uploads and isolated recorded-video reconstruction jobs.

No camera acquisition, live runtime writes or second WorldAtlas writer exists
here. Only immutable finalized submaps enter the owner's WorldImportQueue.
"""
from contextlib import contextmanager
import base64
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import subprocess
import sys
import time
import uuid

if __package__:
    from .video_source import validate_calibration
    from .vision_config import atomic_json
else:
    from video_source import validate_calibration
    from vision_config import atomic_json

CHUNK_BYTES = 262144
TERMINAL = {'completed', 'cancelled', 'failed', 'interrupted'}


@contextmanager
def locked(path, blocking=True):
    with Path(path).open('a+b') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | (0 if blocking else fcntl.LOCK_NB))
        yield handle


class VideoJobs:
    def __init__(self, vision_state, *, launch=True):
        self.state = Path(vision_state).expanduser().resolve()
        self.root = self.state / 'video-imports'
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.launch_enabled = launch

    def folder(self, identifier):
        if not isinstance(identifier, str) or str(uuid.UUID(identifier)) != identifier:
            raise ValueError('Invalid video import identity')
        return self.root / identifier

    @contextmanager
    def edit(self, identifier):
        folder = self.folder(identifier)
        with locked(folder / 'metadata.lock'):
            value = json.loads((folder / 'job.json').read_text())
            yield value
            value['updated_at'] = time.time()
            atomic_json(folder / 'job.json', value)

    def calibrations(self):
        database = self.state / 'observations/vision.sqlite3'
        if not database.is_file():
            return []
        with sqlite3.connect('file:' + str(database) + '?mode=ro', uri=True) as db:
            rows = db.execute('SELECT id,payload FROM calibrations ORDER BY created_at DESC').fetchall()
        result = []
        for identifier, payload in rows:
            value = json.loads(payload)
            try:
                validate_calibration(value)
            except ValueError:
                continue
            result.append({**value, 'calibration_id': identifier,
                           'label': f"{identifier} ({value['width']} × {value['height']})"})
        return result

    def calibration(self, body, fallback=None):
        value = body.get('calibration')
        if value is None and body.get('calibration_id'):
            value = next((p for p in self.calibrations()
                          if p['calibration_id'] == body['calibration_id']), None)
            if value is None:
                raise ValueError('Selected camera calibration is unavailable')
            value = {k: v for k, v in value.items() if k != 'label'}
        value = fallback if value is None else value
        if value is not None:
            validate_calibration(value)
        return value

    def get(self, identifier):
        folder = self.folder(identifier)
        value = json.loads((folder / 'job.json').read_text())
        if value['state'] == 'integrating':
            if __package__:
                from .world_import import WorldImportQueue
            else:
                from world_import import WorldImportQueue
            item = WorldImportQueue(self.state).get(identifier)
            if item is None and value.get('checkpoint_id'):
                # Repair a crash after persisting the final identities but
                # before enqueueing. The exact reference handoff is idempotent.
                item = WorldImportQueue(self.state).submit(identifier, value['map_id'], value['checkpoint_id'])
            if item and item['state'] in ('completed', 'failed'):
                with self.edit(identifier) as current:
                    current['state'] = item['state']
                    current['receipt'] = item.get('receipt')
                    current['error'] = item.get('error')
                value = json.loads((folder / 'job.json').read_text())
        elif value['state'] in ('queued', 'reconstructing', 'publishing'):
            try:
                with locked(folder / 'run.lock', blocking=False):
                    # A worker owns this lock from launch through final handoff.
                    # Never infer failure from an HTTP disconnect or frame age.
                    if time.time() - value.get('launched_at', 0) > 10:
                        with self.edit(identifier) as current:
                            if current['state'] in ('queued', 'reconstructing', 'publishing'):
                                current.update(state='interrupted', error='Mapping process exited before handoff; retry preserves its map/checkpoint identity.')
                        value = json.loads((folder / 'job.json').read_text())
            except BlockingIOError:
                pass
        return {k: v for k, v in value.items() if k not in ('calibration', 'prepared')}

    def create(self, body):
        identifier = str(uuid.UUID(body['request_id']))
        name, size = body.get('name'), body.get('size')
        if not isinstance(name, str) or not name or len(name) > 256 or Path(name).name != name:
            raise ValueError('Choose a recording with a valid filename')
        if type(size) is not int or size <= 0:
            raise ValueError('The recording must contain video bytes')
        calibration = self.calibration(body)
        folder = self.folder(identifier)
        folder.mkdir(exist_ok=True, mode=0o700)
        with locked(folder / 'metadata.lock'):
            path = folder / 'job.json'
            if path.exists():
                previous = json.loads(path.read_text())
                if (previous['name'], previous['size'], previous['calibration']) != (name, size, calibration):
                    raise ValueError('Request identity already names another upload')
            else:
                if size > shutil.disk_usage(self.root).free:
                    raise ValueError('Insufficient disk space for this recording')
                atomic_json(path, dict(id=identifier, name=name, size=size,
                    uploaded_bytes=0, chunk_bytes=CHUNK_BYTES, state='uploading',
                    calibration=calibration, created_at=time.time(), updated_at=time.time(),
                    map_id=str(uuid.uuid5(uuid.UUID(identifier), 'video-map')),
                    frames_processed=0, localized_frames=0))
                (folder / 'video.bin').touch(mode=0o600)
        return self.get(identifier)

    def chunk(self, body):
        identifier = body['id']
        raw = body.get('data')
        if not isinstance(raw, str) or len(raw) > 4 * ((CHUNK_BYTES + 2) // 3):
            raise ValueError('Video chunk is too large')
        data = base64.b64decode(raw, validate=True)
        offset = body.get('offset')
        if type(offset) is not int or offset < 0 or not data:
            raise ValueError('Invalid video chunk offset or bytes')
        with self.edit(identifier) as value:
            if value['state'] != 'uploading' or offset + len(data) > value['size']:
                raise ValueError('Upload is closed or chunk exceeds recording size')
            with (self.folder(identifier) / 'video.bin').open('r+b') as stream:
                if offset < value['uploaded_bytes']:
                    stream.seek(offset)
                    if offset + len(data) > value['uploaded_bytes'] or stream.read(len(data)) != data:
                        raise ValueError('Conflicting repeated video chunk')
                elif offset == value['uploaded_bytes']:
                    stream.seek(offset); stream.write(data); stream.flush(); os.fsync(stream.fileno())
                    value['uploaded_bytes'] += len(data)
                else:
                    raise ValueError('Resume at the acknowledged upload offset')
        return self.get(identifier)

    def finish(self, identifier):
        with self.edit(identifier) as value:
            if value['state'] == 'uploading':
                if value['uploaded_bytes'] != value['size']:
                    raise ValueError('Recording upload is incomplete')
                value['state'] = 'ready' if value['calibration'] else 'awaiting_calibration'
        return self.get(identifier)

    def start(self, body):
        identifier = body['id'];folder = self.folder(identifier)
        with self.edit(identifier) as value:
            if value['state'] in ('queued', 'reconstructing', 'publishing', 'integrating', 'completed'):
                return {k: v for k, v in value.items() if k not in ('calibration', 'prepared')}
            if value['state'] not in ('ready', 'awaiting_calibration', 'failed', 'interrupted'):
                raise ValueError('Upload the recording before starting mapping')
            calibration = self.calibration(body, value.get('calibration'))
            if calibration is None:
                raise ValueError('Choose validated calibration for the camera used to record this video')
            if value.get('native_saved') and calibration != value['calibration']:
                raise ValueError('A saved reconstruction cannot change camera calibration; create another import')
            value.update(calibration=calibration, state='queued', error=None, launched_at=time.time())
            (folder / 'cancel').unlink(missing_ok=True)
            if self.launch_enabled:
                # Independent process and session: tab/server restarts do not
                # terminate reconstruction or discard its durable identities.
                subprocess.Popen([sys.executable, str(Path(__file__).with_name('video_worker.py')),
                                  '--state', str(self.state), '--id', identifier],
                    stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL, start_new_session=True)
        return self.get(identifier)

    def cancel(self, identifier):
        with self.edit(identifier) as value:
            if value['state'] in ('publishing', 'integrating', 'completed'):
                raise ValueError('Submap publication has begun; its accumulated geometry is retained')
            (self.folder(identifier) / 'cancel').touch(mode=0o600)
            if value['state'] not in ('queued', 'reconstructing'):
                value['state'] = 'cancelled'
                (self.folder(identifier) / 'video.bin').unlink(missing_ok=True)
        return self.get(identifier)

    def handle(self, body):
        action = body.get('action')
        if action == 'list':
            jobs = [self.get(p.parent.name) for p in self.root.glob('*/job.json')]
            return dict(ok=True, jobs=sorted(jobs, key=lambda j: j['created_at'], reverse=True), calibrations=self.calibrations())
        if action == 'create': job = self.create(body)
        elif action == 'status': job = self.get(body['id'])
        elif action == 'chunk': job = self.chunk(body)
        elif action == 'finish': job = self.finish(body['id'])
        elif action == 'start': job = self.start(body)
        elif action == 'cancel': job = self.cancel(body['id'])
        else: raise ValueError('Unknown video import action')
        return dict(ok=True, job=job)


async def video_handler(request):
    import asyncio
    from aiohttp import web
    try:
        body = await request.clone(client_max_size=524288).json()
        if not isinstance(body, dict): raise ValueError('Expected video import request object')
        # File fsync, hashing and metadata reads never block live socket IO.
        return web.json_response(await asyncio.to_thread(request.app['video_jobs'].handle, body))
    except (ValueError, KeyError, FileNotFoundError) as exc:
        return web.json_response({'ok': False, 'error': {'code': 'VIDEO_INPUT', 'message': str(exc)[:500]}}, status=400)
