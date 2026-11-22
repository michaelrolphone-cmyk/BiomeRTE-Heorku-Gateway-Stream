"""Supervised-by-durable-state offline mapping entry point, independent of rover IO."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import time

from video_jobs import VideoJobs, locked
from video_source import RecordedVideo, replay_video, video_digest
from vision_config import atomic_json


def publish(jobs, identifier, *, store_factory=None, queue_factory=None, entities_reader=None):
    """Idempotently publish one saved native package using persisted identities."""
    from vision_store import VisionStore
    from world_import import WorldImportQueue
    if entities_reader is None:
        from slam_backend import _saved_entities
        entities_reader = _saved_entities
    folder = jobs.folder(identifier)
    value = json.loads((folder / 'job.json').read_text())
    native = folder / 'native'
    manifest = json.loads((native / 'backend_manifest.json').read_text())
    if manifest['map_id'] != value['map_id']:
        raise ValueError('Saved video map identity differs from the durable job')
    store = (store_factory or VisionStore)(jobs.state / 'observations')
    try:
        calibration = value['calibration']
        store.save_calibration(calibration, calibration['calibration_id'])
        try:
            info = store.read_map(value['map_id'], limit=1)['map']
        except KeyError:
            info = store.create_map(dict(kind='monocular_sparse_map', calibration_id=calibration['calibration_id'],
                camera_source_id='video:' + value['source_sha256'], source='recorded_video',
                source_sha256=value['source_sha256'], import_id=identifier,
                units='arbitrary', scale_status='unobserved'), map_id=value['map_id'])
        checkpoint = store.latest_backend_checkpoint(value['map_id'])
        if checkpoint.get('availability') != 'available':
            if info['revision'] == 0:
                document = json.loads((native / 'map.json').read_text())
                entities = entities_reader(document, manifest.get('source_frames', {}))
                changes = [dict(op='upsert', kind=kind, entity_id=key, data=data)
                           for (kind, key), data in entities.items()]
                store.apply_map_update(value['map_id'], 0, changes, reason='video_import_final_geometry')
            prepared = value.get('prepared')
            if prepared is None:
                prepared = store.prepare_checkpoint(value['map_id'], metadata={
                    'geometry_matches_backend_state': True, 'checkpoint_origin': 'video_import',
                    'import_id': identifier, 'source_sha256': value['source_sha256']})
                with jobs.edit(identifier) as current: current['prepared'] = prepared
            else:
                # A process crash before folder publication can reuse the same
                # prepared ID if its partial snapshot survived. If finish had
                # already published, VisionStore startup reconciles its manifest.
                staging = store.checkpoint_dir / (prepared['checkpoint_id'] + '.partial')
                if not staging.exists():
                    raise RuntimeError('Prepared checkpoint is unavailable; preserve import for reconciliation')
            checkpoint = store.finish_checkpoint(prepared, backend_artifact=native)
        if value.get('checkpoint_id') and value['checkpoint_id'] != checkpoint['checkpoint_id']:
            raise RuntimeError('Refusing to replace the recorded import checkpoint')
        with jobs.edit(identifier) as current:
            current.update(checkpoint_id=checkpoint['checkpoint_id'], state='integrating')
        (queue_factory or WorldImportQueue)(jobs.state).submit(identifier, value['map_id'], checkpoint['checkpoint_id'])
        # The store now owns the only retained native package. Avoid accumulating
        # extra copies of the same checkpoint or keeping large uploaded media.
        shutil.rmtree(native)
        (folder / 'video.bin').unlink(missing_ok=True)
    finally:
        store.close()


def run_job(jobs, identifier, *, backend_factory=None, source_factory=RecordedVideo,
            publisher=publish):
    folder = jobs.folder(identifier)
    cancelled = lambda: (folder / 'cancel').exists()
    with locked(folder / 'run.lock'):
        while True:
            if cancelled():
                with jobs.edit(identifier) as current: current['state'] = 'cancelled'
                shutil.rmtree(folder / 'native', ignore_errors=True)
                (folder / 'video.bin').unlink(missing_ok=True)
                return
            try:
                global_lock = (jobs.root / 'native-worker.lock').open('a+b')
                fcntl.flock(global_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                global_lock.close(); time.sleep(.5)
        backend = None
        try:
            value = json.loads((folder / 'job.json').read_text())
            if value.get('checkpoint_id'):
                from world_import import WorldImportQueue
                WorldImportQueue(jobs.state).submit(identifier, value['map_id'], value['checkpoint_id'])
                with jobs.edit(identifier) as current: current['state'] = 'integrating'
                return
            native = folder / 'native'
            if not value.get('native_saved'):
                # Recovery before the completed native manifest replays the same
                # recording into the SAME private map identity, never the rover.
                shutil.rmtree(native, ignore_errors=True)
                source = source_factory(folder / 'video.bin', value['calibration'])
                media = source.probe() if hasattr(source, 'probe') else {'fps': 30.}
                if backend_factory is None:
                    from slam_backend import SlamBackend
                    backend_factory = SlamBackend
                config = json.loads((jobs.state / 'config.json').read_text())
                backend = backend_factory({**config.get('slam', {}),
                    'calibration': value['calibration'], 'fps': media['fps'],
                    'pyslam_root': str(jobs.state / 'vendor/pyslam')}, folder)
                backend.start(value['map_id'])
                last_progress = [0.]
                def progress(data):
                    if time.monotonic() - last_progress[0] >= 1:
                        with jobs.edit(identifier) as current: current.update(data, state='reconstructing')
                        last_progress[0] = time.monotonic()
                with jobs.edit(identifier) as current: current['state'] = 'reconstructing'
                result = replay_video(source, backend, cancelled=cancelled, progress=progress)
                with jobs.edit(identifier) as current: current.update(result)
                if result['status'] == 'cancelled' or cancelled():
                    with jobs.edit(identifier) as current: current['state'] = 'cancelled'
                    return
                if not result['localized_frames']:
                    raise ValueError('No valid camera poses reconstructed; use textured footage with camera translation and matching calibration')
                backend.checkpoint(native)
                with jobs.edit(identifier) as current:
                    current.update(native_saved=True, source_sha256=source.sha256)
                backend.close(); backend = None
            # Cancellation wins until the durable publication transition. Once
            # handed to the atlas, cancel never erases accumulated world data.
            with jobs.edit(identifier) as current:
                if cancelled():
                    current['state'] = 'cancelled'
                    return
                current['state'] = 'publishing'
            publisher(jobs, identifier)
        except Exception as exc:
            with jobs.edit(identifier) as current:
                current.update(state='failed', error=f'{type(exc).__name__}: {exc}'[:700])
        finally:
            if backend is not None: backend.close()
            if json.loads((folder / 'job.json').read_text()).get('state') == 'cancelled':
                shutil.rmtree(folder / 'native', ignore_errors=True)
                (folder / 'video.bin').unlink(missing_ok=True)
            global_lock.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--state', required=True)
    parser.add_argument('--id', required=True)
    args = parser.parse_args()
    jobs = VideoJobs(args.state)
    os.chdir(jobs.folder(args.id))
    run_job(jobs, args.id)
