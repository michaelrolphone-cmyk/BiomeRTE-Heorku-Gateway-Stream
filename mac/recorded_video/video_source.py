"""Sequential recorded-video frames for an isolated SLAM import worker.

This source never opens the rover camera, changes the active map or assigns a
world transform. The world-atlas owner registers and aligns the resulting submap.
"""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import math
from pathlib import Path


class VideoInputError(ValueError):
    pass


@dataclass(frozen=True)
class VideoFrame:
    image: object
    frame_id: str
    index: int
    timestamp_s: float
    source_sha256: str


def video_digest(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def validate_calibration(calibration):
    if not isinstance(calibration, dict) or calibration.get('validated') is not True:
        raise VideoInputError('The recorded camera needs validated intrinsic calibration.')
    if not calibration.get('calibration_id') or not calibration.get('provenance'):
        raise VideoInputError('Calibration identity and provenance are required.')
    for field in ('width', 'height'):
        if type(calibration.get(field)) is not int or calibration[field] <= 0:
            raise VideoInputError('Calibration dimensions must be positive integers.')
    for field in ('fx', 'fy', 'cx', 'cy'):
        value = calibration.get(field)
        if type(value) not in (int, float) or not math.isfinite(value):
            raise VideoInputError('Finite camera intrinsics are required.')
    if calibration['fx'] <= 0 or calibration['fy'] <= 0:
        raise VideoInputError('Camera focal lengths must be positive.')
    distortion = calibration.get('distortion')
    if (not isinstance(distortion, list) or len(distortion) not in (4, 5) or
            any(type(x) not in (int, float) or not math.isfinite(x) for x in distortion)):
        raise VideoInputError('Four or five finite distortion coefficients are required.')


class RecordedVideo:
    """Decode every frame in presentation order with media timestamps.

    PyAV is loaded only in the import worker. Native media PTS, including variable
    frame rates, determine time; processing speed and wall clocks never do.
    Missing or regressing PTS reject the import instead of inventing timing.
    The decoder owns one current image, and closes even on cancellation/error.
    """

    def __init__(self, path, calibration, *, opener=None):
        self.path = Path(path)
        validate_calibration(calibration)
        self.calibration = dict(calibration)
        self.opener = opener
        self.sha256 = None

    def probe(self):
        opener = self.opener
        if opener is None:
            import av
            opener = av.open
        with opener(str(self.path)) as container:
            if len(container.streams.video) != 1:
                raise VideoInputError('Select a recording with one video stream.')
            stream = container.streams.video[0]
            if (stream.codec_context.width != self.calibration['width'] or
                    stream.codec_context.height != self.calibration['height']):
                raise VideoInputError('Video dimensions do not match camera calibration.')
            fps = float(stream.average_rate) if stream.average_rate else None
            if fps is None or not math.isfinite(fps) or fps <= 0:
                raise VideoInputError('The recording needs a valid nominal camera frame rate.')
            return {'fps': fps, 'width': stream.codec_context.width,
                    'height': stream.codec_context.height}

    def frames(self, cancelled=lambda: False):
        if not self.path.is_file():
            raise VideoInputError('The uploaded video is unavailable.')
        self.sha256 = video_digest(self.path)
        opener = self.opener
        if opener is None:
            try:
                import av
            except ImportError as exc:
                raise VideoInputError('The video import worker requires PyAV.') from exc
            opener = av.open
        count = 0
        with opener(str(self.path)) as container:
            if len(container.streams.video) != 1:
                raise VideoInputError('Select a recording with one video stream.')
            stream = container.streams.video[0]
            previous = None
            origin = None
            for frame in container.decode(stream):
                if cancelled():
                    return
                if frame.pts is None or frame.time_base is None:
                    raise VideoInputError('Video frames need recorded presentation timestamps.')
                pts = frame.pts * frame.time_base
                if not math.isfinite(float(pts)) or (previous is not None and pts <= previous):
                    raise VideoInputError('Video timestamps must be finite and strictly increasing.')
                if (frame.width != self.calibration['width'] or
                        frame.height != self.calibration['height']):
                    raise VideoInputError('Video dimensions do not match camera calibration.')
                rotation = getattr(frame, 'rotation', 0)
                if rotation and int(rotation) % 360:
                    raise VideoInputError('Calibrate the encoded video orientation before importing.')
                image = frame.to_ndarray(format='bgr24')
                if origin is None:
                    origin = pts
                previous = pts
                yield VideoFrame(image, f'video:{self.sha256}:{count}', count,
                                 float(pts - origin), self.sha256)
                count += 1
        if count == 0 and not cancelled():
            raise VideoInputError('The recording contains no decodable video frames.')


def replay_video(source, backend, *, cancelled=lambda: False, progress=lambda value: None):
    """Feed an already-isolated backend; caller owns checkpoint/atlas publication.

    A cancellation or failure never calls any world-map operation. The caller
    must keep the imported submap identity independent of the active rover map.
    """
    processed = 0
    localized = 0
    last_timestamp = None
    frames = source.frames(cancelled)
    try:
        for frame in frames:
            if cancelled():
                break
            result = backend.track(frame.image, frame.frame_id, frame.timestamp_s)
            processed += 1
            localized += int(result.get('localization', {}).get('valid') is True)
            last_timestamp = frame.timestamp_s
            progress({'frames_processed': processed, 'localized_frames': localized,
                      'media_time_s': last_timestamp, 'source_frame_id': frame.frame_id})
    finally:
        frames.close()
    return {'status': 'cancelled' if cancelled() else 'decoded',
            'frames_processed': processed, 'localized_frames': localized,
            'media_time_s': last_timestamp, 'source_sha256': source.sha256}
