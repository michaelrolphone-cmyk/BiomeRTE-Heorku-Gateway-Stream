"""Configuration and durable atomic metadata writes for the local vision process."""
from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

DEFAULT_STATE = Path.home() / ".local/share/biome-robot-vision"
DEFAULT_CONFIG = {
    "camera": {
        "url": "rtsp://192.168.1.45:554/mjpeg/1",
        "source_id": "esp32-98574b858428",
        "expected_width": 1280,
        "expected_height": 720,
        "expected_fps": 5.0,
        "network_timeout_s": 5.0,
        "reconnect_delay_s": 1.0,
    },
    "detector": {"model": "yolo11n.pt", "device": "auto", "image_size": 640, "confidence": 0.25},
    "calibration": {
        "calibration_id": "mounting-user-reported-20261109",
        "width": 1280, "height": 720,
        "fx": None, "fy": None, "cx": None, "cy": None,
        "distortion": None, "validated": False,
        "provenance": "intrinsics_unmeasured",
        "mounting": {
            "height_m": 0.0635,
            "pitch_down_deg": 0.0,
            "roll_deg": 0.0,
            "provenance": "user_reported_height_and_vertical_camera_face",
            "validated": False,
            "height_datum": "assumed_optical_center",
            "roll_status": "assumed_level_unmeasured",
        },
        "camera_to_robot": None,
    },
    "tof": {},
    "ahrs": {
        "enabled": False,
        "poll_interval_s": 0.1,
        "network_timeout_s": 2.0,
        "history_capacity": 256,
        "history_retention_s": 30.0,
    },
    "ultrasonic": {"enabled": True, "poll_interval_s": 0.26,
                   "network_timeout_s": 1.0, "freshness_threshold_s": 1.0},
    "max_observations": 300,
    "checkpoint_interval_s": 60.0,
    "freshness_threshold_s": 2.0,
}


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name+".", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, sort_keys=True, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def load_config(state_dir, config_path=None):
    state_dir = Path(state_dir)
    path = Path(config_path) if config_path else state_dir / "config.json"
    if not path.exists():
        atomic_json(path, DEFAULT_CONFIG)
    value = json.loads(path.read_text())
    if not isinstance(value.get("camera"), dict) or not value["camera"].get("url"):
        raise ValueError("camera.url is required")
    for key in ("network_timeout_s", "reconnect_delay_s"):
        if float(value["camera"].get(key, 1)) <= 0:
            raise ValueError(f"camera.{key} must be positive")
    return value
