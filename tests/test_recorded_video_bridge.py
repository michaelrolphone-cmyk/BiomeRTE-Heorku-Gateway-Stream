import base64
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid

from aiohttp.test_utils import TestClient, TestServer
from mac.site_bridge import SiteBridge
from test_camera_socket import Rover


class RecordedVideoBridgeTest(unittest.IsolatedAsyncioTestCase):
    async def test_real_video_upload_resumes_and_finishes_through_authenticated_route(self):
        token = "x" * 32
        headers = {"Authorization": "Bearer " + token}
        identifier = str(uuid.uuid4())
        with tempfile.TemporaryDirectory() as folder, patch.object(Path, "home", return_value=Path(folder)):
            def build():
                return SiteBridge(Rover(), token, dict(forward=1, reverse=2, left=3, right=4)).app()

            async with TestClient(TestServer(build())) as client:
                request = dict(action="create", request_id=identifier, name="clip.mov", size=4)
                async with client.post("/api/video", json=request) as denied:
                    self.assertEqual(denied.status, 401)
                async with client.post("/api/video", json=request, headers=headers) as created:
                    self.assertEqual((await created.json())["job"]["state"], "uploading")
                chunk = dict(action="chunk", id=identifier, offset=0, data=base64.b64encode(b"test").decode())
                async with client.post("/api/video", json=chunk, headers=headers) as written:
                    self.assertEqual((await written.json())["job"]["uploaded_bytes"], 4)

            async with TestClient(TestServer(build())) as resumed:
                async with resumed.post("/api/video", json=chunk, headers=headers) as replay:
                    self.assertEqual((await replay.json())["job"]["uploaded_bytes"], 4)
                async with resumed.post("/api/video", json={"action": "finish", "id": identifier},
                                        headers=headers) as done:
                    self.assertEqual((await done.json())["job"]["state"], "awaiting_calibration")
            imported = Path(folder) / ".local/share/biome-robot-vision/video-imports" / identifier
            self.assertEqual((imported / "video.bin").read_bytes(), b"test")
            self.assertEqual(json.loads((imported / "job.json").read_text())["uploaded_bytes"], 4)

    def test_bundled_reconstruction_worker_resolves_its_upload_modules_as_an_executable(self):
        from mac.recorded_video import video_jobs
        worker = Path(video_jobs.__file__).with_name("video_worker.py")
        result = subprocess.run([sys.executable, str(worker), "--help"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--state", result.stdout)
