"""The production entry point must retire a live witness before WebSocket drain."""
import asyncio
import json
import os
import signal
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from aiohttp import ClientSession, web
from biomerte.authority_transport import authority_key
from biomerte.durable_witness import DurableWitness
from biomerte.heroku_stream_router import main

NODE = "a" * 64
SECRET = "local gateway test secret " * 3


def run_gateway(directory):
    """Use the production gateway and record only the external witness exchange."""
    directory = Path(directory)
    async def witness_request(self, operation, **body):
        if operation in ("acquire", "renew"):
            now = time.monotonic()
            owner = {"boot_id": self.boot_id, "epoch": 7}
            (directory / "acquired").write_text(json.dumps(owner))
            return {"lease": dict(owner, witness=self.witness_id, server_now=now,
                                  expires_at=now + self.lease_s, ready_at=now,
                                  max_grant_s=self.coordinator.max_lease_ttl_s)}
        if operation == "heads":
            return {"heads": [], "next": None}
        if operation == "release":
            (directory / "released").write_text(json.dumps(body["owner"]))
            return {"released": True}
        raise AssertionError(operation)

    start = web.TCPSite.start
    async def report_port(site):
        await start(site)
        (directory / "port").write_text(str(site._server.sockets[0].getsockname()[1]))
    with patch.object(DurableWitness, "request", witness_request), patch.object(web.TCPSite, "start", report_port):
        main()


@unittest.skipUnless(os.name == "posix", "Production termination uses Unix signals")
class GatewayShutdownTests(unittest.IsolatedAsyncioTestCase):
    async def test_process_termination_with_a_live_authority_socket(self):
        for sig in (signal.SIGTERM, signal.SIGKILL):
            with self.subTest(signal=sig), tempfile.TemporaryDirectory() as temp:
                path = Path(temp)
                env = dict(os.environ, PORT="0", CONTROLLER_NODE_ID=NODE, STREAM_ROUTER_SECRET=SECRET)
                repo = Path(__file__).resolve().parents[1]
                env["PYTHONPATH"] = os.pathsep.join((str(repo), str(repo / "tests")))
                child = await asyncio.create_subprocess_exec(
                    sys.executable, "-m", "test_gateway_shutdown", temp, env=env, cwd=repo)
                try:
                    async def wait_for(name):
                        while not (path / name).is_file():
                            if child.returncode is not None:
                                self.fail(f"Gateway exited before {name} with {child.returncode}")
                            await asyncio.sleep(0.01)
                        return (path / name).read_text()
                    port = await asyncio.wait_for(wait_for("port"), 5)
                    owner = json.loads(await asyncio.wait_for(wait_for("acquired"), 2))
                    async with ClientSession() as client:
                        async with client.ws_connect(
                                f"http://127.0.0.1:{port}/authority/{NODE}/connect",
                                headers={"Authorization": "Authority " + authority_key(SECRET, NODE)}) as socket:
                            self.assertFalse(socket.closed)
                            child.send_signal(sig)
                            if sig == signal.SIGTERM:
                                release = await asyncio.wait_for(wait_for("released"), 2)
                                self.assertEqual(owner, json.loads(release))
                                await asyncio.wait_for(socket.receive(), 2)
                    self.assertEqual(0 if sig == signal.SIGTERM else -signal.SIGKILL,
                                     await asyncio.wait_for(child.wait(), 3))
                    self.assertEqual(sig == signal.SIGTERM, (path / "released").exists())
                finally:
                    if child.returncode is None:
                        child.kill()
                    await child.wait()


if __name__ == "__main__":
    run_gateway(sys.argv[1])
