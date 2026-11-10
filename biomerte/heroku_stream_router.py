"""Heroku entrypoint for the BiomeRTE stream router.

Reads environment variables, writes a temporary router config, and starts
the router listening plain on $PORT so Heroku can terminate TLS.

Requires the upstream stream_router.serve() to accept a missing certificate
(ssl=None) and to bind to 0.0.0.0.
"""

import json
import os
import sys
import tempfile


def build_config():
    port = int(os.environ.get("PORT", "8751"))
    secret = os.environ.get("STREAM_ROUTER_SECRET", "")
    node_id = os.environ.get("CONTROLLER_NODE_ID", "")
    if len(secret) < 32:
        sys.exit("STREAM_ROUTER_SECRET must be at least 32 characters")
    if not node_id:
        sys.exit("CONTROLLER_NODE_ID is required")
    return {
        "listen": "0.0.0.0",
        "port": port,
        "controllers": [],
        "secrets": {node_id: secret},
        "inspect_url": os.environ.get("STREAM_INSPECT_URL", ""),
    }


def main():
    config = build_config()
    fd, path = tempfile.mkstemp(prefix="router-", suffix=".json")
    with os.fdopen(fd, "w") as f:
        json.dump(config, f)
    # Prefer the local BiomeRTE tree if present; otherwise expect the
    # package on PYTHONPATH from the main BiomeRTE repo.
    try:
        from biomerte.stream_router import serve
        import asyncio
        asyncio.run(serve(config))
    except ImportError:
        sys.exit(
            "biomerte.stream_router not importable. "
            "Install the BiomeRTE package or add it to PYTHONPATH."
        )


if __name__ == "__main__":
    main()
