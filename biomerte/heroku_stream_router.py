"""Heroku entrypoint. Builds config from env and starts the router."""

import asyncio
import os
import sys

from biomerte.stream_router import serve


def main():
    secret = os.environ.get('STREAM_ROUTER_SECRET', '')
    node_id = os.environ.get('CONTROLLER_NODE_ID', '')
    if len(secret) < 32:
        sys.exit('STREAM_ROUTER_SECRET must be at least 32 characters')
    if not node_id:
        sys.exit('CONTROLLER_NODE_ID is required')
    config = {
        'listen': '0.0.0.0',
        'port': int(os.environ.get('PORT', '8751')),
        'secrets': {node_id: secret},
        'inspect_url': os.environ.get('STREAM_INSPECT_URL', ''),
    }
    asyncio.run(serve(config))


if __name__ == '__main__':
    main()
