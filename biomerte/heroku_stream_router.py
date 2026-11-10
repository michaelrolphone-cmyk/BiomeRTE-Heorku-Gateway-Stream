"""Heroku TLS-terminated leased-stream router; controller IDs remain authorities."""
import asyncio
import json
import os
import pathlib
import sys
from biomerte.public_server import serve

def build_config(environ=None):
    env = os.environ if environ is None else environ
    secret = env.get('STREAM_ROUTER_SECRET', '')
    if len(secret) < 32:
        raise ValueError('STREAM_ROUTER_SECRET must be at least 32 characters')
    deployment = json.loads((pathlib.Path(__file__).parent.parent/'controllers.json').read_text())
    configured = env.get('CONTROLLER_NODE_ID', '')
    nodes = env.get('CONTROLLER_NODE_IDS', '').split(',') if env.get('CONTROLLER_NODE_IDS') else deployment['authorities']
    if len(configured)==64 and all(c in '0123456789abcdef' for c in configured):
        nodes = [configured]
    if not nodes or any(len(n)!=64 or any(c not in '0123456789abcdef' for c in n) for n in nodes):
        raise ValueError('Expected controller authority fingerprints')
    return {'listen':'0.0.0.0', 'port':int(env.get('PORT','8751')),
        'secrets':{node:secret for node in nodes},
        'rover_authority':deployment.get('rover_authority'),
        'router_owner':env.get('STREAM_ROUTER_OWNER') or deployment.get('router_owner','hosted'),
        'inspect_url':env.get('STREAM_INSPECT_URL') or deployment['inspect_url']}

def main():
    try:
        config=build_config()
    except ValueError as error:
        sys.exit(str(error))
    asyncio.run(serve(config))

if __name__=='__main__':
    main()

