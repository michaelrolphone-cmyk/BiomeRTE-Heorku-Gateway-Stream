# BiomeRTE Heroku Gateway Stream

Heroku web dyno that hosts the BiomeRTE leased stream router.

Field nodes, providers, and the Rover server dial the existing gateway. The gateway proxies WebSocket sessions to this dyno. No public listener or certificate is required on any disaster-relief node.

## What this provides

- Always-on process that owns live stream sessions
- Public `wss://` endpoint via Heroku TLS termination
- Shared secret authentication with controllers
- No inbound ports on field hardware

## Required code change in BiomeRTE

The upstream `stream_router.py` always loads a certificate and forces TLS. Behind Heroku the process must listen plain on `$PORT`.

In `biomerte/stream_router.py` `serve()`, make TLS optional and bind to `0.0.0.0`:

```python
ssl_context = None
if config.get('certificate') and config.get('key'):
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.load_cert_chain(config['certificate'], config['key'])

async with websocket_server(
    handle,
    config.get('listen', '0.0.0.0'),
    int(config.get('port', 8751)),
    ssl=ssl_context,
    max_size=4194304,
    max_queue=1,
    write_limit=65536,
):
    await asyncio.Future()
```

Also allow secrets to be supplied without local controller directories (see `heroku_stream_router.py`).

## Deploy

1. Create a Heroku app (Basic or Standard dyno so it does not sleep).
2. Set config vars:

   | Var | Purpose |
   |-----|---------|
   | `STREAM_ROUTER_SECRET` | Shared secret, ≥32 characters |
   | `CONTROLLER_NODE_ID` | Controller identity the secret belongs to |
   | `STREAM_INSPECT_URL` | Optional HTTPS endpoint for `stream.inspect` |
   | `WEB_CONCURRENCY` | `1` |

3. Push this repo (or connect it) and scale the web dyno to 1.

4. Point the gateway at the dyno:

   ```
   STREAM_ROUTER_ORIGIN=https://<app-name>.herokuapp.com
   ```

5. On each controller `node.json`:

   ```json
   {
     "stream_router_secret": "<same secret>",
     "stream_gateway_url": "wss://<your-gateway>/api/stream"
   }
   ```

Providers and the Rover server continue to dial the gateway. The gateway proxies to this dyno.

## Layout

```
Procfile                          web: python -m biomerte.heroku_stream_router
runtime.txt                       Python 3.12
requirements.txt                  websockets
biomerte/heroku_stream_router.py   generates config from env and starts the router
```

## Limits

- Session state is in-process. A dyno restart drops live sessions; clients re-attach under the existing lease and epoch rules.
- One dyno only. Multiple dynos need sticky routing or shared session state, which the current router does not have.
- If Heroku is unreachable from the field, streaming stops. Discrete capability leases and destinations do not depend on this dyno.
