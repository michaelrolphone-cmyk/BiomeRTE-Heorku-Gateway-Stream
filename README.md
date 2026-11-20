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
    max_size=None,
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
     "stream_gateway_url": "wss://<app-name>.herokuapp.com"
   }
   ```

Native provider/server sockets may dial this dyno directly at `/<session>/<lane>`, using the controller-issued Stream credential. The gateway `/api/stream` proxy remains supported.

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
- If Heroku is unreachable from the field, streaming stops. Native discrete capability leases and destinations keep their existing controller paths. Portable Docker authority admission depends on the current gateway placement witness.

## Rover intent lifetime

The paired Mac service uses a configurable 500 ms desired-state lifetime. Set
`ROVER_INTENT_TTL_MS` on that service to an integer from 20 through 1000 ms.
Directions and neutral use the same policy. This accommodates measured public
transport delay and clock uncertainty; the router and provider still reject
expired input and enforce the one-second maximum. Keep host clocks synchronized
with NTP. A server acceptance is separate from provider received/applied feedback.

## Telemetry sizes
Telemetry headers and packets have no artificial byte cap. Complete JSON metadata is preserved; framing lengths, identities and epochs remain validated. Superseded camera frames remain latest-only.

## Portable Docker services

The single-process service coordinator can place a manifest-defined Docker
service on either controller. Logical authority connections are bound to the
exact current placement; stateful authorities must publish a verified checkpoint
baseline and establish their external fence before effects are admitted.
Independent per-host helpers retain and transfer checkpoints while the gateway
keeps current placement in memory. When the durable witness is configured below, only its current epoch lease and the latest checkpoint reference for each service survive a dyno restart; checkpoint payloads remain on the hosts.

[Portable service authorities](docs/portable-service-authorities.md) describes
the manifest, preparation and effect gates, outbound authority/helper protocols,
checkpoint publication, native stream-router integration, and Rover routing.
Existing native authorities remain supported. New Rover work resolves declared
logical scopes through the witness; unrelated native resources keep their native
availability. Existing lease, session, and pending command origins stay pinned.

## Durable controller recovery across dyno restarts

A signed local checkpoint proves its bytes, but does not prove that an absent
replica has no newer accepted state. Configure the bounded durable witness when
a surviving controller must recover after a dyno restart while its peer is down.

The Sites gateway exposes authenticated HTTPS POST
`/internal/service-witness/v1` through `durableWitness(request, env)`. Its D1
database stores one current witness lease and one latest sequence/digest/scope
per service. It stores no checkpoint payloads and no execution history.

Set these runtime variables through the existing authenticated deployment
configuration path. Never put their secret values in Git, images, command logs,
or the service manifest.

| Variable | Heroku | Sites |
|---|---|---|
| `DURABLE_WITNESS_SECRET` | A separately generated high-entropy credential, at least 32 characters | The identical private credential |
| `DURABLE_WITNESS_ORIGIN` | HTTPS origin of the existing Sites gateway | Not used |
| `DURABLE_WITNESS_ID` | Optional; default `biomerte-services` | Same configured identity; default `biomerte-services` |
| `DURABLE_WITNESS_LEASE_S` | Optional; default 120; at least twice the maximum service grant interval, at most 600 | The protocol validates the requested bound |

The stream credential and durable-witness credential must be different.
`WEB_CONCURRENCY=1` and one web dyno remain required for live stream sessions.
The durable epoch lease additionally prevents two coordinator processes from
issuing overlapping service grants.

Publication requires the exact host acknowledgements followed by durable
latest-reference compare-and-swap before an effect-bearing response. Grants are
capped by the durable lease. A new epoch waits for the prior lease to expire and
applies the maximum service-grant quarantine. It loads the persisted exact head;
a surviving host must verify that reference and every block before recovery.
An unknown CAS outcome blocks replacement ownership until the exact write is
reconciled or a newer epoch fences all delayed old writes.

Do not delete witness rows to reset a rollout. Missing durable reference
metadata retains the original all-replica initial-custody requirement. An
unavailable D1 witness does not authorize an arbitrary local snapshot.

The existing GitHub workflow validates coordinator, transport, controller-route
and durable-witness regressions on Python 3.12 using requirements.txt. It is a
test workflow, not a Heroku deployment workflow. Production version and config
changes still require the existing authenticated Heroku operational path.


### Git-pinned witness deployment

The optional repository file `durable-witness.json` pins the public witness
origin, identity and lease interval before the coordinator is constructed. Its
exact fields are `format`, `origin`, `witness_id` and `lease_s`, with format
`biomerte-durable-witness-deployment/1`. The origin must be a canonical bare HTTPS
origin, with no credentials, path, trailing slash, query or fragment; the lease
must be finite and between 60 and 600 seconds. A present malformed file, unknown
field, duplicate field or conflicting explicit origin, identity or lease setting
stops startup. An absent file with no witness environment settings retains the
legacy configuration.

The file contains no secrets or host placement. With the file present, an
explicit separate `DURABLE_WITNESS_SECRET` takes precedence. Otherwise the
credential is derived from the existing service-admin bearer. The existing
Heroku environment variable is `STREAM_ROUTER_SECRET`; `coordinator_key` derives
its service-admin bearer using HMAC-SHA256 with message
`b'biomerte/service-coordinator/v1'` and returns lowercase hexadecimal text.

`biomerte.witness_deployment.derive_witness_secret(token, origin, witness_id)`
accepts that exact 64-character bearer text as its ASCII HMAC key. Its message is
the concatenation of `b'biomerte/durable-witness/deployment/v1 '`, the canonical
origin in ASCII, `b' '`, and the witness identity in ASCII. It returns the
lowercase HMAC-SHA256 hexadecimal digest. The parent stream credential is never
sent to Sites. Derive and install the matching witness credential through the
existing private operational path; never print it or commit it.

Deploy the Sites endpoint and matching private witness credential before merging
the enabling file into the companion's deployed branch. This ordering alone does
not establish an initial checkpoint reference for an existing service. A new
witness has no evidence that one surviving helper has the latest accepted state.
Initial custody still requires the configured replicas or a separately verified
quiesced migration from the old live authority. Do not manufacture an empty
checkpoint, copy a stale helper head, or treat an unreachable peer as consent.
