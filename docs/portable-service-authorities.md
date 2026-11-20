# Portable service authorities

The gateway places Docker services and relays their traffic through outbound
connections. The logical resource authority stays stable when the process moves
between controllers. Native authorities, native inspection, and existing Rover
lease/session origins keep their existing paths.

Placement is ephemeral. The gateway stores current grants, helper connections,
in-flight requests, prepared-baseline metadata, and the latest checkpoint
sequence/digest. It does not store checkpoint packages or an execution journal.

## Manifest and identity

A service with an external authority adds these fields to its immutable,
canonically hashed manifest:

```json
{
  "id": "relay-authority",
  "effect_policy": "externally_fenced",
  "state_policy": "externally_managed",
  "activation": {
    "protocol": "biomerte-service-adapter/1",
    "binding": "relay-authority"
  },
  "authority": {
    "id": "<logical authority fingerprint, 64 lowercase hex characters>",
    "resources": ["<exact resource ID>"],
    "providers": ["<exact provider actor fingerprint>"]
  }
}
```

Images, platform entries, command, environment, named mounts, policies and these
fields all participate in the existing manifest revision. Resources and provider
actors are matched exactly. Another service cannot claim overlapping authority,
resource, or provider identities. A logical authority cannot shadow a configured
native authority.

The complete placement binding, called **B** below, is exactly:

```json
{
  "service_id": "...",
  "manifest_revision": "...",
  "worker_id": "...",
  "worker_incarnation": "...",
  "boot_epoch": "...",
  "grant_token": "..."
}
```

Every field is required. A hostname or service ID alone does not identify a
current owner.

The deployment's optional `controllers.json.service_manifests` catalog reserves
logical scopes immediately after a gateway restart. Replicated supervisor
manifests still supply candidates. Reserving scopes prevents a known service
from being mistaken for an unmanaged native resource when its hosts are absent.

## Activation ordering

1. A prepared image and eligible supervisor can receive a provisional placement.
   No B-bound recovered state proof is required to choose that candidate.
2. The independent per-host Docker helper prepares verified state and publishes
   its checkpoint high water. A helper remains available on a standby host
   before an authority child exists.
3. The supervisor reports `state_ready: true` with the proof below. The first
   accepted proof must match the published head's digest, current coordinator
   epoch, and exact B.
4. The assignment returns `state_ready: true` only after that acceptance. The
   supervisor must observe this acknowledgement before fencing can advance the
   checkpoint head. Merely completing its local prepare request is insufficient.
5. The inert child and adapter establish the external resource fence. The
   supervisor reports the matching fence acknowledgement.
6. The assignment becomes `starting` or `active` with `effect_ready: true`.
   The adapter may then activate the child. The adapter and provider still
   enforce their normal resource-level fencing.

```json
{
  "state_ready": true,
  "state_proof": {
    "protocol": "biomerte-service-adapter/1",
    "binding": "<B object>",
    "checkpoint": "<64 lowercase hex characters>",
    "fresh": true,
    "custody_complete": true,
    "ready": true
  },
  "fence_ack": {
    "fenced": true,
    "boot_epoch": "<B.boot_epoch>",
    "grant_token": "<B.grant_token>",
    "binding": "<same B object>",
    "checkpoint": "<same checkpoint digest>"
  }
}
```

The prepared baseline remains fixed for this B while later live checkpoints
advance the witnessed high water. A new grant or coordinator epoch cannot reuse
that acceptance. Missing or mismatched current proofs are not hidden by a cached
successful fence acknowledgement.

Private `POST /api/service-coordinator/validate-preparation` accepts B and permits
staging/recovery for an unexpired, nondraining placement. It never authorizes
effects. Existing `/validate` continues to require state and fencing readiness.
For activation manifests it also requires exact B; legacy services retain their
existing validation shape.
Private coordinator and checkpoint endpoints require
`Authorization: Bearer <coordinator_key>` and reject browser Origin headers.

## Logical authority connection and calls

The authority connects outward to:

```text
/service-authority/{service_id}/connect
```

It uses the coordinator Bearer credential, then sends:

```json
{"type":"register","binding":"<B object>"}
```

The registration acknowledgement contains `type: registered`,
`protocol: biomerte-service-authority/1`, B, the logical authority ID, and the
remaining placement TTL. Only the current complete grant may register. An
obsolete connection cannot replace the admitted connection.

Registration retains a 5-second deadline. Application RPC replies have a separate
20-second default budget (`ServiceAuthorities.rpc_timeout`); they no longer inherit
the registration deadline. The Site proxy and relay firmware each allow 30 seconds
for their request, leaving room for forwarding and response delivery.

The authority can publish a checkpoint before a native call and again after a
successful call. Each checkpoint replication phase allows up to 10 seconds for
concurrent helper replies. Two full phases plus other work can still exceed the
RPC budget: 20 seconds is not a worst-case execution guarantee. An RPC timeout
removes the pending correlation and ignores a late reply; it does not cancel or
replay work already dispatched. Placement and binding are still checked after
every accepted reply.

Authenticated `GET /api/service-coordinator/status` advertises
`independent_authority_rpc_timeout` in `supported_features` and reports the current
`authority_transport.registration_timeout_s` and `authority_transport.rpc_timeout_s`
values. The production defaults are 5 and 20 respectively. This local status
observation neither contacts a worker nor configures or admits a service; it can
verify the deployed feature after a main-branch deployment.

A call RPC is:

```json
{
  "type": "call",
  "id": "<transport correlation ID>",
  "binding": "<B object>",
  "preparation": false,
  "envelope": "<original complete signed envelope>"
}
```

Reply with `type: call_result`, the same transport ID and B, an integer HTTP-like
`status`, and `result` or `error`. The public HTTP entry point is
`POST /api/service-authorities/call`; its body is the original signed envelope,
not a wrapper. A successful HTTP response is `{"result": ...}`.

The gateway preserves all signed fields. The authority must verify the original
actor signature, site/realm, resource scope and operation permissions. A
consumer specifies the logical authority in the signed `controller` field.
A declared provider may omit that field and is resolved by its signed actor.

During preparation only declared providers may call `cap.offer`,
`provider.poll`, `cap.fenced`, `provider.result`, or `cap.status`. The adapter
must restrict preparation poll output to fences/reconciliation and accept
results only for already known, in-scope action identities. Provider claiming
and stream attachment are not admitted before effects are ready. Once effects
are ready, provider calls carry `preparation: false`.

The gateway validates the socket and full placement again after every awaited
reply. An expired, draining, superseded, or previous-boot process cannot return
an admitted result even if its connection resumes.

### Resolution and pinned Rover origins

`POST /api/service-authorities/resolve` accepts exactly one of:

```json
{"resource":"<exact resource>"}
{"provider":"<exact provider actor>"}
{"authority":"<exact logical authority>"}
```

A 200 response returns the logical authority, service ID, B, remaining TTL,
state and effect readiness. Resource/authority resolution requires effect
readiness. Provider resolution may return a provisional registered endpoint so
it can complete recovery operations.

At the gateway, 404 means its current catalog does not manage the scope. 503
means a declared service is unavailable, or initial restart discovery is still
in progress. Declared scopes stay reserved in this process even after all
workers expire. Callers that already have a trusted logical scope declaration
must never interpret a later 404 as permission to fall back to a native issuer.

Rover consults trusted local scope declarations for each new acquisition or
previously unbound submission. Its private node.json field is:

```json
{
  "service_authority_declarations": [{
    "service_id": "relay-authority",
    "authority": "<logical authority fingerprint>",
    "resources": ["<exact resource ID>"],
    "providers": ["<exact provider actor>"]
  }]
}
```

These declarations contain no host binding. Install or update them before
enabling the corresponding logical scope. Rover reloads the field for new work,
so adding a future service does not require replacing pinned existing origins.
For a declared resource it requires an admitted gateway response with the exact
declared service and authority; 404 and 503 both fail closed. Undeclared resources
use the deployed native ControllerRoutes offer-discovery path without contacting
the witness, so a witness outage does not interrupt unrelated native resources.

Existing lease and stream IDs, and existing pending command IDs, retain their
recorded authority. Resource-move detection for existing native work is
preserved. There is no relay resource-prefix or Pi-host rule in the routing
code.

The source preserves the deployed LocalAuthority HTTP client to the running
native controller, pending submission bindings, exact native offer discovery,
sensor/world snapshot passthrough, and map reads through capabilities. It does
not start a bundled native Application against the controller database.

## Inspection and stream credentials

The service connection also accepts `type: inspect` with B, a transport ID,
logical `authority`, and `session`. Its `inspect_result` reply must echo ID/B
and return an exact matching authority/session grant.

The existing private `/stream/inspect` path supports both logical and native
authorities. Native secrets and outbound native inspection connections are
unchanged. Logical authority secrets are:

```text
hex(HMAC-SHA256(shared_stream_secret, "service-authority:" + authority_id))
```

A native Mac router opts into generic future logical services using:

```json
{
  "service_authority_secret": "<shared stream secret seed>",
  "service_inspect_url": "https://<gateway>/stream/inspect"
}
```

The native router integration adds the stdlib-only
`biomerte/service_inspection.py` module. In the existing native `serve()`, after
its `add_remote(config, secrets, validators)` call, insert:

```python
from .service_inspection import add_services
secrets, validators = add_services(config, secrets, validators)
```

For native health reporting, obtain each session's role counters with:

```python
roles = peers.setdefault(session.grant["authority"], {"provider": {}, "server": {}})
```

This preserves the deployed native router's peer-status, control-clock,
multi-consumer, vision-mailbox, quarantine, device discovery and wire-error
features. The gateway's compact vendored router is not a replacement for that
native runtime.

The logical authority uses the derived secret when signing its stream
credentials. The router verifies current authority state for every attachment
and periodic watcher pass. Possessing a correctly signed old token does not
bypass inspection or revive a revoked stream.

## Independent checkpoint helpers

Each helper connects outward to
`/service-worker/{worker_id}/connect`, using the coordinator Bearer credential:

```json
{"type":"register","worker_id":"...","worker_incarnation":"...","service_id":"..."}
```

The incarnation must identify an unambiguous eligible current supervisor for that
exact service. Ownership is not required. A helper connection is indexed by
(worker_id, service_id); separate relay and controller helpers on the same host
coexist without synthetic worker IDs. A duplicate current connection for the
same pair is rejected, and reconnect cleanup retires only that pair.

The registration acknowledgement echoes service_id, worker identity and the
gateway boot_epoch. New helpers wait for that exact acknowledgement before
reporting connection readiness. Deploy the scoped gateway before new helpers.

The deployed legacy relay helper omits service_id in registration. Within the
same bounded registration deadline, the gateway sends a read-only
checkpoint.status probe to that exact authenticated socket before acknowledging
it. The legacy reply must match the fresh request ID, supervisor incarnation,
gateway epoch and null binding, and supply status 200, durable true and a valid
nonempty checkpoint reference. That reference binds the socket once to its
service. Empty or inconsistent legacy stores are refused; there is no wildcard,
host-only lookup, service-name default or fallback to a sibling helper. This
compatibility probe does not publish a head or create a witness.

Every ordinary RPC is selected by the exact service and worker, and its reply
is correlated to that socket, service, incarnation, epoch and binding. New
helpers echo service_id; legacy replies may omit it only on their already
bound socket, and checkpoint/retained references must match that scope.
Local adapter bindings and remote helper messages are also checked against the
helper's configured service before store or block access. Authenticated coordinator status exposes candidates
under `services[service_id].candidates`, including their worker ID, incarnation,
readiness reason, and advertised checkpoint metadata.

Private checkpoint APIs are:

| Endpoint | Request |
| --- | --- |
| `/api/service-checkpoints/replicate` | B, checkpoint, `local_durable: true`, targets |
| `/api/service-checkpoints/status` | service_id, workers |
| `/api/service-checkpoints/get` | service_id, worker, exact checkpoint identity |

A checkpoint is `{service_id, sequence, digest, package}`: sequence is a
nonnegative integer, digest is 64 lowercase hex, and package is an opaque signed
object. Worker/target identities contain exactly worker_id and worker_incarnation.
Empty status workers selects current registered helpers for that exact service. Empty replicate targets
permits a degraded owner to publish its locally fsynced high water.

Helper requests have `type: checkpoint.put|get|status`, a transport ID, current
boot_epoch, worker identity, service_id, binding, and checkpoint. Put carries
the signed package and exact B. Read-only get/status carry binding null; get
specifies the requested checkpoint identity and status carries checkpoint null.

A helper reply is:

```json
{
  "type": "checkpoint_result",
  "id": "<same transport ID>",
  "worker_id": "...",
  "worker_incarnation": "...",
  "service_id": "...",
  "boot_epoch": "<same gateway epoch>",
  "binding": "<same B, or null for get/status>",
  "status": 200,
  "checkpoint": {
    "service_id": "...",
    "sequence": 1,
    "digest": "<64 lowercase hex>"
  },
  "result": {"durable": true}
}
```

Put succeeds only after durable fsync and an exact digest acknowledgement.
Get may return the package in result; status may return checkpoint null if no
local snapshot exists. The helper verifies package signatures, scope and
lineage, rejects lower sequences and conflicting equal-sequence digests, and
never lets a stale writer overwrite newer local state.

After every target acknowledgement, the gateway revalidates B and checks high
water again. It records only `{service_id, sequence, digest, binding, boot_epoch}`.
A lower sequence or conflicting equal-sequence digest returns 409. Status
returns this `head` alongside the current boot_epoch and helper replies.

The adapter must publish local fsync/high water before releasing an effect/RPC
response and replicate to available peers. A takeover must match witnessed
current high water. After the gateway loses its ephemeral witness, a cached
checkpoint alone does not prove freshness: the adapter must compare the
configured hosts' durable current-incarnation evidence and synchronize the
verified maximum before publishing a new baseline. If that evidence is
unavailable, the adapter cannot assert `fresh: true`. These package and custody
checks are adapter responsibilities; transport acknowledgements do not invent
them.

## Validation

The service tests cover provisional versus effect admission, exact B/checkpoint
matching, witnessed baseline acknowledgement, old-owner replies and sockets,
unknown versus unavailable scopes, preparation-only provider operations,
standby checkpoint replication, high-water conflicts, restart invalidation,
dynamic logical stream inspection, preserved native validators, and pinned
Rover lease/session origins. Existing native public ingress and authority
transport tests remain part of the integration suite.
