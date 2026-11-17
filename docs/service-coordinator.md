# Generic Docker service coordination

The public gateway now provides an authenticated, memory-only placement authority
for reusable Docker services. It does not change controller leases, firmware
ownership or the existing stream protocol.

Run exactly one gateway process for this namespace. This is an explicit single
authority, not a two-node consensus algorithm. Running two independently writable
gateway replicas with the same credentials is unsupported. Restart invalidates
every prior grant and waits the configured maximum lease lifetime before
assigning another instance.

## Authentication and routes

All endpoints use HTTPS in deployment and the header:

`Authorization: Bearer HEX(HMAC-SHA256(STREAM_ROUTER_SECRET, "biomerte/service-coordinator/v1"))`

Requests with a browser Origin are refused. The derived credential is shared only
with trusted service workers. No credentials or grant tokens appear in status.

- `POST /api/service-coordinator/heartbeat`: report candidate preparation and
  health; receive assignments, statuses, errors and the desired manifest catalog.
- `GET` or `POST /api/service-coordinator/catalog`: bootstrap the catalog.
- `POST /api/service-coordinator/configure`: explicitly publish a manifest with
  `expected_revision` when replacing a registered revision. Conflicts return 409.
- `POST /api/service-coordinator/validate`: check a grant at an enforcement point.
- `POST /api/service-coordinator/release`: drain an exact grant.
- `GET /api/service-coordinator/status`: inspect redacted ownership and candidates.

Heartbeat errors are isolated per service. One mismatched rollout does not
prevent unrelated services from renewing. Configure is a separate explicit CAS.

## Canonical manifest

Required fields are `id`, `revision`, `images`, `effect_policy`, and
`state_policy`. Images map platform strings such as `linux/arm64` and
`linux/arm/v7` to either `name@sha256:<64 hex>` or an already prepared local
`sha256:<64 hex>` image ID.

Revision is SHA256 of the complete manifest excluding its revision field:

`json.dumps(body, sort_keys=True, separators=(',', ':'), ensure_ascii=True, allow_nan=False)`

Runner-specific fields such as command, environment, mounts, ports and health are
preserved and bound by this hash. Secret references are preferable to embedding
secret values in a manifest replicated to every eligible worker.

Optional placement fields are `lease_ttl_s` (default10), `worker_ttl_s`
(default10), `startup_grace_s` (default20) and ordered `preferred_workers`.
Preferences apply only when choosing a new owner; a healthy active owner stays
where it is. Intentional handback drains the old grant before another assignment.

The server maximum lease lifetime defaults to30seconds and is configurable through
`service_coordinator_max_lease_ttl_s`. This is also the restart grace. Do not lower
it across a deployment while earlier, longer grants could still be alive; first
quiesce them or retain the previous maximum as the restart grace.

## Worker heartbeat

The envelope is:

```json
{
  "worker_id": "mac",
  "worker_incarnation": "random-on-supervisor-boot",
  "services": [{
    "manifest": {"id": "...", "revision": "...", "images": {}, "effect_policy": "...", "state_policy": "..."},
    "manifest_revision": "...",
    "platform": "linux/arm64",
    "image_ref": "sha256:...",
    "prepared": true,
    "dependencies_ready": true,
    "healthy": null,
    "boot_epoch": "present-when-renewing",
    "grant_token": "present-when-renewing"
  }]
}
```

`healthy:null` describes a prepared standby or a starting instance. An assigned
instance must become healthy within startup grace. `healthy:false` revokes its
eligibility immediately. Preparation requires the exact platform image and actual
runtime/configuration/dependency readiness.

An assignment contains `service_id`, `manifest_revision`, `worker_id`,
`worker_incarnation`, `boot_epoch`, `grant_token`, `lease_ttl_s`, `state`
and `effect_ready`. After an external fence is acknowledged, state is
`starting` with `effect_ready:true` until child health becomes true; then state
is `active`. A worker may start only with its exact accepted fence acknowledgement
and effect readiness. Lease time is the *remaining* interval when the response was
formed. Workers conservatively calculate their deadline from the monotonic time
at request START, never response arrival. A response arriving after that deadline
cannot authorize starting or continuing work.

Renewal requires the matching previous token and boot epoch. A worker that lost
the initial response can reread its assignment, but an unauthenticated token retry
does not extend the old deadline. Token validity also binds worker incarnation.

Heartbeat replies include `manifests` for automatic distribution and
`configured_revisions` for explicit publication evidence in this gateway boot.
Workers persist desired manifests locally and repopulate the transient gateway
after restart. A new service can be adopted automatically. A differing local
revision may be replaced automatically only when `configured_revisions` names
the new revision and no replica conflict is reported.

Conflicting durable replicas observed before explicit selection in this gateway boot
block placement, including replicas arriving after restart grace. They
are excluded from automatic catalog adoption until explicitly resolved through
configure CAS. If the only returning replica contains an older spec and the newer
replica remains offline, a memory-only authority cannot prove which version was
last published. This is a declared limit, not silent rollback assurance.
Removal is intentionally absent in this version; adding it requires durable
tombstone distribution to avoid resurrection after restart.

## Effects, state and safe transfer

Every service must choose an effect policy:

- `stateless`: no external exclusive side effects requiring a shared fence.
- `externally_fenced`: a real resource adapter must install the existing fence
  before starting external effects. The worker returns a `fence_ack` containing
  `fenced:true`, and the exact grant token and boot epoch. Until accepted, the
  assignment remains fencing and validate returns false. The acknowledgement is
  an authenticated worker attestation, not independent hardware verification.
- `gateway_fenced` is reserved and rejected in this version. No mandatory
  production routing adapter is installed; a label or watchdog cannot substitute
  for that enforcement.

A running child cannot rely on periodically stopping itself for arbitrary
physical or stateful safety. A paused or isolated old instance may resume; the
mandatory effect boundary must reject its old grant. Relay firmware's existing
authority/generation/epoch, FreshIntent and neutral-on-disconnect rules remain
authoritative. Lease expiry is not permission to bypass firmware takeover rules.

An unhealthy owner, explicit release or rollout invalidates validation immediately
but retains the old expiry before issuing a replacement. A stop acknowledgement
alone does not shorten that interval. New physical command intent must be fresh.

State policy is either `stateless` or `externally_managed`. Local-only volumes
and undeclared state are refused for automatic relocation. External state
replication, consistency and actual resource authority transfer remain the
service adapter's responsibilities.

Idle coordination entries expire; the module writes no shared journal or
permanent heartbeat history. Workers own durable local configuration.

## Verification

`python -m unittest discover -s tests -p test_service_coordinator.py`

Tests exercise two workers, preference and sticky ownership, startup grace,
missing images, incompatible platform/image revision, primary loss, delayed
renewals, stale tokens, new boot epochs, partition/resume, explicit draining,
CAS rollout, conflicting replicas, policy admission, fence attestation and grant validation,
catalog distribution, authentication and idle retention. Existing stream tests
must also pass before publication.
