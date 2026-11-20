# Waiting for an external physical fence

This candidate changes only an explicitly opted-in scoped authority. Existing
manifests retain their bounded startup behavior.

## Problem and evidence

A physical provider can legitimately take longer than a process startup grace
to return its signed fence acknowledgement. Restarting a successfully prepared
authority under a new grant creates a new physical fence intent. Repeating that
cycle grows its durable state and rewrites full replicated checkpoints.

The November20 incident established repeated new bindings and fence intent
records, a large relay checkpoint, and SD write pressure. Both absent authority
links and later live exact-binding links were observed. This change does not
identify the initial cause of missing registration or prove hardware recovery.

## Opt-in contract

An operator may add `"wait_for_external_fence": true` inside the existing
`activation` object of a scoped logical authority manifest. The full manifest
revision still changes and must pass the existing compare-and-set rollout.

Enable this only for an adapter whose authority connects its recovery WebSocket
**after** publishing a new, durable physical fence intent. BiomeRTE's scoped
AuthorityDaemon currently orders start_fences and its checkpoint publication
before authority registration. A generic child that can register earlier does
not satisfy this contract.

The coordinator preserves a preparation binding across startup grace only while
all these independently checked conditions remain true:

- The worker is current, its image and dependencies are ready, and its finite
  placement lease continues to renew.
- Its exact prepared baseline was acknowledged by the current witness.
- The witness holds a later checkpoint under this same full binding.
- That exact authority WebSocket completed registration and remains open.
- The child is still in preparation: the runner reports unknown readiness, and
  no external fence acknowledgement has been admitted.

The existing WebSocket ping/pong heartbeat closes an unresponsive connection.
A missing or closed link does not receive the exception. Status inspection and
socket activity never extend the placement lease.

The resulting status is `waiting_for_external_fence`. Effects remain closed;
no acknowledgement, generation, command outcome or completed operation is
invented. The coordinator does not modify or prune any checkpoint or lease.

## Readiness and liveness

The current generic runner checks that the staged child is running, then reports
`healthy: null` until activation. Its Docker health endpoint can legitimately
return503 during that period. This is not an advertised `healthy: false`.

An explicit false report still drains the grant. After a real fence arrives,
activation receives its own bounded startup window using the existing startup
grace duration. Failure to become healthy then revokes the grant. A later
unhealthy active process also retains the existing failure behavior.

Loss of worker heartbeat, placement expiry, witness restart, manifest change,
explicit release or loss of the required current state invalidates the old
binding. A successor still needs the exact current state and a new physical
fence. The exception cannot be re-entered after activation has started.

## Verification and limits

`tests/test_external_fence_wait.py` exercises repeated waiting windows with the
same binding/head; absent registration; loss of link, state, epoch and binding;
process failure; worker and lease expiry; revision/release/restart boundaries;
real fence arrival; and bounded activation. Real local WebSocket cases verify
registration, closure and missing-pong behavior without physical devices.

Run the targeted suite from an isolated checkout:

```sh
PYTHONPATH=tests python -m unittest -v test_external_fence_wait test_service_activation test_service_coordinator test_service_transport
```

The authenticated read-only coordinator status includes
`supported_features: ["verified_external_fence_wait"]`. Verify this marker
after deployment before opting in a manifest; it does not configure or admit
any service.

No production manifest is changed by this branch. The candidate requires review,
successful tests, source correspondence and an explicit manifest opt-in before
live use. Its expected benefit is to stop needless grant/intent churn while
recovery traffic remains available; it does not repair a broken route or certify
a physical provider's state.
