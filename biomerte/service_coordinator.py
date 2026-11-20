"""Ephemeral single-authority placement for Docker services.

This module grants execution placement, never replaces Biome resource ownership.
External effects require the existing resource fence. Gateway-fenced admission
is reserved until a mandatory routing adapter exists. One gateway process owns
this namespace. No assignment, heartbeat or grant is written to a journal.
"""
import hashlib
import hmac
import json
import math
import re
import secrets
import time
import uuid

from aiohttp import web


PROTOCOL = "biomerte-service-coordinator/1"
KEY_CONTEXT = b"biomerte/service-coordinator/v1"
DIGEST = re.compile(r"(?:sha256:[0-9a-f]{64}|[^\s]+@sha256:[0-9a-f]{64})\Z")
REVISION = re.compile(r"[0-9a-f]{64}\Z")
ADAPTER_PROTOCOL = "biomerte-service-adapter/1"
BINDING_KEYS = ("service_id", "manifest_revision", "worker_id",
                "worker_incarnation", "boot_epoch", "grant_token")


def grant_binding(value):
    """The complete identity of one placement, never a host-only identity."""
    return {key: value[key] for key in BINDING_KEYS}


def exact_binding(value, grant):
    return (isinstance(value, dict) and set(value) == set(BINDING_KEYS) and
            all(isinstance(value.get(key), str) and value[key] == grant.get(key)
                for key in BINDING_KEYS))


def coordinator_key(secret):
    if not isinstance(secret, str) or not secret:
        raise ValueError("Service coordinator secret is required")
    return hmac.new(secret.encode(), KEY_CONTEXT, hashlib.sha256).hexdigest()


def manifest_revision(manifest):
    body = {key: value for key, value in manifest.items() if key != "revision"}
    raw = json.dumps(body, sort_keys=True, separators=(",", ":"),
                     ensure_ascii=True, allow_nan=False).encode()
    return hashlib.sha256(raw).hexdigest()


def _number(value, name, positive=True):
    if type(value) not in (int, float) or not math.isfinite(value):
        raise ValueError(name + " must be finite")
    if (positive and value <= 0) or (not positive and value < 0):
        raise ValueError(name + " is outside its permitted range")
    return float(value)


def _text(value, name):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(name + " must be a nonempty string")
    return value


class Conflict(ValueError):
    pass


class Coordinator:
    """Synchronous transitions run atomically in aiohttp's single event loop."""

    def __init__(self, secret, *, max_lease_ttl_s=30, clock=time.monotonic,
                 reserved_authorities=(), checkpoint_head=None):
        self.key = coordinator_key(secret)
        self.max_lease_ttl_s = _number(max_lease_ttl_s, "max_lease_ttl_s")
        self.clock = clock
        self.boot_epoch = uuid.uuid4().hex
        self.boot_until = clock() + self.max_lease_ttl_s
        self.services = {}
        self.reserved_authorities = frozenset(reserved_authorities)
        self.checkpoint_head = checkpoint_head
        # The authority transport supplies a pure local liveness observation.
        self.preparation_connected = None
        self.witness_deadline = None
        self.publication_pending = None

    def _manifest(self, value):
        if not isinstance(value, dict):
            raise ValueError("manifest must be an object")
        value = json.loads(json.dumps(value, allow_nan=False))
        _text(value.get("id"), "manifest.id")
        revision = value.get("revision")
        if not isinstance(revision, str) or not REVISION.fullmatch(revision):
            raise ValueError("manifest.revision must be its canonical SHA256")
        if revision != manifest_revision(value):
            raise ValueError("manifest.revision does not bind the complete spec")
        images = value.get("images")
        if not isinstance(images, dict) or not images:
            raise ValueError("manifest.images must map platform to immutable image")
        for platform, image in images.items():
            _text(platform, "platform")
            if not isinstance(image, str) or not DIGEST.fullmatch(image):
                raise ValueError("images must be digest-pinned references or image IDs")
        effect = value.get("effect_policy")
        if effect == "gateway_fenced":
            raise ValueError("gateway_fenced is reserved: mandatory routing enforcement is not installed")
        if effect not in ("stateless", "externally_fenced"):
            raise ValueError("explicit supported effect_policy is required")
        if value.get("state_policy") not in ("stateless", "externally_managed"):
            raise ValueError("local-only or undeclared state cannot automatically migrate")
        ttl = _number(value.get("lease_ttl_s", 10), "lease_ttl_s")
        if ttl > self.max_lease_ttl_s:
            raise ValueError("lease_ttl_s exceeds the configured restart grace bound")
        _number(value.get("worker_ttl_s", 10), "worker_ttl_s")
        _number(value.get("startup_grace_s", 20), "startup_grace_s")
        preferred = value.get("preferred_workers", [])
        if not isinstance(preferred, list) or len(set(preferred)) != len(preferred):
            raise ValueError("preferred_workers must be a unique ordered list")
        for worker in preferred:
            _text(worker, "preferred worker")
        activation = value.get("activation")
        if activation is not None:
            if (not isinstance(activation, dict) or
                    activation.get("protocol") != ADAPTER_PROTOCOL):
                raise ValueError("unsupported activation protocol")
            _text(activation.get("binding"), "activation.binding")
            if effect != "externally_fenced":
                raise ValueError("an activation adapter requires external fencing")
        authority = value.get("authority")
        if authority is not None:
            if not activation or not isinstance(authority, dict):
                raise ValueError("logical authority requires an activation adapter")
            identity = authority.get("id")
            if (not isinstance(identity, str) or not REVISION.fullmatch(identity) or
                    identity in self.reserved_authorities):
                raise ValueError("logical authority must be distinct from native authorities")
            kind = authority.get("kind", "resource")
            if kind not in ("resource", "controller"):
                raise ValueError("unsupported logical authority kind")
            controller = kind == "controller"
            for name in ("resources", "providers"):
                entries = authority.get(name)
                if (not isinstance(entries, list) or not controller and not entries or
                        any(not isinstance(entry, str) or not entry for entry in entries) or
                        len(set(entries)) != len(entries)):
                    raise ValueError("authority." + name + " must contain unique exact identities")
            if any(not REVISION.fullmatch(p) for p in authority["providers"]):
                raise ValueError("authority.providers must contain actor IDs")
            aliases = authority.get("controller_aliases", [])
            if controller:
                if (not isinstance(aliases, list) or not aliases or
                        any(not isinstance(alias, str) or not REVISION.fullmatch(alias)
                            or alias not in self.reserved_authorities for alias in aliases) or
                        len(set(aliases)) != len(aliases)):
                    raise ValueError("controller aliases must be unique registered native authorities")
                if value.get("state_policy") != "externally_managed":
                    raise ValueError("controller runtime requires externally managed durable state")
            elif "controller_aliases" in authority:
                raise ValueError("controller aliases require an explicit controller authority")
            for sid, item in self.services.items():
                other = item["manifest"].get("authority")
                if sid == value["id"] or other is None:
                    continue
                if (identity == other["id"] or
                        set(authority["resources"]) & set(other["resources"]) or
                        set(authority["providers"]) & set(other["providers"]) or
                        set(aliases) & set(other.get("controller_aliases", []))):
                    raise ValueError("logical authority scope is already registered")
        wait = value.get("wait_for_external_fence", False)
        if type(wait) is not bool:
            raise ValueError("wait_for_external_fence must be boolean")
        if wait and not authority:
            raise ValueError("external fence waiting requires a scoped logical authority")
        return value

    def _new_service(self, manifest, now):
        return dict(manifest=manifest, candidates={}, grant=None,
                    last_seen=now, reason="waiting_for_prepared_worker",
                    conflicting_revisions=set(), configured=False)

    def configure(self, manifest, expected_revision=None):
        """Explicit CAS for a rollout; stale worker heartbeats cannot change it."""
        manifest = self._manifest(manifest)
        now = self.clock()
        item = self.services.get(manifest["id"])
        if item is None:
            if expected_revision is not None:
                raise Conflict("service is not registered in this boot")
            item = self._new_service(manifest, now)
            self.services[manifest["id"]] = item
        elif item.get("conflicting_revisions") and expected_revision != item["manifest"]["revision"]:
            raise Conflict("replica conflict requires an explicit expected_revision")
        if item["manifest"]["revision"] != manifest["revision"]:
            if expected_revision != item["manifest"]["revision"]:
                raise Conflict("expected_revision must match the current manifest")
            if item["grant"]:
                item["grant"]["draining"] = True
            item["manifest"] = manifest
            item["last_seen"] = now
            item["reason"] = "manifest_update_draining"
        item["conflicting_revisions"].clear()
        item["configured"] = True
        return self._summary(manifest["id"], item, now)

    def _candidate_reason(self, item, candidate, now, grant=None):
        manifest = item["manifest"]
        if not candidate:
            return "worker_unavailable"
        if now - candidate["seen"] >= manifest.get("worker_ttl_s", 10):
            return "worker_heartbeat_expired"
        if candidate["manifest_revision"] != manifest["revision"]:
            return "manifest_revision_mismatch"
        if not candidate["prepared"]:
            return "image_or_runtime_not_prepared"
        if not candidate["dependencies_ready"]:
            return "dependencies_not_ready"
        expected = manifest["images"].get(candidate["platform"])
        if expected is None:
            return "platform_not_supported"
        if candidate["image_ref"] != expected:
            return "image_revision_mismatch"
        if candidate["healthy"] is False:
            return "service_unhealthy"
        if grant and candidate["healthy"] is None:
            if self._verified_fence_wait(item, candidate, grant):
                grant["external_fence_wait"] = True
            else:
                # A real fence ends the external wait. Activation still gets a
                # bounded startup window; it cannot borrow an indefinite wait.
                if (grant.get("external_fence_wait") and
                        "activation_started" not in grant and
                        self._fence_ready(item, candidate, grant)):
                    grant["activation_started"] = now
                started = grant.get("activation_started", grant["started"])
                if now - started >= manifest.get("startup_grace_s", 20):
                    return "startup_health_timeout"
        if candidate.get("released"):
            return "worker_released"
        return None

    def _state_ready(self, item, candidate, grant):
        if not item["manifest"].get("activation"):
            return True
        proof = candidate.get("state_proof")
        ready = (candidate.get("state_ready") is True and isinstance(proof, dict) and
                 proof.get("protocol") == ADAPTER_PROTOCOL and
                 exact_binding(proof.get("binding"), grant) and
                 isinstance(proof.get("checkpoint"), str) and
                 REVISION.fullmatch(proof["checkpoint"]) is not None and
                 all(proof.get(flag) is True
                     for flag in ("fresh", "custody_complete", "ready")))
        if not ready or not item["manifest"].get("authority"):
            return ready
        # The preparation baseline must first be published durably by helpers.
        # It stays bound to B as newer live checkpoints advance the high water.
        head = self.checkpoint_head(grant["service_id"]) if self.checkpoint_head else None
        if (not head or head.get("boot_epoch") != self.boot_epoch or
                not exact_binding(head.get("binding"), grant)):
            return False
        baseline = grant.get("prepared_checkpoint")
        if baseline is not None:
            return (baseline["digest"] == proof["checkpoint"] and
                    head["sequence"] >= baseline["sequence"])
        if head.get("digest") != proof["checkpoint"]:
            return False
        grant["prepared_checkpoint"] = {"digest": head["digest"], "sequence": head["sequence"]}
        return True

    def _fence_ready(self, item, candidate, grant):
        if item["manifest"].get("activation"):
            # An older successful proof cannot mask a new missing/stale proof.
            ack = candidate.get("fence_ack")
            return (self._state_ready(item, candidate, grant) and
                    isinstance(ack, dict) and ack.get("fenced") is True and
                    exact_binding(ack.get("binding"), grant) and
                    ack.get("boot_epoch") == self.boot_epoch and
                    ack.get("grant_token") == grant["grant_token"] and
                    ack.get("checkpoint") == candidate["state_proof"]["checkpoint"])
        if item["manifest"]["effect_policy"] != "externally_fenced":
            return True
        ack = grant.get("fence_ack") or candidate.get("fence_ack")
        return (isinstance(ack, dict) and ack.get("fenced") is True and
                ack.get("boot_epoch") == self.boot_epoch and
                ack.get("grant_token") == grant["grant_token"])

    def _verified_fence_wait(self, item, candidate, grant):
        """A live scoped recovery process may wait without creating new intent.

        This is opt-in because registration must follow durable publication of
        the physical fence intent. Readiness or a cached checkpoint alone is
        insufficient. The transport callback must not call back into reconcile.
        """
        manifest = item["manifest"]
        if (not manifest.get("wait_for_external_fence") or
                not manifest.get("authority") or candidate is None or
                candidate.get("healthy") is not None or
                "activation_started" in grant or
                not self._state_ready(item, candidate, grant) or
                self._fence_ready(item, candidate, grant)):
            return False
        baseline = grant.get("prepared_checkpoint")
        head = self.checkpoint_head(grant["service_id"]) if self.checkpoint_head else None
        if (not baseline or not head or
                head.get("boot_epoch") != self.boot_epoch or
                not exact_binding(head.get("binding"), grant) or
                head.get("sequence", -1) <= baseline["sequence"]):
            return False
        connected = self.preparation_connected
        return connected is not None and connected(grant_binding(grant)) is True

    def _reconcile(self, service_id, item, now):
        manifest = item["manifest"]
        ttl = manifest.get("worker_ttl_s", 10)
        item["candidates"] = {key: value for key, value in item["candidates"].items()
                              if now - value["seen"] < ttl}
        grant = item["grant"]
        durable_limit = self.witness_deadline() if self.witness_deadline else None
        if grant and durable_limit is not None:
            grant["expires"] = min(grant["expires"], durable_limit)
        if grant and now >= grant["expires"]:
            old_candidate = item["candidates"].get(grant["worker_key"])
            if old_candidate:
                old_candidate["released"] = True
            item["grant"] = grant = None
        if durable_limit is not None and now >= durable_limit:
            item["reason"] = "durable_witness_unavailable"
            return
        if grant is None and self.publication_pending and self.publication_pending(service_id):
            item["reason"] = "checkpoint_publication_reconciliation"
            return
        if item.get("conflicting_revisions"):
            if grant:
                grant["draining"] = True
            item["reason"] = "replica_revision_conflict"
            return
        if grant:
            candidate = item["candidates"].get(grant["worker_key"])
            reason = self._candidate_reason(item, candidate, now, grant)
            if grant["manifest_revision"] != manifest["revision"]:
                reason = "manifest_update_draining"
            if reason:
                grant["draining"] = True
                item["reason"] = reason
            if grant["draining"]:
                return
            if not self._state_ready(item, candidate, grant):
                if now - grant["started"] >= manifest.get("startup_grace_s", 20):
                    grant["draining"] = True
                    item["reason"] = "state_preparation_timeout"
                else:
                    item["reason"] = "awaiting_state_preparation"
                return
            if (manifest["effect_policy"] == "externally_fenced" and
                    not self._fence_ready(item, candidate, grant)):
                if self._verified_fence_wait(item, candidate, grant):
                    grant["external_fence_wait"] = True
                    item["reason"] = "waiting_for_external_fence"
                elif now - grant["started"] >= manifest.get("startup_grace_s", 20):
                    grant["draining"] = True
                    item["reason"] = "external_fence_timeout"
                else:
                    item["reason"] = "awaiting_external_fence"
                return
            item["reason"] = "active" if candidate["healthy"] else "starting"
            return
        if now < self.boot_until:
            item["reason"] = "restart_grace"
            return
        eligible = [(key, candidate) for key, candidate in item["candidates"].items()
                    if self._candidate_reason(item, candidate, now) is None]
        if not eligible:
            item["reason"] = "waiting_for_prepared_worker"
            return
        preferred = manifest.get("preferred_workers", [])
        def rank(pair):
            worker_id, incarnation = pair[0]
            return (preferred.index(worker_id) if worker_id in preferred else len(preferred),
                    worker_id, incarnation)
        worker_key, candidate = min(eligible, key=rank)
        item["grant"] = dict(service_id=service_id,
            manifest_revision=manifest["revision"], boot_epoch=self.boot_epoch,
            grant_token=secrets.token_urlsafe(32), worker_key=worker_key,
            worker_id=worker_key[0], worker_incarnation=worker_key[1],
            started=now, expires=min(now + manifest.get("lease_ttl_s", 10), durable_limit)
                if durable_limit is not None else now + manifest.get("lease_ttl_s", 10),
            draining=False)
        item["reason"] = ("awaiting_state_preparation" if manifest.get("activation") else
                          "awaiting_external_fence"
                          if manifest["effect_policy"] == "externally_fenced"
                          else "starting")

    def _matches(self, grant, value):
        return bool(grant and value.get("boot_epoch") == self.boot_epoch and
                    value.get("worker_id") == grant["worker_id"] and
                    value.get("worker_incarnation") == grant["worker_incarnation"] and
                    isinstance(value.get("grant_token"), str) and
                    hmac.compare_digest(value["grant_token"], grant["grant_token"]))

    def _prune(self, now):
        for service_id, item in list(self.services.items()):
            self._reconcile(service_id, item, now)
            retention = max(300, self.max_lease_ttl_s * 4,
                            item["manifest"].get("worker_ttl_s", 10) * 4)
            if (not item["manifest"].get("authority") and
                    not item["candidates"] and item["grant"] is None and
                    now - item["last_seen"] >= retention):
                del self.services[service_id]

    def heartbeat(self, body):
        if not isinstance(body, dict):
            raise ValueError("heartbeat must be an object")
        worker = _text(body.get("worker_id"), "worker_id")
        incarnation = _text(body.get("worker_incarnation"), "worker_incarnation")
        rows = body.get("services")
        if not isinstance(rows, list):
            raise ValueError("services must be a list")
        now = self.clock()
        self._prune(now)
        seen_ids, accepted_ids, errors = set(), set(), {}
        # Validate each service independently: one bad rollout must not expire
        # unrelated healthy services in the same worker heartbeat.
        for row in rows:
            service_id = None
            try:
                if not isinstance(row, dict):
                    raise ValueError("service heartbeat must be an object")
                manifest = self._manifest(row.get("manifest"))
                service_id = manifest["id"]
                if service_id in seen_ids:
                    raise ValueError("duplicate service heartbeat")
                seen_ids.add(service_id)
                if row.get("manifest_revision") != manifest["revision"]:
                    raise ValueError("reported manifest_revision does not match")
                for flag in ("prepared", "dependencies_ready"):
                    if type(row.get(flag)) is not bool:
                        raise ValueError(flag + " must be a boolean")
                if row.get("healthy") is not None and type(row["healthy"]) is not bool:
                    raise ValueError("healthy must be true, false or null")
                platform = _text(row.get("platform"), "platform")
                image_ref = _text(row.get("image_ref"), "image_ref")
                item = self.services.get(service_id)
                if item is None:
                    item = self._new_service(manifest, now)
                    self.services[service_id] = item
                if item["manifest"]["revision"] != manifest["revision"]:
                    item["last_seen"] = now
                    if not item.get("configured"):
                        item["conflicting_revisions"].update(
                            (item["manifest"]["revision"], manifest["revision"]))
                    raise Conflict("manifest_conflict: configure with expected_revision")
                candidate = dict(seen=now, manifest_revision=manifest["revision"],
                    platform=platform, image_ref=image_ref, prepared=row["prepared"],
                    dependencies_ready=row["dependencies_ready"],
                    healthy=row.get("healthy"), fence_ack=row.get("fence_ack"),
                    state_ready=row.get("state_ready"), state_proof=row.get("state_proof"),
                    checkpoint=row.get("checkpoint"))
                item["candidates"][(worker, incarnation)] = candidate
                item["last_seen"] = now
                accepted_ids.add(service_id)
                grant = item["grant"]
                identity = dict(row, worker_id=worker, worker_incarnation=incarnation)
                if (self._matches(grant, identity) and not grant["draining"] and
                        self._candidate_reason(item, candidate, now, grant) is None):
                    limit = self.witness_deadline() if self.witness_deadline else None
                    grant["expires"] = (min(now + manifest.get("lease_ttl_s", 10), limit)
                        if limit is not None else now + manifest.get("lease_ttl_s", 10))
                    if self._fence_ready(item, candidate, grant):
                        grant["fence_ack"] = candidate.get("fence_ack") or grant.get("fence_ack")
                self._reconcile(service_id, item, now)
            except (ValueError, TypeError) as error:
                errors[service_id or "invalid_service_" + str(len(errors))] = str(error)
        assignments = []
        for service_id, item in self.services.items():
            self._reconcile(service_id, item, now)
            grant = item["grant"]
            if (service_id in accepted_ids and grant and not grant["draining"] and
                    grant["worker_key"] == (worker, incarnation)):
                assignments.append(self._grant_view(item, now))
        statuses = {sid: self._summary(sid, item, now)
                    for sid, item in self.services.items() if sid in seen_ids}
        return dict(protocol=PROTOCOL, boot_epoch=self.boot_epoch,
                    boot_grace_remaining_s=max(0, self.boot_until - now),
                    assignments=assignments, statuses=statuses, errors=errors,
                    manifests=self._catalog_manifests(),
                    configured_revisions=self._configured_revisions())

    def _grant_view(self, item, now):
        grant = item["grant"]
        candidate = item["candidates"].get(grant["worker_key"], {})
        value = {key: grant[key] for key in (
            "service_id", "manifest_revision", "boot_epoch", "grant_token",
            "worker_id", "worker_incarnation")}
        value.update(lease_ttl_s=max(0, grant["expires"] - now),
                     state=item["reason"],
                     state_ready=self._state_ready(item, candidate, grant),
                     effect_ready=self._fence_ready(item, candidate, grant))
        return value

    def _summary(self, service_id, item, now):
        grant = item["grant"]
        return dict(service_id=service_id,
            manifest_revision=item["manifest"]["revision"],
            state=item["reason"],
            conflicting_revisions=sorted(item.get("conflicting_revisions", ())),
            desired_manifest=(None if item.get("conflicting_revisions")
                              else item["manifest"]),
            owner=(dict(worker_id=grant["worker_id"],
                        worker_incarnation=grant["worker_incarnation"],
                        lease_ttl_s=max(0, grant["expires"] - now),
                        draining=grant["draining"]) if grant else None),
            candidates=[dict(worker_id=key[0], worker_incarnation=key[1],
                             reason=self._candidate_reason(item, candidate, now),
                             checkpoint=candidate.get("checkpoint"))
                        for key, candidate in sorted(item["candidates"].items())])

    def _catalog_manifests(self):
        return [json.loads(json.dumps(item["manifest"]))
                for _, item in sorted(self.services.items())
                if not item.get("conflicting_revisions")]

    def _configured_revisions(self):
        return {sid: item["manifest"]["revision"]
                for sid, item in self.services.items()
                if item.get("configured") and not item.get("conflicting_revisions")}

    def catalog(self):
        now = self.clock()
        self._prune(now)
        return dict(protocol=PROTOCOL, boot_epoch=self.boot_epoch,
                    manifests=self._catalog_manifests(),
                    configured_revisions=self._configured_revisions(),
                    conflicts={sid: sorted(item["conflicting_revisions"])
                               for sid, item in self.services.items()
                               if item.get("conflicting_revisions")})

    def status(self):
        now = self.clock()
        self._prune(now)
        return dict(protocol=PROTOCOL, authority_mode="single_process",
            boot_epoch=self.boot_epoch, max_lease_ttl_s=self.max_lease_ttl_s,
            boot_grace_remaining_s=max(0, self.boot_until - now),
            supported_features=["verified_external_fence_wait"] +
                (["durable_checkpoint_witness"] if self.witness_deadline else []),
            durable_witness_ready=(self.witness_deadline() > now if self.witness_deadline else None),
            services={sid: self._summary(sid, item, now)
                      for sid, item in self.services.items()})

    def validate_preparation(self, value):
        """Admission for staging/recovery only; this is never an effect grant."""
        now = self.clock()
        self._prune(now)
        item = self.services.get(value.get("service_id")) if isinstance(value, dict) else None
        grant = item["grant"] if item else None
        valid = bool(grant and exact_binding(value, grant) and
                     grant["boot_epoch"] == self.boot_epoch and
                     not grant["draining"] and now < grant["expires"] and
                     grant["manifest_revision"] == item["manifest"]["revision"])
        return dict(valid=valid, reason="valid_preparation" if valid else "stale_or_revoked_grant",
                    boot_epoch=self.boot_epoch,
                    lease_ttl_s=max(0, grant["expires"] - now) if valid else 0)

    def current_grant(self, service_id, *, preparation=False):
        now = self.clock()
        self._prune(now)
        item = self.services.get(service_id)
        grant = item["grant"] if item else None
        if not grant:
            return None
        binding = grant_binding(grant)
        if not self.validate_preparation(binding)["valid"]:
            return None
        if not preparation and not self.validate(binding)["valid"]:
            return None
        return self._grant_view(item, now)

    def worker_current(self, worker_id, worker_incarnation, service_id=None):
        """Helpers follow supervisor liveness, independently of child health."""
        now = self.clock()
        self._prune(now)
        incarnations = set()
        eligible = False
        for sid, item in self.services.items():
            for (worker, incarnation), candidate in item["candidates"].items():
                if worker != worker_id:
                    continue
                incarnations.add(incarnation)
                if ((service_id is None or sid == service_id) and
                        incarnation == worker_incarnation and
                        candidate["manifest_revision"] == item["manifest"]["revision"] and
                        not item.get("conflicting_revisions")):
                    eligible = True
        return eligible and incarnations == {worker_incarnation}

    def validate(self, value):
        now = self.clock()
        self._prune(now)
        item = self.services.get(value.get("service_id"))
        grant = item["grant"] if item else None
        valid = bool(self._matches(grant, value) and not grant["draining"] and
                     (value.get("manifest_revision") in (None, grant["manifest_revision"])))
        if valid and item["manifest"].get("activation"):
            valid = exact_binding(value, grant)
        reason = "stale_or_revoked_grant"
        if valid:
            candidate = item["candidates"].get(grant["worker_key"], {})
            valid = self._fence_ready(item, candidate, grant)
            reason = "valid" if valid else "awaiting_external_fence"
        return dict(valid=bool(valid), reason=reason, boot_epoch=self.boot_epoch,
                    lease_ttl_s=max(0, grant["expires"] - now) if valid else 0)

    def release(self, value):
        now = self.clock()
        self._prune(now)
        item = self.services.get(value.get("service_id"))
        grant = item["grant"] if item else None
        if not self._matches(grant, value):
            raise Conflict("stale_or_revoked_grant")
        grant["draining"] = True
        item["reason"] = "released_draining"
        candidate = item["candidates"].get(grant["worker_key"])
        if candidate:
            candidate["released"] = True
        # A stop acknowledgement alone is not an external resource fence.
        # All policies keep the old expiry before another instance is granted.
        return self._summary(value["service_id"], item, now)

    def mount(self, app):
        def authorize(request):
            if request.headers.get("Origin"):
                raise web.HTTPForbidden()
            expected = "Bearer " + self.key
            if not hmac.compare_digest(request.headers.get("Authorization", ""), expected):
                raise web.HTTPUnauthorized()

        async def handler(request):
            authorize(request)
            try:
                operation = request.match_info["operation"]
                if operation == "status" and request.method == "GET":
                    result = self.status()
                elif operation == "catalog":
                    result = self.catalog()
                else:
                    body = await request.json()
                    if not isinstance(body, dict):
                        raise ValueError("JSON object required")
                    if operation == "heartbeat":
                        result = self.heartbeat(body)
                    elif operation == "configure":
                        result = self.configure(body.get("manifest"),
                                                body.get("expected_revision"))
                    elif operation == "validate":
                        result = self.validate(body)
                    elif operation == "validate-preparation":
                        result = self.validate_preparation(body)
                    elif operation == "release":
                        result = self.release(body)
                    else:
                        raise web.HTTPNotFound()
                return web.json_response(result, headers={"Cache-Control": "no-store"})
            except Conflict as error:
                return web.json_response({"error": str(error)}, status=409)
            except (ValueError, TypeError, KeyError) as error:
                return web.json_response({"error": str(error)}, status=400)

        app.router.add_get("/api/service-coordinator/{operation:status|catalog}", handler)
        app.router.add_post(
            "/api/service-coordinator/{operation:heartbeat|configure|validate|validate-preparation|release|catalog}",
            handler)
