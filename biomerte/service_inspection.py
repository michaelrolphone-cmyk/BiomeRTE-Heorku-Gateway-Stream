"""Optional logical service inspection for existing native stream routers."""
import hashlib
import hmac
import json
import re
import time
import urllib.request


def service_authority_key(secret, authority):
    return hmac.new(secret.encode(), ("service-authority:" + authority).encode(),
                    hashlib.sha256).hexdigest()


def service_routes(secrets, validators, service_secret, inspect_service):
    """Optional logical authorities share derivation, never native routes.

    A derived credential still requires current authoritative inspection on
    every attachment and watcher pass. Unknown/stale services therefore fail
    closed without a host-specific authority list or a token-only fallback.
    """
    class Secrets(dict):
        def __missing__(self, authority):
            if not isinstance(authority, str) or not re.fullmatch("[a-f0-9]{64}", authority):
                raise KeyError(authority)
            return service_authority_key(service_secret, authority)

    class Validators(dict):
        def get(self, authority, default=None):
            if authority in self:
                return super().get(authority, default)
            if not isinstance(authority, str) or not re.fullmatch("[a-f0-9]{64}", authority):
                return default
            return lambda sid: inspect_service(authority, sid)

    return Secrets(secrets), Validators(validators)


def inspect_service(url, session, authority, secret):
    raw = json.dumps(dict(authority=authority, session=session, timestamp=int(time.time())),
                     sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    signature = hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
    request = urllib.request.Request(url, data=raw, headers={
        "Content-Type": "application/json", "Authorization": "Router " + signature,
        "User-Agent": "BiomeRTE-Stream-Router/1"})
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.load(response)


def add_services(config, secrets, validators):
    """Layer logical inspection over existing native validators and secrets."""
    seed = config.get("service_authority_secret")
    if not seed:
        return secrets, validators
    if not isinstance(seed, str):
        raise ValueError("Service authority secret must be a string")
    url = config.get("service_inspect_url", "")
    if not isinstance(url, str) or not url.startswith("https://"):
        raise ValueError("Authenticated HTTPS service inspection URL is required")
    return service_routes(secrets, validators, seed,
        lambda authority, session: inspect_service(url, session, authority,
            service_authority_key(seed, authority)))
