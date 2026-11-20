"""Git-pinned public witness identity, resolved before coordinator startup.

Only origin, witness identity and bounded lease duration belong in the file.
The existing service-admin bearer derives a separate deployment credential.
No remote environment mutation or credential publication is provided here.
"""
import hashlib
import hmac
import json
import math
import pathlib
import re
from urllib.parse import urlsplit

FORMAT = "biomerte-durable-witness-deployment/1"
KEY_CONTEXT = b"biomerte/durable-witness/deployment/v1\0"
FILE_NAME = "durable-witness.json"
ENV_FIELDS = ("DURABLE_WITNESS_ORIGIN", "DURABLE_WITNESS_ID",
              "DURABLE_WITNESS_LEASE_S", "DURABLE_WITNESS_SECRET")


def canonical_origin(value):
    if not isinstance(value, str) or not value.isascii() or any(c.isspace() for c in value):
        raise ValueError("Canonical HTTPS durable witness origin required")
    try:
        parts = urlsplit(value)
        host, port = parts.hostname, parts.port
        if (parts.scheme != "https" or not host or parts.username is not None
                or parts.password is not None or parts.path or parts.query or parts.fragment
                or any(ord(c) < 32 or ord(c) == 127 for c in value)
                or port == 0):
            raise ValueError()
        authority = "[" + host + "]" if ":" in host else host
        expected = "https://" + authority
        if port not in (None, 443):
            expected += ":" + str(port)
        if value != expected:
            raise ValueError()
    except (ValueError, TypeError):
        raise ValueError("Canonical HTTPS durable witness origin required") from None
    return value


def witness_identity(value):
    if not isinstance(value, str) or re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value) is None:
        raise ValueError("Exact durable witness identity required")
    return value


def lease_duration(value):
    if (type(value) not in (int, float) or not math.isfinite(value)
            or not 60 <= value <= 600):
        raise ValueError("Durable witness lease must be finite and between 60 and 600 seconds")
    return value


def derive_witness_secret(coordinator_token, origin, witness_id):
    """Derive from the existing 64-hex service-admin bearer, never its parent."""
    if not isinstance(coordinator_token, str) or re.fullmatch(r"[0-9a-f]{64}", coordinator_token) is None:
        raise ValueError("Exact service coordinator bearer required")
    origin, witness_id = canonical_origin(origin), witness_identity(witness_id)
    message = KEY_CONTEXT + origin.encode("ascii") + b"\0" + witness_id.encode("ascii")
    return hmac.new(coordinator_token.encode("ascii"), message, hashlib.sha256).hexdigest()


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError("Duplicate durable witness deployment field")
        value[key] = item
    return value


def _invalid_constant(value):
    raise ValueError("Invalid durable witness deployment number")


def read_deployment(path):
    path = pathlib.Path(path)
    try:
        with path.open("rb") as stream:
            raw = stream.read(16385)
    except FileNotFoundError:
        if path.is_symlink():
            raise ValueError("Unable to read durable witness deployment configuration") from None
        return None
    except OSError:
        raise ValueError("Unable to read durable witness deployment configuration") from None
    try:
        if len(raw) > 16384:
            raise ValueError()
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_unique_object,
                           parse_constant=_invalid_constant)
        if (not isinstance(value, dict)
                or set(value) != {"format", "origin", "witness_id", "lease_s"}
                or value["format"] != FORMAT):
            raise ValueError()
        return dict(origin=canonical_origin(value["origin"]),
                    witness_id=witness_identity(value["witness_id"]),
                    lease_s=lease_duration(value["lease_s"]))
    except (ValueError, TypeError, UnicodeError):
        raise ValueError("Invalid durable witness deployment configuration") from None


def deployment_witness_config(environ, coordinator_token, path, *, parent_secret=None):
    # A present invalid file is fatal even when explicit environment settings exist.
    pinned = read_deployment(path)
    if pinned is None and not any(name in environ for name in ENV_FIELDS):
        return {}
    if pinned is None:
        if "DURABLE_WITNESS_ORIGIN" not in environ or "DURABLE_WITNESS_SECRET" not in environ:
            raise ValueError("Environment witness configuration requires origin and explicit credential")
        public = dict(origin=canonical_origin(environ["DURABLE_WITNESS_ORIGIN"]),
                      witness_id=witness_identity(environ.get("DURABLE_WITNESS_ID", "biomerte-services")),
                      lease_s=120)
        if "DURABLE_WITNESS_LEASE_S" in environ:
            try:
                public["lease_s"] = lease_duration(float(environ["DURABLE_WITNESS_LEASE_S"]))
            except (TypeError, ValueError):
                raise ValueError("Invalid durable witness lease environment setting") from None
    else:
        public = pinned
        for env_name, field, validate in (
                ("DURABLE_WITNESS_ORIGIN", "origin", canonical_origin),
                ("DURABLE_WITNESS_ID", "witness_id", witness_identity),
                ("DURABLE_WITNESS_LEASE_S", "lease_s", lambda v: lease_duration(float(v)))):
            if env_name in environ:
                try:
                    configured = validate(environ[env_name])
                except (TypeError, ValueError):
                    raise ValueError("Invalid explicit durable witness deployment setting") from None
                if configured != pinned[field]:
                    raise ValueError("Explicit durable witness settings conflict with the pinned deployment")
    if "DURABLE_WITNESS_SECRET" in environ:
        secret = environ["DURABLE_WITNESS_SECRET"]
        if (not isinstance(secret, str) or len(secret) < 32
                or secret in (coordinator_token, parent_secret)):
            raise ValueError("Separate durable witness credential required")
    else:
        secret = derive_witness_secret(coordinator_token, public["origin"], public["witness_id"])
    return dict(durable_witness_origin=public["origin"],
                durable_witness_id=public["witness_id"],
                durable_witness_lease_s=public["lease_s"],
                durable_witness_secret=secret)
