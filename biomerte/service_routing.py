"""Resolve declared logical services without coupling native work to the witness."""
import json
import re
import urllib.error
import urllib.request


class ServiceOwnerResolver:
    def __init__(self, origin, *, declarations=(), timeout=5):
        if not origin.startswith("https://"):
            raise ValueError("HTTPS service authority origin required")
        self.url = origin.rstrip("/") + "/api/service-authorities/resolve"
        self.declarations = declarations
        self.timeout = timeout

    def declaration(self, resource):
        declarations = self.declarations() if callable(self.declarations) else self.declarations
        if not isinstance(declarations, (list, tuple)):
            raise ValueError("Logical service declarations must be a list")
        selected = None
        for value in declarations:
            if (not isinstance(value, dict) or
                    not isinstance(value.get("service_id"), str) or not value["service_id"] or
                    not isinstance(value.get("authority"), str) or
                    not re.fullmatch("[a-f0-9]{64}", value["authority"]) or
                    not isinstance(value.get("resources"), list) or
                    any(not isinstance(r, str) or not r for r in value["resources"])):
                raise ValueError("Invalid logical service declaration")
            if resource in value["resources"]:
                if selected is not None:
                    raise ValueError("Logical service resource declarations overlap")
                selected = value
        return selected

    def __call__(self, resource):
        declaration = self.declaration(resource)
        if declaration is None:
            # Native availability must not depend on an unrelated service witness.
            return None
        request = urllib.request.Request(self.url,
            data=json.dumps({"resource": resource}).encode(),
            headers={"Content-Type": "application/json",
                     "User-Agent": "BiomeRTE-Rover-Server/1"})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                value = json.load(response)
        except (urllib.error.HTTPError, OSError, ValueError) as error:
            # A declared scope stays logical even if a restarted or unavailable
            # witness answers 404; it must never fall back to a native issuer.
            raise ConnectionError("Declared service ownership is unavailable") from error
        if (not isinstance(value, dict) or value.get("effect_ready") is not True or
                value.get("authority") != declaration["authority"] or
                value.get("service_id") != declaration["service_id"] or
                value.get("state") not in ("starting", "active")):
            raise ConnectionError("Service authority does not match its admitted declaration")
        return value["authority"]
