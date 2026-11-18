"""Resolve a logical authority before creating a new native resource lease."""
import json
import re
import urllib.error
import urllib.request


class ServiceOwnerResolver:
    def __init__(self, origin, *, timeout=5):
        if not origin.startswith("https://"):
            raise ValueError("HTTPS service authority origin required")
        self.url = origin.rstrip("/") + "/api/service-authorities/resolve"
        self.timeout = timeout

    def __call__(self, resource):
        request = urllib.request.Request(self.url,
            data=json.dumps({"resource": resource}).encode(),
            headers={"Content-Type": "application/json",
                     "User-Agent": "BiomeRTE-Rover-Server/1"})
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                value = json.load(response)
        except urllib.error.HTTPError as error:
            if error.code == 404:
                return None
            raise ConnectionError("Service ownership lookup is unavailable") from error
        except (OSError, ValueError) as error:
            raise ConnectionError("Service ownership lookup failed") from error
        if (not isinstance(value, dict) or value.get("effect_ready") is not True or
                not isinstance(value.get("authority"), str) or
                not re.fullmatch("[a-f0-9]{64}", value["authority"]) or
                value.get("state") not in ("starting", "active")):
            raise ConnectionError("Service authority is not admitted")
        return value["authority"]
