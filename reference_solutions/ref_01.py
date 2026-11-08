import threading
from typing import Optional, Dict, Any

class WebhookDeduplicator:
    def __init__(self):
        self._lock = threading.Lock()
        self._seen_keys = set()
        self._entities: Dict[str, Dict[str, Any]] = {}

    def process_event(self, event: dict) -> dict:
        if not isinstance(event, dict):
            return {"status": "INVALID", "current_state": None, "applied_version": None}

        # Validate required fields
        if "idempotency_key" not in event or "entity_id" not in event:
            return {"status": "INVALID", "current_state": None, "applied_version": None}
        if "event_ts" not in event or "version" not in event or "payload" not in event:
            return {"status": "INVALID", "current_state": None, "applied_version": None}

        k = event["idempotency_key"]
        eid = event["entity_id"]
        ts = event["event_ts"]
        ver = event["version"]
        payload = event["payload"]

        # Type checks: ensure int is not bool, strings are non-empty, payload is dict
        if not isinstance(k, str) or len(k.strip()) == 0:
            return {"status": "INVALID", "current_state": None, "applied_version": None}
        if not isinstance(eid, str) or len(eid.strip()) == 0:
            return {"status": "INVALID", "current_state": None, "applied_version": None}
        if type(ts) is not int or isinstance(ts, bool):
            return {"status": "INVALID", "current_state": None, "applied_version": None}
        if type(ver) is not int or isinstance(ver, bool):
            return {"status": "INVALID", "current_state": None, "applied_version": None}
        if not isinstance(payload, dict):
            return {"status": "INVALID", "current_state": None, "applied_version": None}

        with self._lock:
            ent = self._entities.get(eid)
            current_state = dict(ent["state"]) if (ent and ent["state"] is not None) else None
            applied_version = ent["last_version"] if (ent and ent["last_version"] != -1) else None

            # Duplicate check
            if k in self._seen_keys:
                return {
                    "status": "DUPLICATE",
                    "current_state": current_state,
                    "applied_version": applied_version,
                }

            last_ts = ent["last_ts"] if ent else -1
            last_version = ent["last_version"] if ent else -1

            if (ts, ver) > (last_ts, last_version):
                new_state = dict(payload)
                self._entities[eid] = {
                    "state": new_state,
                    "last_ts": ts,
                    "last_version": ver,
                }
                self._seen_keys.add(k)
                return {
                    "status": "APPLIED",
                    "current_state": dict(new_state),
                    "applied_version": ver,
                }
            else:
                self._seen_keys.add(k)
                return {
                    "status": "STALE_IGNORED",
                    "current_state": current_state,
                    "applied_version": applied_version,
                }
