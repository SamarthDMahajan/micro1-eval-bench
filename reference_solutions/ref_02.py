import threading
from typing import Optional, Dict, Any

class LeaseManager:
    def __init__(self):
        self._lock = threading.Lock()
        self._token_seq = 0
        self._leases: Dict[str, Dict[str, Any]] = {}
        self._storage: Dict[str, Dict[str, Any]] = {}

    def acquire(self, resource_id: str, worker_id: str, ttl_ms: int, now_ms: int) -> dict:
        with self._lock:
            lease = self._leases.get(resource_id)
            if lease and now_ms < lease["expires_at_ms"]:
                return {
                    "granted": False,
                    "fencing_token": None,
                    "expires_at_ms": lease["expires_at_ms"],
                }

            self._token_seq += 1
            token = self._token_seq
            expires_at = now_ms + ttl_ms
            self._leases[resource_id] = {
                "worker_id": worker_id,
                "fencing_token": token,
                "expires_at_ms": expires_at,
            }

            if resource_id not in self._storage:
                self._storage[resource_id] = {
                    "value": None,
                    "highest_issued_token": token,
                    "last_write_token": 0,
                }
            else:
                self._storage[resource_id]["highest_issued_token"] = max(
                    self._storage[resource_id]["highest_issued_token"], token
                )

            return {
                "granted": True,
                "fencing_token": token,
                "expires_at_ms": expires_at,
            }

    def renew(self, resource_id: str, worker_id: str, fencing_token: int, ttl_ms: int, now_ms: int) -> bool:
        with self._lock:
            lease = self._leases.get(resource_id)
            if not lease:
                return False
            if lease["worker_id"] == worker_id and lease["fencing_token"] == fencing_token:
                if now_ms < lease["expires_at_ms"] and ttl_ms > 0:
                    lease["expires_at_ms"] = now_ms + ttl_ms
                    return True
            return False

    def release(self, resource_id: str, worker_id: str, fencing_token: int) -> bool:
        with self._lock:
            lease = self._leases.get(resource_id)
            if not lease:
                return False
            if lease["worker_id"] == worker_id and lease["fencing_token"] == fencing_token:
                del self._leases[resource_id]
                return True
            return False

    def write_resource(self, resource_id: str, fencing_token: int, value: str, now_ms: int) -> dict:
        if not isinstance(resource_id, str) or not resource_id:
            return {"success": False, "reason": "INVALID_ARGUMENT"}
        if not isinstance(value, str):
            return {"success": False, "reason": "INVALID_ARGUMENT"}
        if type(now_ms) is not int or isinstance(now_ms, bool):
            return {"success": False, "reason": "INVALID_ARGUMENT"}
        if type(fencing_token) is not int or isinstance(fencing_token, bool) or fencing_token <= 0:
            return {"success": False, "reason": "STALE_TOKEN"}

        with self._lock:
            store = self._storage.get(resource_id)
            if not store:
                return {"success": False, "reason": "STALE_TOKEN"}

            threshold = max(store["highest_issued_token"], store["last_write_token"])
            if fencing_token < threshold:
                return {"success": False, "reason": "STALE_TOKEN"}

            store["value"] = value
            store["last_write_token"] = fencing_token
            return {"success": True, "reason": "OK"}

    def read_resource(self, resource_id: str) -> dict:
        with self._lock:
            store = self._storage.get(resource_id)
            if not store:
                return {"value": None, "last_token": 0}
            return {"value": store["value"], "last_token": store["last_write_token"]}
