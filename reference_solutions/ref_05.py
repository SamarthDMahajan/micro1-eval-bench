import threading
import collections
from typing import Dict, List, Tuple, Any

class SlidingWindowRateLimiter:
    def __init__(self, limit: int, window_ms: int, max_burst_borrow: int, max_clock_skew_ms: int):
        self.limit = limit
        self.window_ms = window_ms
        self.max_burst_borrow = max_burst_borrow
        self.max_clock_skew_ms = max_clock_skew_ms

        self._lock = threading.Lock()
        self._client_records: Dict[str, List[Tuple[int, str]]] = collections.defaultdict(list)
        self._last_effective_ts: Dict[str, int] = collections.defaultdict(int)

    def allow_request(self, client_id: str, node_ts_ms: int, server_recv_ts_ms: int) -> Dict[str, Any]:
        with self._lock:
            skew_clamped = abs(node_ts_ms - server_recv_ts_ms) > self.max_clock_skew_ms
            candidate_ts = server_recv_ts_ms if skew_clamped else node_ts_ms
            last_eff = self._last_effective_ts[client_id]
            effective_ts_ms = max(candidate_ts, last_eff)
            self._last_effective_ts[client_id] = effective_ts_ms

            if effective_ts_ms != node_ts_ms:
                skew_clamped = True

            records = self._client_records[client_id]
            cutoff = effective_ts_ms - 2 * self.window_ms
            records[:] = [rec for rec in records if rec[0] > cutoff]

            active_boundary = effective_ts_ms - self.window_ms
            prior_boundary = effective_ts_ms - 2 * self.window_ms

            active_total = 0
            active_borrowed = 0
            prior_borrowed = 0

            for ts, tag in records:
                if active_boundary < ts <= effective_ts_ms:
                    active_total += 1
                    if tag == "BORROWED":
                        active_borrowed += 1
                elif prior_boundary < ts <= active_boundary:
                    if tag == "BORROWED":
                        prior_borrowed += 1

            effective_limit = max(0, self.limit - prior_borrowed)

            if active_total < effective_limit:
                records.append((effective_ts_ms, "BASE"))
                remaining = effective_limit - (active_total + 1)
                reason = "CLOCK_SKEW_CLAMPED" if skew_clamped else "OK"
                return {
                    "allowed": True,
                    "effective_ts_ms": effective_ts_ms,
                    "borrowed_used": active_borrowed,
                    "remaining_in_window": remaining,
                    "reason": reason,
                }
            elif active_borrowed < self.max_burst_borrow:
                records.append((effective_ts_ms, "BORROWED"))
                return {
                    "allowed": True,
                    "effective_ts_ms": effective_ts_ms,
                    "borrowed_used": active_borrowed + 1,
                    "remaining_in_window": 0,
                    "reason": "BORROWED",
                }
            else:
                return {
                    "allowed": False,
                    "effective_ts_ms": effective_ts_ms,
                    "borrowed_used": active_borrowed,
                    "remaining_in_window": 0,
                    "reason": "RATE_LIMITED",
                }
