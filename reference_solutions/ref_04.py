import threading
from typing import Any, Dict, List, Callable, Optional

class TransactionalOutbox:
    def __init__(self, max_attempts: int = 3):
        self.max_attempts = max_attempts
        self._lock = threading.Lock()
        self._state: Dict[str, Any] = {}
        self._pending: List[Dict[str, Any]] = []
        self._dlq: List[Dict[str, Any]] = []
        self._next_event_id = 1

    def execute_in_transaction(
        self,
        state_key: str,
        state_value: Any,
        aggregate_id: str,
        event_payload: dict,
        fail_after_state_write: bool = False
    ) -> bool:
        with self._lock:
            if fail_after_state_write:
                return False
            self._state[state_key] = state_value
            event = {
                "event_id": self._next_event_id,
                "aggregate_id": aggregate_id,
                "payload": dict(event_payload),
                "attempts": 0,
            }
            self._next_event_id += 1
            self._pending.append(event)
            return True

    def relay_pending(self, publish_fn: Callable, batch_limit: int = 50) -> Dict[str, int]:
        with self._lock:
            published = 0
            failed_retryable = 0
            dead_lettered = 0

            blocked_aggregates = set()
            new_pending: List[Dict[str, Any]] = []
            processed_count = 0

            for event in self._pending:
                agg_id = event["aggregate_id"]

                if processed_count >= batch_limit:
                    new_pending.append(event)
                    continue

                if agg_id in blocked_aggregates:
                    new_pending.append(event)
                    continue

                processed_count += 1
                try:
                    publish_fn(dict(event))
                    published += 1
                except Exception:
                    event["attempts"] += 1
                    if event["attempts"] < self.max_attempts:
                        failed_retryable += 1
                        blocked_aggregates.add(agg_id)
                        new_pending.append(event)
                    else:
                        dead_lettered += 1
                        self._dlq.append(dict(event))
                        # Poison pill isolated to DLQ; does not block subsequent events

            self._pending = new_pending
            return {
                "published": published,
                "failed_retryable": failed_retryable,
                "dead_lettered": dead_lettered,
            }

    def get_state(self, state_key: str) -> Any:
        with self._lock:
            return self._state.get(state_key, None)

    def get_dlq(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(e) for e in self._dlq]
