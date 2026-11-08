import threading
import random
from typing import Tuple
from .base_grader import BaseGrader

class GraderTask01(BaseGrader):
    task_id = "task_01"

    def test_happy_path(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "WebhookDeduplicator", None)
        if not cls:
            return False, "WebhookDeduplicator class not found"

        dedup = cls()
        e1 = {
            "idempotency_key": "k1",
            "entity_id": "ent1",
            "event_ts": 1000,
            "version": 1,
            "payload": {"name": "item1", "count": 10},
        }
        r1 = dedup.process_event(e1)
        if r1.get("status") != "APPLIED":
            return False, f"Expected APPLIED, got {r1.get('status')}"
        if r1.get("current_state") != {"name": "item1", "count": 10}:
            return False, f"Unexpected state: {r1.get('current_state')}"
        if r1.get("applied_version") != 1:
            return False, f"Expected applied_version=1, got {r1.get('applied_version')}"

        e2 = {
            "idempotency_key": "k2",
            "entity_id": "ent1",
            "event_ts": 2000,
            "version": 2,
            "payload": {"name": "item1", "count": 20},
        }
        r2 = dedup.process_event(e2)
        if r2.get("status") != "APPLIED":
            return False, f"Expected APPLIED, got {r2.get('status')}"
        if r2.get("current_state") != {"name": "item1", "count": 20}:
            return False, f"Unexpected state: {r2.get('current_state')}"
        if r2.get("applied_version") != 2:
            return False, f"Expected applied_version=2, got {r2.get('applied_version')}"

        return True, "Sequential events applied cleanly with correct state and version tracking."

    def test_edge_case_1_boundary(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "WebhookDeduplicator", None)
        if not cls:
            return False, "WebhookDeduplicator class not found"

        dedup = cls()
        # Non-dict event
        r = dedup.process_event("not a dict")
        if r.get("status") != "INVALID" or r.get("current_state") is not None or r.get("applied_version") is not None:
            return False, "Failed to reject non-dict event with INVALID"

        # Missing idempotency_key
        r = dedup.process_event({"entity_id": "e", "event_ts": 1, "version": 1, "payload": {}})
        if r.get("status") != "INVALID":
            return False, "Failed to reject missing idempotency_key"

        # Empty idempotency_key
        r = dedup.process_event({"idempotency_key": "  ", "entity_id": "e", "event_ts": 1, "version": 1, "payload": {}})
        if r.get("status") != "INVALID":
            return False, "Failed to reject whitespace idempotency_key"

        # Boolean event_ts
        r = dedup.process_event({"idempotency_key": "k", "entity_id": "e", "event_ts": True, "version": 1, "payload": {}})
        if r.get("status") != "INVALID":
            return False, "Failed to reject boolean event_ts"

        # Boolean version
        r = dedup.process_event({"idempotency_key": "k", "entity_id": "e", "event_ts": 1, "version": False, "payload": {}})
        if r.get("status") != "INVALID":
            return False, "Failed to reject boolean version"

        # Non-dict payload
        r = dedup.process_event({"idempotency_key": "k", "entity_id": "e", "event_ts": 1, "version": 1, "payload": "not dict"})
        if r.get("status") != "INVALID":
            return False, "Failed to reject non-dict payload"

        # Duplicate check
        r_valid = dedup.process_event({"idempotency_key": "dup_key", "entity_id": "e_dup", "event_ts": 100, "version": 1, "payload": {"val": 1}})
        if r_valid.get("status") != "APPLIED":
            return False, "Initial event failed to apply"

        r_dup = dedup.process_event({"idempotency_key": "dup_key", "entity_id": "e_dup", "event_ts": 200, "version": 2, "payload": {"val": 2}})
        if r_dup.get("status") != "DUPLICATE":
            return False, f"Expected DUPLICATE, got {r_dup.get('status')}"
        if r_dup.get("current_state") != {"val": 1} or r_dup.get("applied_version") != 1:
            return False, "DUPLICATE must return previous current_state and applied_version without mutating"

        return True, "All validation edge cases and duplicate rejections verified."

    def test_edge_case_2_recovery(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "WebhookDeduplicator", None)
        if not cls:
            return False, "WebhookDeduplicator class not found"

        dedup = cls()
        # Seed entity with high timestamp
        r1 = dedup.process_event({"idempotency_key": "k_high", "entity_id": "e_order", "event_ts": 1000, "version": 5, "payload": {"val": "high"}})
        if r1.get("status") != "APPLIED":
            return False, "Setup event failed"

        # Stale event with lower timestamp
        r_stale = dedup.process_event({"idempotency_key": "k_stale", "entity_id": "e_order", "event_ts": 800, "version": 10, "payload": {"val": "stale"}})
        if r_stale.get("status") != "STALE_IGNORED":
            return False, f"Expected STALE_IGNORED for lower ts, got {r_stale.get('status')}"
        if r_stale.get("current_state") != {"val": "high"} or r_stale.get("applied_version") != 5:
            return False, "STALE_IGNORED must retain current state and applied version"

        # Same timestamp, lower version -> STALE_IGNORED
        r_tie_low = dedup.process_event({"idempotency_key": "k_tie_low", "entity_id": "e_order", "event_ts": 1000, "version": 4, "payload": {"val": "tie_low"}})
        if r_tie_low.get("status") != "STALE_IGNORED":
            return False, f"Expected STALE_IGNORED for tie with lower version, got {r_tie_low.get('status')}"

        # Same timestamp, higher version -> APPLIED
        r_tie_high = dedup.process_event({"idempotency_key": "k_tie_high", "entity_id": "e_order", "event_ts": 1000, "version": 6, "payload": {"val": "tie_high"}})
        if r_tie_high.get("status") != "APPLIED":
            return False, f"Expected APPLIED for tie with higher version, got {r_tie_high.get('status')}"
        if r_tie_high.get("current_state") != {"val": "tie_high"} or r_tie_high.get("applied_version") != 6:
            return False, "State not updated after higher version tie-break"

        # Resubmission of stale event key -> DUPLICATE (not STALE_IGNORED again)
        r_stale_resub = dedup.process_event({"idempotency_key": "k_stale", "entity_id": "e_order", "event_ts": 800, "version": 10, "payload": {"val": "stale"}})
        if r_stale_resub.get("status") != "DUPLICATE":
            return False, f"Resubmission of stale event key should be DUPLICATE, got {r_stale_resub.get('status')}"

        return True, "Causal out-of-order recovery and tie-breaking verified."

    def test_concurrency_race_safety(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "WebhookDeduplicator", None)
        if not cls:
            return False, "WebhookDeduplicator class not found"

        dedup = cls()
        num_threads = 30
        barrier = threading.Barrier(num_threads)
        results = [None] * num_threads

        def worker(thread_idx: int):
            try:
                barrier.wait(timeout=3.0)
                results[thread_idx] = dedup.process_event({
                    "idempotency_key": "race_key_all_same",
                    "entity_id": "ent_race",
                    "event_ts": 100,
                    "version": 1,
                    "payload": {"winner": thread_idx},
                })
            except Exception as e:
                results[thread_idx] = {"error": str(e)}

        threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3.0)

        applied_count = sum(1 for r in results if r and r.get("status") == "APPLIED")
        dup_count = sum(1 for r in results if r and r.get("status") == "DUPLICATE")

        if applied_count != 1:
            return False, f"Expected exactly 1 APPLIED in concurrency race, got {applied_count}"
        if dup_count != num_threads - 1:
            return False, f"Expected {num_threads - 1} DUPLICATE in concurrency race, got {dup_count}"

        return True, "30-thread race barrier executed safely: exactly 1 APPLIED, 29 DUPLICATE."

    def test_invariant_stress_test(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "WebhookDeduplicator", None)
        if not cls:
            return False, "WebhookDeduplicator class not found"

        dedup = cls()
        num_events = 100
        barrier = threading.Barrier(num_events)

        # Generate 100 random events across 3 entities
        events = []
        max_ts_ver = {}
        for i in range(num_events):
            eid = f"ent_{i % 3}"
            ts = random.randint(10, 500)
            ver = random.randint(1, 20)
            payload = {"event_num": i, "eid": eid, "ts": ts, "ver": ver}
            events.append({
                "idempotency_key": f"stress_key_{i}",
                "entity_id": eid,
                "event_ts": ts,
                "version": ver,
                "payload": payload,
            })
            if eid not in max_ts_ver or (ts, ver) > max_ts_ver[eid][0]:
                max_ts_ver[eid] = ((ts, ver), payload)

        results = [None] * num_events

        def stress_worker(idx: int):
            try:
                barrier.wait(timeout=3.0)
                results[idx] = dedup.process_event(events[idx])
            except Exception as e:
                results[idx] = {"error": str(e)}

        threads = [threading.Thread(target=stress_worker, args=(i,), daemon=True) for i in range(num_events)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3.0)

        # Check that all events returned valid statuses
        statuses = [r.get("status") for r in results if r]
        if len(statuses) != num_events:
            return False, f"Expected {num_events} responses, got {len(statuses)}"

        # Final state check for each entity
        for eid, (expected_pair, expected_payload) in max_ts_ver.items():
            # Query current state using a probe event with same max ts & ver (which should be STALE or DUP)
            probe = dedup.process_event({
                "idempotency_key": f"probe_{eid}",
                "entity_id": eid,
                "event_ts": 0,
                "version": 0,
                "payload": {},
            })
            if probe.get("applied_version") != expected_pair[1]:
                return False, f"Entity {eid} final applied version {probe.get('applied_version')} != expected {expected_pair[1]}"
            if probe.get("current_state") != expected_payload:
                return False, f"Entity {eid} final payload mismatch. Got {probe.get('current_state')}, expected {expected_payload}"

        return True, "100-event multi-entity stress test invariant held: final state matches highest (ts, ver) payload."
