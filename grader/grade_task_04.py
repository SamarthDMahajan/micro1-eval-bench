import threading
import random
from typing import Tuple
from .base_grader import BaseGrader

class GraderTask04(BaseGrader):
    task_id = "task_04"

    def test_happy_path(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "TransactionalOutbox", None)
        if not cls:
            return False, "TransactionalOutbox class not found"

        outbox = cls(max_attempts=3)
        # Execute transactions
        t1 = outbox.execute_in_transaction("k1", "val1", "agg1", {"msg": "m1"})
        t2 = outbox.execute_in_transaction("k2", "val2", "agg1", {"msg": "m2"})
        t3 = outbox.execute_in_transaction("k3", "val3", "agg2", {"msg": "m3"})

        if not (t1 and t2 and t3):
            return False, "execute_in_transaction returned False in happy path"

        if outbox.get_state("k1") != "val1" or outbox.get_state("k2") != "val2":
            return False, "Application state not updated properly"

        published = []
        res = outbox.relay_pending(lambda ev: published.append(ev))
        if res.get("published") != 3 or res.get("failed_retryable") != 0 or res.get("dead_lettered") != 0:
            return False, f"Unexpected relay_pending result: {res}"
        if len(published) != 3:
            return False, f"Expected 3 published events, got {len(published)}"

        return True, "Transactions and outbox relay executed successfully in happy path."

    def test_edge_case_1_boundary(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "TransactionalOutbox", None)
        if not cls:
            return False, "TransactionalOutbox class not found"

        outbox = cls(max_attempts=3)
        # Atomicity failure test: fail_after_state_write=True
        aborted = outbox.execute_in_transaction("aborted_key", "aborted_val", "agg_x", {"foo": "bar"}, fail_after_state_write=True)
        if aborted:
            return False, "execute_in_transaction with fail_after_state_write=True should return False"

        # State must not be set
        if outbox.get_state("aborted_key") is not None:
            return False, "State was persisted despite transaction rollback"

        # Outbox event must not be queued
        res = outbox.relay_pending(lambda ev: None)
        if res.get("published") != 0:
            return False, "Event was queued and published despite transaction rollback"

        # Batch limit testing
        for i in range(5):
            outbox.execute_in_transaction(f"k_{i}", f"v_{i}", f"agg_{i}", {"idx": i})

        batch1 = outbox.relay_pending(lambda ev: None, batch_limit=2)
        if batch1.get("published") != 2:
            return False, f"Expected exactly 2 published with batch_limit=2, got {batch1.get('published')}"

        batch2 = outbox.relay_pending(lambda ev: None, batch_limit=2)
        if batch2.get("published") != 2:
            return False, f"Expected exactly 2 published in second batch, got {batch2.get('published')}"

        batch3 = outbox.relay_pending(lambda ev: None, batch_limit=2)
        if batch3.get("published") != 1:
            return False, f"Expected 1 remaining published in third batch, got {batch3.get('published')}"

        return True, "Transaction rollback atomicity and batch limits strictly verified."

    def test_edge_case_2_recovery(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "TransactionalOutbox", None)
        if not cls:
            return False, "TransactionalOutbox class not found"

        outbox = cls(max_attempts=3)
        # Agg A has 2 events, Agg B has 1 event
        outbox.execute_in_transaction("sA1", 1, "agg_A", {"seq": 1})
        outbox.execute_in_transaction("sA2", 2, "agg_A", {"seq": 2})
        outbox.execute_in_transaction("sB1", 1, "agg_B", {"seq": 1})

        # Relay where agg_A events fail
        def failing_publisher(ev):
            if ev["aggregate_id"] == "agg_A":
                raise RuntimeError("Broker connection timeout")

        # Cycle 1: agg_A event 1 fails (attempts becomes 1), agg_A event 2 is blocked, agg_B succeeds!
        r1 = outbox.relay_pending(failing_publisher)
        if r1.get("published") != 1 or r1.get("failed_retryable") != 1 or r1.get("dead_lettered") != 0:
            return False, f"Cycle 1 mismatch: expected published=1, failed_retryable=1; got {r1}"

        # Cycle 2: agg_A event 1 fails again (attempts becomes 2)
        r2 = outbox.relay_pending(failing_publisher)
        if r2.get("published") != 0 or r2.get("failed_retryable") != 1 or r2.get("dead_lettered") != 0:
            return False, f"Cycle 2 mismatch: got {r2}"

        # Cycle 3: agg_A event 1 fails 3rd time -> reaches max_attempts (3) -> DEAD LETTERED!
        r3 = outbox.relay_pending(failing_publisher)
        if r3.get("dead_lettered") != 1:
            return False, f"Cycle 3 mismatch: expected dead_lettered=1, got {r3}"

        dlq = outbox.get_dlq()
        if len(dlq) != 1 or dlq[0]["aggregate_id"] != "agg_A" or dlq[0]["payload"]["seq"] != 1:
            return False, f"DLQ contents unexpected: {dlq}"

        # Cycle 4: Now that poison pill is isolated in DLQ, agg_A event 2 is unblocked!
        # Let's publish with success
        pub_unblocked = []
        r4 = outbox.relay_pending(lambda ev: pub_unblocked.append(ev))
        if r4.get("published") != 1 or len(pub_unblocked) != 1 or pub_unblocked[0]["payload"]["seq"] != 2:
            return False, f"Unblocked event failed to publish: r4={r4}, published={pub_unblocked}"

        return True, "Non-blocking cross-aggregate isolation and poison-pill DLQ unblocking verified."

    def test_concurrency_race_safety(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "TransactionalOutbox", None)
        if not cls:
            return False, "TransactionalOutbox class not found"

        outbox = cls(max_attempts=3)
        # Pre-seed 30 events across 3 aggregates
        for i in range(30):
            outbox.execute_in_transaction(f"k_{i}", i, f"agg_{i % 3}", {"idx": i})

        published_events = []
        pub_lock = threading.Lock()

        def safe_publish(ev):
            with pub_lock:
                published_events.append(ev)

        num_threads = 10
        barrier = threading.Barrier(num_threads)

        def worker():
            try:
                barrier.wait(timeout=3.0)
                outbox.relay_pending(safe_publish, batch_limit=10)
            except Exception:
                pass

        threads = [threading.Thread(target=worker, daemon=True) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3.0)

        # In case some remained due to batch limits, flush
        outbox.relay_pending(safe_publish, batch_limit=50)

        # Invariant 1: Exactly 30 events published
        if len(published_events) != 30:
            return False, f"Expected 30 total published events, got {len(published_events)}"

        # Invariant 2: No duplicates
        event_ids = [e["event_id"] for e in published_events]
        if len(set(event_ids)) != 30:
            return False, f"Duplicate events published under concurrency: {len(set(event_ids))} distinct out of 30"

        # Invariant 3: Per-aggregate ordering
        by_agg = {f"agg_{a}": [] for a in range(3)}
        for e in published_events:
            by_agg[e["aggregate_id"]].append(e["payload"]["idx"])

        for agg, idxs in by_agg.items():
            if idxs != sorted(idxs):
                return False, f"Per-aggregate ordering violated for {agg}: {idxs}"

        return True, "10 concurrent worker threads relayed 30 events with zero duplicates and strict per-aggregate FIFO order."

    def test_invariant_stress_test(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "TransactionalOutbox", None)
        if not cls:
            return False, "TransactionalOutbox class not found"

        outbox = cls(max_attempts=3)
        num_tx = 60
        for i in range(num_tx):
            outbox.execute_in_transaction(f"stress_{i}", i, f"agg_{i % 4}", {"i": i})

        published_count = 0
        dlq_count = 0

        # Simulate flaky publisher (20% random failures)
        def flaky_publisher(ev):
            if random.random() < 0.25:
                raise RuntimeError("Network glitch")

        # Run multiple relay cycles until pending is drained
        for _ in range(15):
            res = outbox.relay_pending(flaky_publisher, batch_limit=20)
            published_count += res["published"]

        # Final drain with reliable publisher
        final_res = outbox.relay_pending(lambda ev: None, batch_limit=100)
        published_count += final_res["published"]

        dlq = outbox.get_dlq()
        total_accounted = published_count + len(dlq)

        if total_accounted != num_tx:
            return False, f"Invariant violated: published ({published_count}) + DLQ ({len(dlq)}) != total transactions ({num_tx})"

        return True, f"Stress invariant held: {published_count} published + {len(dlq)} in DLQ = {num_tx} total transactions."
