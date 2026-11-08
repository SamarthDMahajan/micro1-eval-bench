import asyncio
import time
from typing import Tuple
from .base_grader import BaseGrader

class RetryableError(Exception):
    retryable = True

class FatalError(Exception):
    retryable = False

class GraderTask03(BaseGrader):
    task_id = "task_03"

    def test_happy_path(self, target_module) -> Tuple[bool, str]:
        fn = getattr(target_module, "process_batches", None)
        if not fn:
            return False, "process_batches function not found"

        items = [{"id": f"item_{i}", "payload": i * 10} for i in range(6)]
        processed = []

        async def worker(item):
            processed.append(item["id"])

        async def rollback(item):
            pass

        async def run():
            return await fn(
                items=items,
                sub_batch_size=2,
                rate_limit_per_sec=50,
                max_retries=2,
                worker_fn=worker,
                rollback_fn=rollback
            )

        res = asyncio.run(run())
        if res.get("committed_items") != [f"item_{i}" for i in range(6)]:
            return False, f"Unexpected committed_items: {res.get('committed_items')}"
        if res.get("rolled_back_items") != []:
            return False, f"Expected empty rolled_back_items, got {res.get('rolled_back_items')}"
        if res.get("sub_batch_statuses") != ["COMMITTED", "COMMITTED", "COMMITTED"]:
            return False, f"Expected 3 COMMITTED sub-batches, got {res.get('sub_batch_statuses')}"
        if res.get("total_attempts") != 6:
            return False, f"Expected 6 total attempts, got {res.get('total_attempts')}"

        return True, "All items committed successfully in happy path with correct batch statuses."

    def test_edge_case_1_boundary(self, target_module) -> Tuple[bool, str]:
        fn = getattr(target_module, "process_batches", None)
        if not fn:
            return False, "process_batches function not found"

        # 4 items in 2 sub-batches: [item_0, item_1], [item_2, item_3]
        # item_1 will raise FatalError (retryable = False)
        items = [{"id": f"item_{i}", "payload": i} for i in range(4)]
        rolled_back = []

        async def worker(item):
            if item["id"] == "item_1":
                raise FatalError("Fatal failure on item_1")

        async def rollback(item):
            rolled_back.append(item["id"])

        async def run():
            return await fn(
                items=items,
                sub_batch_size=2,
                rate_limit_per_sec=50,
                max_retries=2,
                worker_fn=worker,
                rollback_fn=rollback
            )

        res = asyncio.run(run())
        if res.get("sub_batch_statuses") != ["ROLLED_BACK", "COMMITTED"]:
            return False, f"Expected ['ROLLED_BACK', 'COMMITTED'], got {res.get('sub_batch_statuses')}"
        if rolled_back != ["item_0"]:
            return False, f"Expected item_0 to be rolled back, got {rolled_back}"
        if res.get("committed_items") != ["item_2", "item_3"]:
            return False, f"Expected item_2 and item_3 committed, got {res.get('committed_items')}"

        return True, "Fatal non-retryable error correctly rolled back sub-batch and allowed next sub-batch to proceed."

    def test_edge_case_2_recovery(self, target_module) -> Tuple[bool, str]:
        fn = getattr(target_module, "process_batches", None)
        if not fn:
            return False, "process_batches function not found"

        # Test retry recovery: item fails once with RetryableError, then succeeds
        items = [{"id": "item_retry_ok", "payload": 100}]
        fail_count = 0

        async def worker_with_retry(item):
            nonlocal fail_count
            if fail_count < 1:
                fail_count += 1
                raise RetryableError("Temporary error")

        async def rollback(item):
            pass

        async def run1():
            return await fn(
                items=items,
                sub_batch_size=1,
                rate_limit_per_sec=50,
                max_retries=2,
                worker_fn=worker_with_retry,
                rollback_fn=rollback
            )

        res1 = asyncio.run(run1())
        if res1.get("committed_items") != ["item_retry_ok"]:
            return False, "Item failed to commit after retryable recovery"
        if res1.get("total_attempts") != 2:
            return False, f"Expected 2 attempts for 1 retry, got {res1.get('total_attempts')}"

        # Test retry exhaustion: item fails continuously with RetryableError
        exhausted_rolled_back = []
        sub_items = [
            {"id": "first_ok", "payload": 1},
            {"id": "exhaust_fail", "payload": 2},
        ]

        async def worker_exhaust(item):
            if item["id"] == "exhaust_fail":
                raise RetryableError("Persistent error")

        async def rollback_exhaust(item):
            exhausted_rolled_back.append(item["id"])

        async def run2():
            return await fn(
                items=sub_items,
                sub_batch_size=2,
                rate_limit_per_sec=50,
                max_retries=2,
                worker_fn=worker_exhaust,
                rollback_fn=rollback_exhaust
            )

        res2 = asyncio.run(run2())
        if res2.get("sub_batch_statuses") != ["ROLLED_BACK"]:
            return False, f"Expected ROLLED_BACK on retry exhaustion, got {res2.get('sub_batch_statuses')}"
        if exhausted_rolled_back != ["first_ok"]:
            return False, f"Expected first_ok rolled back, got {exhausted_rolled_back}"
        # 1 attempt for first_ok + (1 initial + 2 retries = 3 attempts) for exhaust_fail = 4 attempts total
        if res2.get("total_attempts") != 4:
            return False, f"Expected 4 total attempts on max_retries=2 exhaustion, got {res2.get('total_attempts')}"

        return True, "Retryable errors retried successfully within budget and rolled back on exhaustion."

    def test_concurrency_race_safety(self, target_module) -> Tuple[bool, str]:
        fn = getattr(target_module, "process_batches", None)
        if not fn:
            return False, "process_batches function not found"

        # Verify strict LIFO rollback order for multiple items in sub-batch
        items = [{"id": f"item_{i}", "payload": i} for i in range(5)]
        rollback_order = []

        async def worker(item):
            if item["id"] == "item_3":
                raise FatalError("Crash on item 3")

        async def rollback(item):
            rollback_order.append(item["id"])

        async def run():
            return await fn(
                items=items,
                sub_batch_size=5,
                rate_limit_per_sec=50,
                max_retries=1,
                worker_fn=worker,
                rollback_fn=rollback
            )

        res = asyncio.run(run())
        expected_rollback = ["item_2", "item_1", "item_0"]
        if rollback_order != expected_rollback:
            return False, f"Strict LIFO rollback violated: expected {expected_rollback}, got {rollback_order}"
        if res.get("rolled_back_items") != expected_rollback:
            return False, f"returned rolled_back_items mismatch: {res.get('rolled_back_items')}"

        return True, "Strict reverse-order (LIFO) atomic rollback order verified."

    def test_invariant_stress_test(self, target_module) -> Tuple[bool, str]:
        fn = getattr(target_module, "process_batches", None)
        if not fn:
            return False, "process_batches function not found"

        # Verify sliding window rate limiter: rate_limit_per_sec = 10, items = 14
        items = [{"id": f"rate_item_{i}", "payload": i} for i in range(14)]

        async def worker(item):
            pass

        async def rollback(item):
            pass

        async def run():
            t0 = time.monotonic()
            res = await fn(
                items=items,
                sub_batch_size=14,
                rate_limit_per_sec=10,
                max_retries=0,
                worker_fn=worker,
                rollback_fn=rollback
            )
            elapsed = time.monotonic() - t0
            return res, elapsed

        res, elapsed = asyncio.run(run())
        if res.get("total_attempts") != 14:
            return False, f"Expected 14 total attempts, got {res.get('total_attempts')}"

        # 14 requests with limit 10/sec must take at least 0.95 seconds
        if elapsed < 0.95:
            return False, f"Rate limiter did not throttle: 14 requests at 10/s took only {elapsed:.3f}s"

        return True, f"Sliding window rate limit strictly enforced (elapsed {elapsed:.3f}s >= 0.95s)."
