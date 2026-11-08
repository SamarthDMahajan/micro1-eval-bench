import threading
from typing import Tuple
from .base_grader import BaseGrader

class GraderTask05(BaseGrader):
    task_id = "task_05"

    def test_happy_path(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "SlidingWindowRateLimiter", None)
        if not cls:
            return False, "SlidingWindowRateLimiter class not found"

        rl = cls(limit=5, window_ms=1000, max_burst_borrow=2, max_clock_skew_ms=100)

        for i in range(5):
            res = rl.allow_request(client_id="c1", node_ts_ms=100 + i * 50, server_recv_ts_ms=100 + i * 50)
            if not res.get("allowed"):
                return False, f"Request {i} unexpectedly rejected"
            if res.get("reason") != "OK":
                return False, f"Expected reason OK, got {res.get('reason')}"
            if res.get("borrowed_used") != 0:
                return False, f"Expected borrowed_used=0, got {res.get('borrowed_used')}"
            expected_remaining = 5 - (i + 1)
            if res.get("remaining_in_window") != expected_remaining:
                return False, f"Expected remaining {expected_remaining}, got {res.get('remaining_in_window')}"

        return True, "Standard capacity successfully allocated under sliding window."

    def test_edge_case_1_boundary(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "SlidingWindowRateLimiter", None)
        if not cls:
            return False, "SlidingWindowRateLimiter class not found"

        rl = cls(limit=10, window_ms=1000, max_burst_borrow=2, max_clock_skew_ms=50)

        # Clock skew exceeded: node_ts=500, server_ts=700 (diff=200 > 50)
        r1 = rl.allow_request("c_skew", node_ts_ms=500, server_recv_ts_ms=700)
        if not r1.get("allowed"):
            return False, "Request was rejected instead of skew-clamped"
        if r1.get("effective_ts_ms") != 700:
            return False, f"Expected effective_ts_ms=700, got {r1.get('effective_ts_ms')}"
        if r1.get("reason") != "CLOCK_SKEW_CLAMPED":
            return False, f"Expected reason CLOCK_SKEW_CLAMPED, got {r1.get('reason')}"

        # Monotonic time check: client clock goes backward to 600
        r2 = rl.allow_request("c_skew", node_ts_ms=600, server_recv_ts_ms=600)
        if not r2.get("allowed"):
            return False, "Request was rejected on retrograde timestamp"
        # Since last effective ts was 700, monotonic ts must remain 700
        if r2.get("effective_ts_ms") != 700:
            return False, f"Monotonic time failed: effective_ts_ms={r2.get('effective_ts_ms')} != 700"
        if r2.get("reason") != "CLOCK_SKEW_CLAMPED":
            return False, f"Backward jump should result in CLOCK_SKEW_CLAMPED, got {r2.get('reason')}"

        return True, "Clock skew clamping and per-client monotonic timestamp enforcement verified."

    def test_edge_case_2_recovery(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "SlidingWindowRateLimiter", None)
        if not cls:
            return False, "SlidingWindowRateLimiter class not found"

        # limit = 2, burst_borrow = 2, window = 1000
        rl = cls(limit=2, window_ms=1000, max_burst_borrow=2, max_clock_skew_ms=100)

        # Window 1: Use base capacity (2)
        r1 = rl.allow_request("c_borrow", node_ts_ms=100, server_recv_ts_ms=100)
        r2 = rl.allow_request("c_borrow", node_ts_ms=200, server_recv_ts_ms=200)
        if not (r1["allowed"] and r2["allowed"] and r1["reason"] == "OK" and r2["reason"] == "OK"):
            return False, "Initial base requests failed"

        # Window 1: Burst borrow (up to 2)
        b1 = rl.allow_request("c_borrow", node_ts_ms=300, server_recv_ts_ms=300)
        if not b1["allowed"] or b1["reason"] != "BORROWED" or b1["borrowed_used"] != 1:
            return False, f"First burst borrow failed: {b1}"

        b2 = rl.allow_request("c_borrow", node_ts_ms=400, server_recv_ts_ms=400)
        if not b2["allowed"] or b2["reason"] != "BORROWED" or b2["borrowed_used"] != 2:
            return False, f"Second burst borrow failed: {b2}"

        # Window 1: Now fully exhausted -> RATE_LIMITED
        b3 = rl.allow_request("c_borrow", node_ts_ms=500, server_recv_ts_ms=500)
        if b3["allowed"] or b3["reason"] != "RATE_LIMITED":
            return False, f"Expected RATE_LIMITED when capacity + burst full, got {b3}"

        # Window 2: Advance time to 1500 (in immediately subsequent window, so 300 and 400 are in prior window)
        # Prior window had 2 borrowed tokens. effective_limit = max(0, 2 - 2) = 0!
        # Thus, base request in window 2 should immediately have 0 base capacity!
        # It must borrow or be limited. Since new window has 0 active borrowed, it can borrow 1
        w2_r1 = rl.allow_request("c_borrow", node_ts_ms=1500, server_recv_ts_ms=1500)
        if not w2_r1["allowed"] or w2_r1["reason"] != "BORROWED":
            return False, f"Window 2 repayment penalty failed: expected BORROWED due to 0 effective base limit, got {w2_r1}"

        return True, "Burst borrowing and subsequent window repayment penalty verified."

    def test_concurrency_race_safety(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "SlidingWindowRateLimiter", None)
        if not cls:
            return False, "SlidingWindowRateLimiter class not found"

        limit = 10
        burst = 5
        # Total allowed = 15
        rl = cls(limit=limit, window_ms=2000, max_burst_borrow=burst, max_clock_skew_ms=500)

        num_threads = 40
        barrier = threading.Barrier(num_threads)
        results = [None] * num_threads

        def worker(idx: int):
            try:
                barrier.wait(timeout=3.0)
                results[idx] = rl.allow_request("c_concurrent", node_ts_ms=500, server_recv_ts_ms=500)
            except Exception as e:
                results[idx] = {"error": str(e)}

        threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3.0)

        allowed_count = sum(1 for r in results if r and r.get("allowed"))
        rejected_count = sum(1 for r in results if r and not r.get("allowed"))

        if allowed_count != limit + burst:
            return False, f"Concurrency race allowed {allowed_count} requests, expected exactly {limit + burst}"
        if rejected_count != num_threads - (limit + burst):
            return False, f"Concurrency race rejected {rejected_count} requests, expected {num_threads - (limit + burst)}"

        base_reasons = sum(1 for r in results if r and r.get("reason") in ("OK", "CLOCK_SKEW_CLAMPED") and r.get("allowed"))
        borrow_reasons = sum(1 for r in results if r and r.get("reason") == "BORROWED")

        if base_reasons != limit:
            return False, f"Expected {limit} base admitted requests, got {base_reasons}"
        if borrow_reasons != burst:
            return False, f"Expected {burst} borrowed admitted requests, got {borrow_reasons}"

        return True, "40-thread concurrency race strictly respected limit + max_burst_borrow with zero over-admission."

    def test_invariant_stress_test(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "SlidingWindowRateLimiter", None)
        if not cls:
            return False, "SlidingWindowRateLimiter class not found"

        rl = cls(limit=20, window_ms=1000, max_burst_borrow=5, max_clock_skew_ms=100)

        # 5 distinct clients submitting 30 requests each across simulated time
        for client_idx in range(5):
            cid = f"client_{client_idx}"
            now = 0
            for req in range(30):
                res = rl.allow_request(cid, node_ts_ms=now, server_recv_ts_ms=now)
                # In any single point in time, allowed <= 25
                if req < 20:
                    if not res["allowed"] or res["reason"] != "OK":
                        return False, f"Client {cid} request {req} should have been OK"
                elif req < 25:
                    if not res["allowed"] or res["reason"] != "BORROWED":
                        return False, f"Client {cid} request {req} should have BORROWED"
                else:
                    if res["allowed"] or res["reason"] != "RATE_LIMITED":
                        return False, f"Client {cid} request {req} should be RATE_LIMITED"
                now += 10  # within same 1000ms window

        return True, "Multi-client sliding window capacity invariant held across all client streams."
