import threading
from typing import Tuple
from .base_grader import BaseGrader

class GraderTask02(BaseGrader):
    task_id = "task_02"

    def test_happy_path(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "LeaseManager", None)
        if not cls:
            return False, "LeaseManager class not found"

        lm = cls()
        # Acquire
        r1 = lm.acquire("res_1", "w1", 1000, 100)
        if not r1.get("granted") or r1.get("fencing_token") != 1 or r1.get("expires_at_ms") != 1100:
            return False, f"Acquire failed or unexpected response: {r1}"

        # Write
        w1 = lm.write_resource("res_1", 1, "hello", 200)
        if not w1.get("success") or w1.get("reason") != "OK":
            return False, f"Write failed: {w1}"

        # Read
        rd1 = lm.read_resource("res_1")
        if rd1.get("value") != "hello" or rd1.get("last_token") != 1:
            return False, f"Read returned unexpected data: {rd1}"

        # Renew
        ren = lm.renew("res_1", "w1", 1, 1000, 500)
        if not ren:
            return False, "Renew should have succeeded"

        # Release
        rel = lm.release("res_1", "w1", 1)
        if not rel:
            return False, "Release should have succeeded"

        return True, "Acquire, write, read, renew, and release succeeded in happy path."

    def test_edge_case_1_boundary(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "LeaseManager", None)
        if not cls:
            return False, "LeaseManager class not found"

        lm = cls()
        # Acquire lease active until 1000
        a1 = lm.acquire("res_boundary", "w1", 1000, 0)
        tok1 = a1.get("fencing_token")

        # While active (now_ms=500 < 1000), w2 cannot acquire
        a2 = lm.acquire("res_boundary", "w2", 1000, 500)
        if a2.get("granted"):
            return False, "Acquire succeeded while active lease held by w1"
        if a2.get("expires_at_ms") != 1000:
            return False, f"Expected current expires_at_ms=1000, got {a2.get('expires_at_ms')}"

        # Exact boundary: now_ms == expires_at_ms (1000) -> lease expired, w2 can acquire
        a3 = lm.acquire("res_boundary", "w2", 500, 1000)
        if not a3.get("granted"):
            return False, "Acquire failed at exact expiration boundary (now_ms == expires_at_ms)"
        tok2 = a3.get("fencing_token")
        if tok2 <= tok1:
            return False, f"New fencing token {tok2} must be strictly greater than {tok1}"

        # Renew after expiration returns False
        if lm.renew("res_boundary", "w2", tok2, 500, 1600):
            return False, "Renew succeeded after lease expiration"

        # Renew with wrong worker_id or wrong token returns False
        if lm.renew("res_boundary", "wrong_w", tok2, 500, 1200):
            return False, "Renew succeeded with wrong worker_id"
        if lm.renew("res_boundary", "w2", 9999, 500, 1200):
            return False, "Renew succeeded with invalid token"

        # Invalid arguments to write_resource
        inv1 = lm.write_resource("", tok2, "val", 1200)
        if inv1.get("reason") != "INVALID_ARGUMENT":
            return False, f"Expected INVALID_ARGUMENT for empty resource_id, got {inv1}"
        inv2 = lm.write_resource("res_boundary", tok2, 12345, 1200)
        if inv2.get("reason") != "INVALID_ARGUMENT":
            return False, f"Expected INVALID_ARGUMENT for non-string value, got {inv2}"

        # Non-positive fencing token
        neg_tok = lm.write_resource("res_boundary", 0, "val", 1200)
        if neg_tok.get("reason") != "STALE_TOKEN":
            return False, f"Expected STALE_TOKEN for 0 token, got {neg_tok}"

        return True, "Boundary conditions, argument validation, and expiration semantics verified."

    def test_edge_case_2_recovery(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "LeaseManager", None)
        if not cls:
            return False, "LeaseManager class not found"

        lm = cls()
        # Worker 1 acquires res_zombie
        a1 = lm.acquire("res_zombie", "w1", 500, 100)
        t1 = a1.get("fencing_token")

        # Worker 1 paused. Time advances past expiry. Worker 2 acquires
        a2 = lm.acquire("res_zombie", "w2", 500, 700)
        t2 = a2.get("fencing_token")
        if t2 <= t1:
            return False, "Token did not advance on second acquisition"

        # Worker 1 wakes up and tries to write with old token t1 -> MUST BE REJECTED
        zombie_write = lm.write_resource("res_zombie", t1, "zombie_data", 750)
        if zombie_write.get("success") or zombie_write.get("reason") != "STALE_TOKEN":
            return False, f"Zombie write was not rejected with STALE_TOKEN: {zombie_write}"

        # Worker 2 writes with current token t2 -> MUST SUCCEED
        legit_write = lm.write_resource("res_zombie", t2, "legit_data", 760)
        if not legit_write.get("success") or legit_write.get("reason") != "OK":
            return False, f"Valid write rejected: {legit_write}"

        # Worker 1 tries again with t1 -> STILL REJECTED
        zombie_retry = lm.write_resource("res_zombie", t1, "zombie_data_2", 780)
        if zombie_retry.get("success") or zombie_retry.get("reason") != "STALE_TOKEN":
            return False, "Subsequent zombie write was not rejected"

        # Write to unissued resource -> STALE_TOKEN
        unissued = lm.write_resource("unissued_res", 5, "data", 100)
        if unissued.get("reason") != "STALE_TOKEN":
            return False, f"Expected STALE_TOKEN for unissued resource write, got {unissued}"

        return True, "Zombie writes from pre-empted tokens permanently fenced off and rejected."

    def test_concurrency_race_safety(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "LeaseManager", None)
        if not cls:
            return False, "LeaseManager class not found"

        lm = cls()
        # Seed lease expired at exactly now_ms = 1000
        a0 = lm.acquire("res_race", "w0", 1000, 0)
        t0 = a0.get("fencing_token")

        # 20 threads race at now_ms = 1000
        num_threads = 20
        barrier = threading.Barrier(num_threads)
        results = [None] * num_threads

        def race_worker(idx: int):
            try:
                barrier.wait(timeout=3.0)
                results[idx] = lm.acquire("res_race", f"w_race_{idx}", 500, 1000)
            except Exception as e:
                results[idx] = {"error": str(e)}

        threads = [threading.Thread(target=race_worker, args=(i,), daemon=True) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=3.0)

        granted = [r for r in results if r and r.get("granted")]
        denied = [r for r in results if r and not r.get("granted")]

        if len(granted) != 1:
            return False, f"Expected exactly 1 granted lease in 20-thread race, got {len(granted)}"
        if len(denied) != num_threads - 1:
            return False, f"Expected {num_threads - 1} denied in race, got {len(denied)}"

        winner_token = granted[0].get("fencing_token")
        if winner_token <= t0:
            return False, f"Winner token {winner_token} not strictly greater than previous {t0}"

        return True, "20-thread barrier race at expiry boundary produced exactly 1 winner with monotonic token."

    def test_invariant_stress_test(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "LeaseManager", None)
        if not cls:
            return False, "LeaseManager class not found"

        lm = cls()
        tokens = []
        now = 0

        # Run 50 sequential acquisitions with simulated expiry
        for i in range(50):
            res = lm.acquire(f"res_stress_{i % 5}", f"worker_{i}", 100, now)
            if res.get("granted"):
                tokens.append(res.get("fencing_token"))
            now += 150  # lease expired

        # Invariant 1: Strictly monotonic tokens
        for i in range(len(tokens) - 1):
            if tokens[i] >= tokens[i + 1]:
                return False, f"Monotonicity invariant violated: token[{i}]={tokens[i]} >= token[{i+1}]={tokens[i+1]}"

        # Invariant 2: Stale token writes rejected across all 5 resources
        for i in range(5):
            rid = f"res_stress_{i}"
            rd = lm.read_resource(rid)
            last_tok = rd.get("last_token", 0)
            # Try to write with an older token
            old_tok = 1
            w = lm.write_resource(rid, old_tok, "should_fail", now)
            # If highest issued token for this resource > 1, old_tok 1 must be rejected
            if w.get("success"):
                return False, f"Storage accepted write with old token 1 on {rid}"

        return True, "Fencing tokens strictly monotonic and storage guard invariants held across all rounds."
