import threading
from typing import Tuple
from .base_grader import BaseGrader

class GraderTask06(BaseGrader):
    task_id = "task_06"

    def test_happy_path(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "DynamicDAGScheduler", None)
        if not cls:
            return False, "DynamicDAGScheduler class not found"

        sched = cls()
        r1 = sched.add_task("task_A", [])
        r2 = sched.add_task("task_B", ["task_A"])
        r3 = sched.add_task("task_C", ["task_B"])

        if not (r1["added"] and r2["added"] and r3["added"]):
            return False, "add_task failed in happy path"

        # Initially, only task_A is ready
        c1 = sched.claim_ready_tasks()
        if c1 != ["task_A"]:
            return False, f"Expected ['task_A'] claimed, got {c1}"
        if sched.get_status("task_A") != "RUNNING":
            return False, f"Expected task_A to be RUNNING, got {sched.get_status('task_A')}"

        # Complete task_A
        if not sched.complete_task("task_A"):
            return False, "Failed to complete task_A"
        if sched.get_status("task_A") != "COMPLETED":
            return False, f"Expected task_A COMPLETED, got {sched.get_status('task_A')}"

        # Now task_B is ready
        c2 = sched.claim_ready_tasks()
        if c2 != ["task_B"]:
            return False, f"Expected ['task_B'] claimed, got {c2}"
        sched.complete_task("task_B")

        # Now task_C is ready
        c3 = sched.claim_ready_tasks()
        if c3 != ["task_C"]:
            return False, f"Expected ['task_C'] claimed, got {c3}"
        sched.complete_task("task_C")

        if sched.get_status("task_C") != "COMPLETED":
            return False, "task_C not completed"

        return True, "Linear DAG scheduled, claimed, and completed sequentially in happy path."

    def test_edge_case_1_boundary(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "DynamicDAGScheduler", None)
        if not cls:
            return False, "DynamicDAGScheduler class not found"

        sched = cls()
        sched.add_task("t1", [])

        # Duplicate task
        dup = sched.add_task("t1", [])
        if dup.get("added") or dup.get("reason") != "DUPLICATE_TASK":
            return False, f"Expected DUPLICATE_TASK, got {dup}"

        # Unknown dependency in add_task
        unk = sched.add_task("t2", ["non_existent"])
        if unk.get("added") or unk.get("reason") != "UNKNOWN_DEP":
            return False, f"Expected UNKNOWN_DEP, got {unk}"

        # Self-cycle in add_task
        self_cyc = sched.add_task("t_self", ["t_self"])
        if self_cyc.get("added") or self_cyc.get("reason") != "CYCLE_DETECTED":
            return False, f"Expected CYCLE_DETECTED for self-cycle, got {self_cyc}"

        # add_dependency on unknown task
        unk_dep = sched.add_dependency("t1", "missing_task")
        if unk_dep.get("added") or unk_dep.get("reason") != "UNKNOWN_TASK":
            return False, f"Expected UNKNOWN_TASK, got {unk_dep}"

        # add_dependency on RUNNING task -> INVALID_STATE
        sched.claim_ready_tasks()  # t1 becomes RUNNING
        sched.add_task("t_other", [])
        inv_state = sched.add_dependency("t1", "t_other")
        if inv_state.get("added") or inv_state.get("reason") != "INVALID_STATE":
            return False, f"Expected INVALID_STATE when modifying RUNNING task, got {inv_state}"

        # Cycle detection in add_dependency
        # Setup: n1 -> n2 -> n3 (n1 depends on n2, n2 depends on n3)
        sched2 = cls()
        sched2.add_task("n3", [])
        sched2.add_task("n2", ["n3"])
        sched2.add_task("n1", ["n2"])
        # Attempt to make n3 depend on n1 (would form cycle n1 -> n2 -> n3 -> n1)
        cyc = sched2.add_dependency("n3", "n1")
        if cyc.get("added") or cyc.get("reason") != "CYCLE_DETECTED":
            return False, f"Expected CYCLE_DETECTED in add_dependency, got {cyc}"

        return True, "Boundary conditions: duplicate, unknown, invalid state, and runtime cycle detection verified."

    def test_edge_case_2_recovery(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "DynamicDAGScheduler", None)
        if not cls:
            return False, "DynamicDAGScheduler class not found"

        sched = cls()
        sched.add_task("root", [])
        sched.add_task("c1", ["root"])
        sched.add_task("c2", ["root"])
        sched.add_task("gc1", ["c1"])

        # Fail root
        cancelled = sched.fail_task("root")
        if sched.get_status("root") != "FAILED":
            return False, f"Expected root FAILED, got {sched.get_status('root')}"

        expected_cancelled = ["c1", "c2", "gc1"]
        if sorted(cancelled) != expected_cancelled:
            return False, f"Expected newly cancelled {expected_cancelled}, got {sorted(cancelled)}"

        for tid in expected_cancelled:
            if sched.get_status(tid) != "CANCELLED":
                return False, f"Expected {tid} status CANCELLED, got {sched.get_status(tid)}"

        # Adding a task depending on FAILED or CANCELLED task enters directly as CANCELLED
        direct_cancel = sched.add_task("late_task", ["c1"])
        if not direct_cancel.get("added") or direct_cancel.get("reason") != "OK":
            return False, f"add_task depending on cancelled task failed: {direct_cancel}"
        if sched.get_status("late_task") != "CANCELLED":
            return False, f"Expected late_task CANCELLED, got {sched.get_status('late_task')}"

        # Dynamic add_dependency to a failed task cancels the target task
        sched.add_task("pending_target", [])
        if sched.get_status("pending_target") != "PENDING":
            return False, "pending_target not pending"
        dyn_cancel = sched.add_dependency("pending_target", "root")
        if not dyn_cancel.get("added"):
            return False, "Dynamic dependency addition failed"
        if sched.get_status("pending_target") != "CANCELLED":
            return False, f"Expected pending_target to become CANCELLED, got {sched.get_status('pending_target')}"

        return True, "Cascading cancellation and transitive propagation verified."

    def test_concurrency_race_safety(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "DynamicDAGScheduler", None)
        if not cls:
            return False, "DynamicDAGScheduler class not found"

        sched = cls()
        num_tasks = 30
        for i in range(num_tasks):
            sched.add_task(f"t_{i:02d}", [])

        num_threads = 10
        barrier = threading.Barrier(num_threads)
        claimed_per_thread = [[] for _ in range(num_threads)]

        def worker(idx: int):
            barrier.wait()
            claimed = sched.claim_ready_tasks()
            claimed_per_thread[idx].extend(claimed)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        all_claimed = []
        for t_list in claimed_per_thread:
            all_claimed.extend(t_list)

        # Invariant 1: Exactly 30 tasks claimed in total
        if len(all_claimed) != num_tasks:
            return False, f"Expected {num_tasks} claimed tasks, got {len(all_claimed)}"

        # Invariant 2: No duplicates (disjoint sets)
        if len(set(all_claimed)) != num_tasks:
            return False, f"Double-claiming detected under concurrency: {len(set(all_claimed))} unique out of {num_tasks}"

        return True, "10 concurrent threads claimed 30 tasks with zero overlap or double-claiming."

    def test_invariant_stress_test(self, target_module) -> Tuple[bool, str]:
        cls = getattr(target_module, "DynamicDAGScheduler", None)
        if not cls:
            return False, "DynamicDAGScheduler class not found"

        sched = cls()
        # Build multi-layer diamond DAG:
        # Layer 0: [L0_0, L0_1]
        # Layer 1: [L1_0, L1_1, L1_2] each depending on all Layer 0
        # Layer 2: [L2_0, L2_1] each depending on all Layer 1
        sched.add_task("L0_0", [])
        sched.add_task("L0_1", [])

        sched.add_task("L1_0", ["L0_0", "L0_1"])
        sched.add_task("L1_1", ["L0_0", "L0_1"])
        sched.add_task("L1_2", ["L0_0", "L0_1"])

        sched.add_task("L2_0", ["L1_0", "L1_1", "L1_2"])
        sched.add_task("L2_1", ["L1_0", "L1_1", "L1_2"])

        # Claim L0
        l0_claimed = sched.claim_ready_tasks()
        if sorted(l0_claimed) != ["L0_0", "L0_1"]:
            return False, f"Expected L0 tasks claimed, got {l0_claimed}"

        # Complete L0_0, but fail L0_1!
        sched.complete_task("L0_0")
        cancelled = sched.fail_task("L0_1")

        # Layer 1 and Layer 2 must all be transitively cancelled
        expected_cancelled = sorted(["L1_0", "L1_1", "L1_2", "L2_0", "L2_1"])
        if sorted(cancelled) != expected_cancelled:
            return False, f"Cascading stress failure: expected {expected_cancelled}, got {sorted(cancelled)}"

        for task_id in expected_cancelled:
            if sched.get_status(task_id) != "CANCELLED":
                return False, f"Task {task_id} status is {sched.get_status(task_id)}, expected CANCELLED"

        # Ready tasks should now be empty
        remaining_ready = sched.claim_ready_tasks()
        if remaining_ready != []:
            return False, f"Expected no ready tasks after cascade cancellation, got {remaining_ready}"

        return True, "Multi-layer diamond DAG invariant strictly preserved under partial upstream failure."
