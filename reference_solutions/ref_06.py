import threading
import collections
from typing import Dict, List, Set, Any

class DynamicDAGScheduler:
    def __init__(self):
        self._lock = threading.Lock()
        self._tasks: Dict[str, str] = {}  # task_id -> status
        self._dependencies: Dict[str, Set[str]] = collections.defaultdict(set)  # task -> deps it needs
        self._dependents: Dict[str, Set[str]] = collections.defaultdict(set)  # task -> tasks that depend on it

    def add_task(self, task_id: str, deps: List[str]) -> Dict[str, Any]:
        with self._lock:
            if task_id in self._tasks:
                return {"added": False, "reason": "DUPLICATE_TASK"}
            if task_id in deps:
                return {"added": False, "reason": "CYCLE_DETECTED"}
            for d in deps:
                if d not in self._tasks:
                    return {"added": False, "reason": "UNKNOWN_DEP"}

            has_failed_dep = any(self._tasks[d] in ("FAILED", "CANCELLED") for d in deps)
            initial_status = "CANCELLED" if has_failed_dep else "PENDING"
            self._tasks[task_id] = initial_status
            self._dependencies[task_id] = set(deps)
            for d in deps:
                self._dependents[d].add(task_id)

            return {"added": True, "reason": "OK"}

    def add_dependency(self, task_id: str, dep_id: str) -> Dict[str, Any]:
        with self._lock:
            if task_id not in self._tasks or dep_id not in self._tasks:
                return {"added": False, "reason": "UNKNOWN_TASK"}
            if self._tasks[task_id] != "PENDING":
                return {"added": False, "reason": "INVALID_STATE"}
            if task_id == dep_id:
                return {"added": False, "reason": "CYCLE_DETECTED"}

            # Cycle check: check if task_id is reachable from dep_id by following existing dependencies
            visited = set()
            queue = [dep_id]
            while queue:
                curr = queue.pop(0)
                if curr == task_id:
                    return {"added": False, "reason": "CYCLE_DETECTED"}
                if curr not in visited:
                    visited.add(curr)
                    queue.extend(self._dependencies[curr] - visited)

            self._dependencies[task_id].add(dep_id)
            self._dependents[dep_id].add(task_id)

            if self._tasks[dep_id] in ("FAILED", "CANCELLED"):
                self._cascade_cancel(task_id)

            return {"added": True, "reason": "OK"}

    def _cascade_cancel(self, start_task_id: str) -> List[str]:
        cancelled = []
        queue = [start_task_id]
        visited = set()
        while queue:
            curr = queue.pop(0)
            if curr not in visited:
                visited.add(curr)
                if self._tasks[curr] == "PENDING":
                    self._tasks[curr] = "CANCELLED"
                    cancelled.append(curr)
                queue.extend(self._dependents[curr] - visited)
        return sorted(cancelled)

    def claim_ready_tasks(self) -> List[str]:
        with self._lock:
            claimed = []
            for tid, status in self._tasks.items():
                if status == "PENDING":
                    deps = self._dependencies[tid]
                    if all(self._tasks[d] == "COMPLETED" for d in deps):
                        claimed.append(tid)
            for tid in claimed:
                self._tasks[tid] = "RUNNING"
            return sorted(claimed)

    def complete_task(self, task_id: str) -> bool:
        with self._lock:
            if task_id in self._tasks and self._tasks[task_id] == "RUNNING":
                self._tasks[task_id] = "COMPLETED"
                return True
            return False

    def fail_task(self, task_id: str) -> List[str]:
        with self._lock:
            if task_id not in self._tasks or self._tasks[task_id] not in ("RUNNING", "PENDING"):
                return []
            self._tasks[task_id] = "FAILED"
            cancelled = []
            queue = list(self._dependents[task_id])
            visited = set()
            while queue:
                curr = queue.pop(0)
                if curr not in visited:
                    visited.add(curr)
                    if self._tasks[curr] == "PENDING":
                        self._tasks[curr] = "CANCELLED"
                        cancelled.append(curr)
                    queue.extend(self._dependents[curr] - visited)
            return sorted(cancelled)

    def get_status(self, task_id: str) -> str:
        with self._lock:
            return self._tasks.get(task_id, "UNKNOWN")
