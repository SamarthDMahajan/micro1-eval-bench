import asyncio
import collections
import time
from typing import List, Dict, Any, Callable

async def process_batches(
    items: List[Dict[str, Any]],
    sub_batch_size: int,
    rate_limit_per_sec: int,
    max_retries: int,
    worker_fn: Callable,
    rollback_fn: Callable
) -> Dict[str, Any]:
    committed_items: List[str] = []
    rolled_back_items: List[str] = []
    sub_batch_statuses: List[str] = []
    total_attempts = 0

    call_timestamps = collections.deque()

    async def acquire_rate_token():
        while True:
            now = time.monotonic()
            while call_timestamps and now - call_timestamps[0] >= 1.0:
                call_timestamps.popleft()
            if len(call_timestamps) < rate_limit_per_sec:
                call_timestamps.append(time.monotonic())
                return
            wait_time = 1.0 - (now - call_timestamps[0]) + 0.002
            if wait_time > 0:
                await asyncio.sleep(wait_time)

    sub_batches = [items[i:i + sub_batch_size] for i in range(0, len(items), sub_batch_size)]

    for sub_batch in sub_batches:
        succeeded_in_sub_batch = []
        aborted = False

        for item in sub_batch:
            attempts_for_item = 0
            while True:
                await acquire_rate_token()
                total_attempts += 1
                try:
                    await worker_fn(item)
                    succeeded_in_sub_batch.append(item)
                    break
                except Exception as exc:
                    is_retryable = getattr(exc, "retryable", False)
                    if is_retryable and attempts_for_item < max_retries:
                        attempts_for_item += 1
                        continue
                    else:
                        # Permanent failure - abort sub-batch immediately
                        aborted = True
                        for s_item in reversed(succeeded_in_sub_batch):
                            await rollback_fn(s_item)
                            rolled_back_items.append(s_item["id"])
                        sub_batch_statuses.append("ROLLED_BACK")
                        break

            if aborted:
                break

        if not aborted:
            committed_items.extend([it["id"] for it in succeeded_in_sub_batch])
            sub_batch_statuses.append("COMMITTED")

    return {
        "committed_items": committed_items,
        "rolled_back_items": rolled_back_items,
        "total_attempts": total_attempts,
        "sub_batch_statuses": sub_batch_statuses,
    }
