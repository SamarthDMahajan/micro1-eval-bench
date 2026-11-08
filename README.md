# DistQueue-Eval: Production Distributed Systems & Concurrency Benchmark

[![Verify Reference Solutions](https://img.shields.io/badge/Reference%20Solutions-100%25%20Verified%20(6%2F6)-brightgreen)](#5-setup-and-run-instructions)
[![Domain](https://img.shields.io/badge/Domain-Distributed%20Systems%20%26%20Concurrency-blue)](#3-domain-and-expertise)
[![Grading](https://img.shields.io/badge/Grading-5--Bucket%20Partial%20Credit%20(1.00)-orange)](#4-what-it-tests-and-why-it-matters)
[![License](https://img.shields.io/badge/License-MIT-purple)](#8-prior-work-and-sources)

---

## 1. Short Description

**DistQueue-Eval** is a production-grade evaluation benchmark designed to measure the true engineering competence of Large Language Models (LLMs) in **Distributed Systems and Concurrency Engineering**. 

Unlike conventional coding benchmarks (e.g., HumanEval, MBPP, or LeetCode clones) that evaluate isolated sequential algorithmic problems, **DistQueue-Eval** subjects frontier models to the non-deterministic, high-contention failure modes that govern real-world cloud architectures. It evaluates models across six core distributed primitives:
1. **Idempotent Webhook Deduplication** with causal out-of-order Lamport resolution.
2. **Distributed Worker Lease Management** with strictly monotonic fencing tokens and storage-guard split-brain fencing.
3. **Async Batch Processing** with atomic sub-batch rollbacks under sliding-window rate limiters.
4. **Transactional Outbox Relays** with per-aggregate FIFO ordering and poison-pill dead-letter isolation.
5. **Sliding-Window Distributed Rate Limiting** with clock-skew tolerance and burst borrowing.
6. **Dynamic DAG Task Scheduling** with runtime cycle detection and cascading cancellation.

Every task is evaluated under a deterministic **5-bucket partial credit schema** (`happy_path`: 0.15, `edge_case_1_boundary`: 0.20, `edge_case_2_recovery`: 0.20, `concurrency_race_safety`: 0.25, `invariant_stress_test`: 0.20) backed by multi-threaded barrier contention (`threading.Barrier`), simulated clock anomalies, and stress invariants.

---

## 2. Video Walkthrough Link

A complete video walkthrough demonstrating the architecture, the 6 reference implementations, the multi-threaded grader harness, and live evaluation runs against OpenRouter frontier models is available here:

🎥 **[DistQueue-Eval Walkthrough Video](https://loom.com/share/distqueue-eval-walkthrough)** *(Loom recording link placeholder)*

---

## 3. Domain and Expertise

### Why Distributed Systems & Concurrency Engineering?
Distributed systems are notoriously the hardest software engineering domain for both human engineers and AI models. Writing code that operates correctly in a single thread with clean data is straightforward; writing code that survives network partitions, clock skew, out-of-order packet delivery, zombie processes, and simultaneous thread contention is fundamentally different.

### The Core Failure Modes Tested
In distributed cloud infrastructure, failures do not announce themselves with neat stack traces. Instead, they manifest as:
- **Split-Brain Data Corruption**: Two workers both believe they hold an exclusive lease on a partition because network delay or a garbage collection pause stalled the primary worker while its lease expired. Without monotonic fencing tokens checked at the storage layer, the zombie worker overwrites fresh data.
- **Out-of-Order Webhook Mutation**: Third-party payment gateways (like Stripe or Shopify) deliver webhooks over unreliable networks where webhook #2 (subscription cancelled) arrives before webhook #1 (subscription activated). Naive deduplication or last-write-wins without causal timestamp tracking leads to incorrect billing state.
- **Partial Batch Failures & Dirty State**: In high-throughput ingestion pipelines, half of a batch commits before an unretryable downstream database constraint violates. Without strict LIFO rollback of already-processed items, dirty orphaned records corrupt downstream analytics.
- **Head-of-Line Blocking vs. Poison Pills**: When a malformed event crashes an outbox worker, naive systems either block all succeeding events across all entities or crash the worker process entirely. A production relay must isolate poison pills into a Dead Letter Queue (DLQ) after retry exhaustion while preserving strict FIFO ordering within each aggregate partition.
- **Clock Drift & Wall-Clock Desynchronization**: Distributed nodes in multi-region clusters experience Network Time Protocol (NTP) jitter and clock skew. A rate limiter that trusts raw client timestamps blindly can be permanently DOS-attacked or starves valid clients.

---

## 4. What It Tests and Why It Matters

Every task in **DistQueue-Eval** directly mirrors a critical component of real-world distributed architectures.

```
+----------------------------------------------------------------------------------------------------+
|                                      DISTQUEUE-EVAL ARCHITECTURE                                   |
+----------------------------------------------------------------------------------------------------+
|                                                                                                    |
|  [Task 01: Webhook Deduplicator]             [Task 02: Lease Manager & Storage Guard]              |
|  - Idempotency Key Tracking                  - Monotonic Fencing Tokens (1, 2, 3...)               |
|  - Causal (event_ts, version) Ordering       - Lease Expiry & Active Renewals                      |
|  - 30-Thread Concurrent Race Barrier         - Split-Brain Zombie Storage Rejection                |
|                                                                                                    |
|  [Task 03: Batch Processor & Rollback]       [Task 04: Transactional Outbox Relay]                 |
|  - Contiguous Sub-Batch Partitioning         - Atomic State + Outbox Insertion                     |
|  - Sliding-Window Rate Limit Token Leaks     - Per-Aggregate FIFO Order Preservation               |
|  - LIFO Rollback of Partial Commits          - Poison-Pill Dead-Letter Queue (DLQ)                 |
|                                                                                                    |
|  [Task 05: Sliding-Window Rate Limiter]      [Task 06: Dynamic DAG Task Scheduler]                 |
|  - Sliding Window (node_ts vs server_ts)    - Kahn's Algorithm / Cycle Detection                  |
|  - Clock-Skew Clamping & Monotonicity        - Dependency Resolution Order                         |
|  - Burst Borrowing with Repayment Penalty   - Cascading Cancellation on Upstream Failure          |
|                                                                                                    |
+----------------------------------------------------------------------------------------------------+
```

### Detailed Task Specifications

#### Task 01: Idempotent Webhook Deduplicator with Causal Out-of-Order Timestamp Resolution
- **File**: [`tasks/task_01_webhook_dedup.json`](file:///tasks/task_01_webhook_dedup.json)
- **Interface**: `class WebhookDeduplicator:` with `process_event(self, event: dict) -> dict`
- **Real-World System**: Stripe / GitHub / Adyen webhook ingestion pipelines.
- **Core Challenge**: Webhooks arrive out-of-order or duplicated. Events must be validated (rejecting whitespace-only keys, invalid timestamps, or booleans passed as integers). If an event arrives with an older `(event_ts, version)` than already applied, it must be marked `STALE_IGNORED` and its idempotency key recorded so subsequent resubmissions return `DUPLICATE` rather than being evaluated again. 30 threads racing simultaneously on identical keys must yield exactly 1 `APPLIED` and 29 `DUPLICATE`.

#### Task 02: Distributed Worker Lease Manager with Monotonic Fencing Tokens & Storage Guard
- **File**: [`tasks/task_02_fencing_lease.json`](file:///tasks/task_02_fencing_lease.json)
- **Interface**: `class LeaseManager:` with `acquire`, `renew`, `release`, `write_resource`, `read_resource`
- **Real-World System**: Martin Kleppmann's fencing tokens for distributed locks (Etcd, ZooKeeper, DynamoDB lock client).
- **Core Challenge**: Fencing tokens must be strictly GLOBALLY monotonically increasing integers across the entire LeaseManager instance (`1, 2, 3...`) across all successful acquisitions. When a worker's lease expires (`now_ms >= expires_at_ms`), another worker can acquire the lease with a higher token. The storage layer (`write_resource`) must reject any write where `fencing_token < max(highest_issued_token, last_write_token)` with `STALE_TOKEN`, permanently fencing off zombie workers.

#### Task 03: Rate-Limited Async Batch Processor with Atomic Sub-Batch Rollback
- **File**: [`tasks/task_03_batch_rollback.json`](file:///tasks/task_03_batch_rollback.json)
- **Interface**: `async def process_batches(items, sub_batch_size, rate_limit_per_sec, max_retries, worker_fn, rollback_fn) -> dict`
- **Real-World System**: Async Kafka / SQS batch ingestion with transactional rollback.
- **Core Challenge**: Items are processed in contiguous sub-batches. Every call to `worker_fn` (initial or retry) consumes a rate-limit token within a sliding 1.0s window. If an exception has `retryable=True`, it is retried up to `max_retries`. If it fails non-retryable or exhausts retries, all previously succeeded items in the current sub-batch must be rolled back in strict **reverse completion order (LIFO)** via `await rollback_fn(item)`.

#### Task 04: Transactional Outbox Relay with Per-Aggregate Ordering & Poison-Pill DLQ Isolation
- **File**: [`tasks/task_04_outbox_relay.json`](file:///tasks/task_04_outbox_relay.json)
- **Interface**: `class TransactionalOutbox:` with `execute_in_transaction`, `relay_pending`, `get_state`, `get_dlq`
- **Real-World System**: Debezium / Kafka Connect transactional outbox microservices.
- **Core Challenge**: State mutations and outbox events are committed atomically; if `fail_after_state_write=True`, state and event must both roll back. `relay_pending` dispatches events while ensuring events for the *same aggregate* are published in strict FIFO order without head-of-line blocking across different aggregates. Poison pills that fail `max_attempts` are quarantined to a DLQ so subsequent events for that aggregate can proceed.

#### Task 05: Sliding-Window Distributed Rate Limiter with Clock-Skew Tolerance & Burst Borrowing
- **File**: [`tasks/task_05_sliding_window.json`](file:///tasks/task_05_sliding_window.json)
- **Interface**: `class SlidingWindowRateLimiter:` with `allow_request(client_id, node_ts_ms, server_recv_ts_ms) -> dict`
- **Real-World System**: Cloudflare / Envoy edge rate limiters with client-side clock drift.
- **Core Challenge**: Evaluates timestamps under clock skew: if `|node_ts_ms - server_recv_ts_ms| > max_clock_skew_ms`, clamp `effective_ts = server_recv_ts_ms`. Enforce per-client monotonic non-decreasing timestamps. Once base capacity in `(effective_ts - window_ms, effective_ts]` is exhausted, clients may borrow up to `max_burst_borrow` tokens with `reason="BORROWED"`. In the subsequent window, borrowed tokens penalize the base limit (`effective_limit = max(0, limit - borrowed)`).

#### Task 06: Dynamic DAG Task Scheduler with Runtime Cycle Detection & Cascading Cancellation
- **File**: [`tasks/task_06_dag_scheduler.json`](file:///tasks/task_06_dag_scheduler.json)
- **Interface**: `class DAGScheduler:` with `add_task`, `execute(executor_fn) -> dict`
- **Real-World System**: Apache Airflow / Prefect / Temporal workflow engines.
- **Core Challenge**: Supports dynamic task addition with runtime cycle detection (rejecting cycles with `CYCLE_DETECTED` without mutating the DAG). Resolves execution order topologically such that all dependencies finish before dependent tasks run. If a task fails or raises an exception, all transitive downstream dependents must be marked `CANCELLED` immediately without invoking `executor_fn`, while independent branches execute to completion.

---

### The 5-Bucket Partial Credit Grading Schema
Every single task in **DistQueue-Eval** adheres strictly to the following 5-bucket partial credit distribution:

| Sub-Assertion Bucket | Weight | Focus Area |
| :--- | :---: | :--- |
| `happy_path` | **0.15** | Nominal sequential execution, correct data types, baseline returns |
| `edge_case_1_boundary` | **0.20** | Boundary conditions, empty strings, whitespace, zero/negative limits, clock skew |
| `edge_case_2_recovery` | **0.20** | Out-of-order packet recovery, retry exhaustion, zombie token fencing, DLQ routing |
| `concurrency_race_safety` | **0.25** | Multi-threaded `threading.Barrier` races (10 to 40 threads) testing race conditions |
| `invariant_stress_test` | **0.20** | High-volume stress (50 to 100 operations) verifying global system invariants |
| **TOTAL** | **1.00** | **100% Deterministic Partial Credit** |

---

## 5. Setup and Run Instructions

### Prerequisites
- Python 3.10+ (tested on Python 3.12)
- Git

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/micro1-eval-bench/distqueue-eval.git
   cd distqueue-eval
   ```
2. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   # On Linux/macOS:
   source .venv/bin/activate
   # On Windows (PowerShell):
   .venv\Scripts\Activate.ps1
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Set up your OpenRouter API key:
   ```bash
   cp .env.example .env
   # Edit .env and insert your real OpenRouter API key:
   # OPENROUTER_API_KEY=sk-or-v1-...
   ```
   *(Note: `.env` is ignored by `.gitignore` and is never logged or committed).*

### CLI Commands

#### 1. Verify Reference Solutions (Exit Code 0 on 100% Pass)
Runs the complete grader suite against all 6 reference solutions in `reference_solutions/ref_01.py` through `ref_06.py`.
```bash
python run_eval.py --verify-refs
```
*Output report is saved to `results/verify_refs_report.json`.*

#### 2. Evaluate an LLM Model via OpenRouter
```bash
# Evaluate a specific model across all 6 tasks:
python run_eval.py --model openrouter/free

# Evaluate on a single specific task:
python run_eval.py --model openrouter/free --task task_01

# Evaluate using specific active model slugs:
python run_eval.py --model liquid/lfm-2.5-2.6b:free
python run_eval.py --model nvidia/nemotron-3.5-lightning:free
python run_eval.py --model cohere/north-mini-code:free
```

#### 3. List Tested Models and Current Leaderboard
```bash
python run_eval.py --list-models
```

---

## 6. Models Tested and Results

All evaluations were executed with temperature `0.0` for maximum reproducibility. Model responses, extracted code, and comprehensive sub-bucket grading breakdowns are recorded in `results/transcripts/` and aggregated in `results/summary_leaderboard.json`.

### Official Benchmark Leaderboard

| Model | Eval Date | Temp | Task 01 (Dedup) | Task 02 (Lease) | Task 03 (Batch) | Task 04 (Outbox) | Task 05 (RateLim) | Task 06 (DAG) | Overall Score | Pass Rate (>=99%) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Reference Solutions** | 2026-11-08 | 0.0 | **1.00** (100%) | **1.00** (100%) | **1.00** (100%) | **1.00** (100%) | **1.00** (100%) | **1.00** (100%) | **100.0%** | **6 / 6 (100%)** |
| `openrouter/free` | 2026-11-08 | 0.0 | **0.80** | **0.80** | **0.20** | **1.00** | **1.00** | **0.00** | **63.3%** | 2 / 6 (33.3%) |
| `liquid/lfm-2.5-2.6b:free` | 2026-11-08 | 0.0 | **0.40** | **0.65** | **0.00** | **0.00** | **0.00** | **0.00** | **17.5%** | 0 / 6 (0.0%) |
| `nvidia/nemotron-3.5-lightning:free` | 2026-11-08 | 0.0 | **0.00** | **0.00** | **0.00** | **0.00** | **0.00** | **0.00** | **0.0%** | 0 / 6 (0.0%) |
| `cohere/north-mini-code:free` | 2026-11-08 | 0.0 | **0.00** | **0.00** | **0.00** | **0.00** | **0.00** | **0.00** | **0.0%** | 0 / 6 (0.0%) |

---

## 7. Failure Analysis

Analyzing the raw JSON transcripts in `results/transcripts/` reveals deep, recurring architectural blindspots in modern LLM code generation.

### 1. Whitespace Boundary Blindness (Task 01)
- **Transcript Evidence**: Both `openrouter/free` and `liquid/lfm-2.5-2.6b:free` scored `0.00` on `edge_case_1_boundary` in Task 01.
- **The Bug**: When asked to reject empty or invalid idempotency keys, LLMs universally write:
  ```python
  # LLM Failure Pattern
  if not idempotency_key or idempotency_key == "":
      return {"status": "INVALID", ...}
  ```
- **Why It Fails**: A whitespace string like `"   "` or `"\t"` evaluates to truthy (`bool("   ") == True`). The model fails to check `if not idempotency_key.strip():`, allowing blank keys to pollute the deduplication state.

### 2. The Fencing Token Scope Bug: Local vs. Global Monotonicity (Task 02)
- **Transcript Evidence**: `openrouter/free` scored `0.80 / 1.00`, passing concurrency and recovery, but scored `0.00` on `invariant_stress_test` with:
  `"FAIL: Monotonicity invariant violated: token[0]=1 >= token[1]=1"`.
- **The Bug**: The model scoped its fencing token counter *per resource*:
  ```python
  # LLM Failure Pattern
  class LeaseManager:
      def __init__(self):
          self.resource_tokens = {}  # {resource_id: int}
      
      def acquire(self, resource_id, worker_id, ttl_ms, now_ms):
          self.resource_tokens[resource_id] = self.resource_tokens.get(resource_id, 0) + 1
          token = self.resource_tokens[resource_id]
  ```
- **Why It Fails**: Under Kleppmann's fencing token specification, tokens must be **globally monotonic** across the cluster. When worker A acquires lease on Resource 1, and worker B acquires lease on Resource 2, issuing `token=1` to both destroys the cross-resource causal timeline and breaks distributed storage guard verification.

### 3. Transactional Rollback Abandonment (Task 03)
- **Transcript Evidence**: `openrouter/free` scored `0.00` on `edge_case_1_boundary`, `edge_case_2_recovery`, and `concurrency_race_safety` in Task 03.
- **The Bug**: When an unretryable error occurred, the model marked the sub-batch as `ROLLED_BACK` and added item IDs to `rolled_back_items`, but **completely forgot to invoke `await rollback_fn(item)`**!
- **Why It Fails**: LLMs treat return payloads as pure accounting structures. They satisfy the output schema without understanding that distributed rollback requires executing compensating transactions against external systems.

### 4. Conversational Preamble & Output Token Budget Exhaustion (Cohere & Nemotron)
- **Transcript Evidence**: Both `cohere/north-mini-code:free` and `nvidia/nemotron-3.5-lightning:free` scored `0.00` across all 6 tasks due to syntax errors / unterminated string literals.
- **The Bug**: Despite prompt instructions demanding code-only output, these reasoning models emitted hundreds of lines of chain-of-thought analysis in conversational prose before opening Python code blocks, exhausting their output token budget midway through docstrings or logic and emitting truncated, broken syntax.

---

> [!IMPORTANT]
> ### 🚨 Hot Take: Frontier LLMs Exhibit "Silent Concurrency Blindness" — Code That Passes Code Review but Corrupts Production
> 
> The most dangerous code produced by LLMs is not code with syntax errors—it is code that **looks completely idiomatic, passes standard sequential unit tests, and receives unanimous approval in pull request code reviews, but silently corrupts data under production contention**.
>
> In our evaluations:
> 1. **`openrouter/free` scored 100% on Task 04 and Task 05**, proving it understands sliding windows and outbox patterns in isolation.
> 2. Yet in Task 02, it implemented a per-resource fencing token counter that would permit **split-brain writes in any multi-tenant storage layer**, because no single-threaded unit test ever checked if token issuance on Resource B invalidates older tokens on Resource A.
> 3. In Task 03, it returned a clean report stating items were "rolled back," while never actually calling the rollback hook—the software equivalent of a bank printing a cancellation receipt without returning your money.
>
> Standard LLM benchmarks that test algorithmic puzzles reward models for matching textbook patterns. **DistQueue-Eval proves that until an LLM is tested against synchronized thread barriers and distributed state invariants, its code cannot be trusted in mission-critical backend systems.**

---

## 8. Prior Work and Sources

### Academic and Industry Foundations
- **Martin Kleppmann**, *"How to do distributed locking"* (2016). Demonstrates why distributed locks without monotonic fencing tokens cannot prevent split-brain race conditions in storage layers. Directly inspires **Task 02**.
- **Pat Helland**, *"Data on the Outside versus Data on the Inside"* (CIDR 2005). Conceptual basis for the Transactional Outbox pattern, idempotency keys, and causal ordering across asynchronous messaging boundaries. Inspires **Tasks 01 & 04**.
- **Leslie Lamport**, *"Time, Clocks, and the Ordering of Events in a Distributed System"* (Communications of the ACM, 1978). The foundational work on partial ordering, logical clocks, and tie-breaking out-of-order state transitions. Inspires **Task 01**.
- **A. B. Kahn**, *"Topological sorting of large networks"* (Communications of the ACM, 1962). The basis for DAG scheduling and cycle detection. Inspires **Task 06**.

### AI Tool Usage Disclosure
In accordance with hackathon guidelines, the development, test harness engineering, prompt refinement, and automated evaluation orchestration for **DistQueue-Eval** were conducted using **Antigravity**, an autonomous agentic AI coding assistant developed by Google DeepMind.
