#!/usr/bin/env python3
"""
DistQueue-Eval Evaluation Runner & Reference Verification Suite
"""
import os
import sys
import json
import time
import re
import argparse
import importlib.util
import types
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import requests

from grader import (
    GraderTask01,
    GraderTask02,
    GraderTask03,
    GraderTask04,
    GraderTask05,
    GraderTask06,
)

# Load environment variables
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
TASKS_DIR = BASE_DIR / "tasks"
REFS_DIR = BASE_DIR / "reference_solutions"
RESULTS_DIR = BASE_DIR / "results"
TRANSCRIPTS_DIR = RESULTS_DIR / "transcripts"

TASK_GRADERS = {
    "task_01": (GraderTask01, "ref_01.py", "task_01_webhook_dedup.json"),
    "task_02": (GraderTask02, "ref_02.py", "task_02_fencing_lease.json"),
    "task_03": (GraderTask03, "ref_03.py", "task_03_batch_rollback.json"),
    "task_04": (GraderTask04, "ref_04.py", "task_04_outbox_relay.json"),
    "task_05": (GraderTask05, "ref_05.py", "task_05_sliding_window.json"),
    "task_06": (GraderTask06, "ref_06.py", "task_06_dag_scheduler.json"),
}

RECOMMENDED_MODELS = [
    "openai/gpt-4o-mini",
    "anthropic/claude-3.5-sonnet",
    "google/gemini-2.5-flash",
    "openai/gpt-4o",
    "nvidia/nemotron-3.5-lightning:free",
    "cohere/north-mini-code:free",
    "liquid/lfm-2.5-2.6b:free",
]

def ensure_dirs():
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)

def strip_code_fences(text: str) -> str:
    """Strips markdown code blocks, backticks, and extraneous text."""
    if not text or not isinstance(text, str):
        return ""
    pattern = r"```(?:python)?\s*\n([\s\S]*?)```"
    matches = re.findall(pattern, text)
    if matches:
        return matches[0].strip()
    # Handle unclosed code fence
    unclosed_pattern = r"^```(?:python)?\s*\n([\s\S]*)$"
    unclosed = re.findall(unclosed_pattern, text.strip())
    if unclosed:
        return unclosed[0].strip()
    return text.strip()

def load_module_from_file(filepath: Path) -> types.ModuleType:
    spec = importlib.util.spec_from_file_location(filepath.stem, filepath)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def load_module_from_code(code_str: str, module_name: str = "evaluated_solution") -> types.ModuleType:
    module = types.ModuleType(module_name)
    exec(code_str, module.__dict__)
    return module

def run_verify_refs() -> bool:
    """Verifies all 6 reference solutions against grader test suites."""
    print("=" * 70)
    print("DISTQUEUE-EVAL: VERIFYING ALL 6 REFERENCE SOLUTIONS")
    print("=" * 70)

    ensure_dirs()
    report = {
        "verified_at": datetime.now(timezone.utc).isoformat(),
        "tasks": {},
        "overall_score": 0.0,
        "all_passed": True,
    }

    total_tasks = len(TASK_GRADERS)
    total_score_sum = 0.0

    for task_id in sorted(TASK_GRADERS.keys()):
        grader_cls, ref_file, _ = TASK_GRADERS[task_id]
        ref_path = REFS_DIR / ref_file
        grader = grader_cls()

        print(f"\n[RUNNING] Task {task_id} ({ref_file})...", flush=True)
        try:
            ref_module = load_module_from_file(ref_path)
            res = grader.grade(ref_module)
        except Exception as e:
            res = {
                "task_id": task_id,
                "total_score": 0.0,
                "sub_scores": {},
                "feedback": {"fatal_error": f"Failed to load/execute reference: {str(e)}"},
                "passed": False,
            }

        score = res.get("total_score", 0.0)
        total_score_sum += score
        passed = res.get("passed", False)
        if not passed:
            report["all_passed"] = False

        report["tasks"][task_id] = res

        print(f"Task {task_id}: Score = {score:.2f} / 1.00 {'[PASS]' if passed else '[FAIL]'}", flush=True)
        for bucket, sub_score in res.get("sub_scores", {}).items():
            status = "PASS" if sub_score > 0 else "FAIL"
            print(f"  - {bucket:<25}: {sub_score:.2f} [{status}]", flush=True)

    overall_score = round(total_score_sum / total_tasks, 4)
    report["overall_score"] = overall_score

    report_path = RESULTS_DIR / "verify_refs_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    print("\n" + "=" * 70)
    print(f"VERIFY-REFS SUMMARY: Overall Reference Score: {overall_score * 100:.1f}%")
    print(f"Full report written to: {report_path}")
    print("=" * 70)

    return report["all_passed"]

def call_openrouter(model: str, prompt: str, api_key: str) -> str:
    """Calls OpenRouter API with temperature=0.0."""
    url = "https://openrouter.ai/api/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/micro1-eval-bench/distqueue-eval",
        "X-Title": "DistQueue-Eval Benchmark",
    }
    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": "You are a world-class distributed systems engineer. Output ONLY valid, executable Python code with no markdown formatting or commentary.",
            },
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.0,
        "max_tokens": 4096,
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=(10, 35))
    if resp.status_code != 200:
        raise RuntimeError(f"OpenRouter API error {resp.status_code}: {resp.text}")

    data = resp.json()
    choices = data.get("choices", [])
    if not choices:
        raise RuntimeError(f"No completion choices returned: {data}")

    content = choices[0].get("message", {}).get("content")
    if content is None:
        content = choices[0].get("message", {}).get("reasoning", "") or ""
    return str(content)

def evaluate_model(model_name: str, target_task: str = None):
    """Evaluates an LLM via OpenRouter on all or specific tasks."""
    api_key = os.environ.get("OPENROUTER_API_KEY")
    if not api_key:
        print("ERROR: OPENROUTER_API_KEY environment variable is not set.", file=sys.stderr)
        sys.exit(1)

    ensure_dirs()
    tasks_to_eval = [target_task] if target_task else sorted(TASK_GRADERS.keys())
    sanitized_model = model_name.replace("/", "_").replace(":", "_")

    print("=" * 70)
    print(f"EVALUATING MODEL: {model_name}")
    print(f"Tasks to run: {', '.join(tasks_to_eval)}")
    print("=" * 70)

    model_results = {
        "model": model_name,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
        "temperature": 0.0,
        "task_scores": {},
        "average_score": 0.0,
    }

    total_score = 0.0

    for task_id in tasks_to_eval:
        grader_cls, _, task_json_file = TASK_GRADERS[task_id]
        with open(TASKS_DIR / task_json_file, "r", encoding="utf-8") as f:
            task_data = json.load(f)

        prompt = task_data["prompt"]
        grader = grader_cls()

        print(f"\n[EVAL] Querying {model_name} for {task_id} ({task_data['title']})...", flush=True)
        t0 = time.time()
        transcript = {
            "model": model_name,
            "task_id": task_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "prompt": prompt,
            "raw_response": "",
            "cleaned_code": "",
            "grade_result": {},
            "elapsed_seconds": 0.0,
            "status": "INIT",
        }

        try:
            raw_response = call_openrouter(model_name, prompt, api_key)
            cleaned_code = strip_code_fences(raw_response)
            transcript["raw_response"] = raw_response
            transcript["cleaned_code"] = cleaned_code
            print(f"  [RECEIVED] Response received in {time.time() - t0:.2f}s. Cleaning & grading...", flush=True)
        except Exception as e:
            print(f"  [ERROR] OpenRouter generation failed: {e}", flush=True)
            transcript["status"] = "API_ERROR"
            transcript["error"] = str(e)
            grade_res = {
                "task_id": task_id,
                "total_score": 0.0,
                "sub_scores": {},
                "feedback": {"api_error": str(e)},
                "passed": False,
            }
            transcript["grade_result"] = grade_res
            model_results["task_scores"][task_id] = grade_res
            transcript_path = TRANSCRIPTS_DIR / f"{sanitized_model}_{task_id}.json"
            with open(transcript_path, "w", encoding="utf-8") as tf:
                json.dump(transcript, tf, indent=2)
            continue

        # Grade generated module with exception safety
        try:
            gen_module = load_module_from_code(cleaned_code, f"{sanitized_model}_{task_id}")
            grade_res = grader.grade(gen_module)
            transcript["status"] = "GRADED"
        except Exception as e:
            transcript["status"] = "EXECUTION_ERROR"
            grade_res = {
                "task_id": task_id,
                "total_score": 0.0,
                "sub_scores": {},
                "feedback": {"import_or_syntax_error": str(e)},
                "passed": False,
            }

        transcript["grade_result"] = grade_res
        transcript["elapsed_seconds"] = round(time.time() - t0, 2)
        score = grade_res.get("total_score", 0.0)
        total_score += score
        model_results["task_scores"][task_id] = grade_res

        transcript_path = TRANSCRIPTS_DIR / f"{sanitized_model}_{task_id}.json"
        with open(transcript_path, "w", encoding="utf-8") as tf:
            json.dump(transcript, tf, indent=2)

        print(f"Task {task_id}: Score = {score:.2f} / 1.00 ({grade_res.get('total_score', 0.0)*100:.1f}%)", flush=True)
        for bucket, sub_score in grade_res.get("sub_scores", {}).items():
            print(f"  - {bucket:<25}: {sub_score:.2f}", flush=True)

    avg_score = round(total_score / len(tasks_to_eval), 4)
    model_results["average_score"] = avg_score

    # Update summary leaderboard
    update_leaderboard(model_name, model_results)

    print("\n" + "=" * 70, flush=True)
    print(f"MODEL EVALUATION COMPLETE: {model_name}", flush=True)
    print(f"Average Score across {len(tasks_to_eval)} tasks: {avg_score * 100:.1f}%", flush=True)
    print("=" * 70, flush=True)

def update_leaderboard(model_name: str, new_eval_result: dict):
    leaderboard_path = RESULTS_DIR / "summary_leaderboard.json"
    leaderboard = {}
    if leaderboard_path.exists():
        try:
            with open(leaderboard_path, "r", encoding="utf-8") as f:
                leaderboard = json.load(f)
        except Exception:
            leaderboard = {}

    if model_name in leaderboard:
        existing_tasks = leaderboard[model_name].get("task_scores", {})
        existing_tasks.update(new_eval_result.get("task_scores", {}))
        new_eval_result["task_scores"] = existing_tasks
        avg = round(sum(t.get("total_score", 0.0) for t in existing_tasks.values()) / len(TASK_GRADERS), 4)
        new_eval_result["average_score"] = avg

    leaderboard[model_name] = new_eval_result

    with open(leaderboard_path, "w", encoding="utf-8") as f:
        json.dump(leaderboard, f, indent=2)

def main():
    parser = argparse.ArgumentParser(description="DistQueue-Eval evaluation runner.")
    parser.add_argument("--verify-refs", action="store_true", help="Run verification on all 6 reference solutions.")
    parser.add_argument("--model", type=str, help="OpenRouter model slug to evaluate (e.g. openai/gpt-4o-mini).")
    parser.add_argument("--task", type=str, help="Specific task ID to evaluate (e.g. task_01).")
    parser.add_argument("--list-models", action="store_true", help="List recommended models for benchmarking.")

    args = parser.parse_args()

    if args.list_models:
        print("Recommended models for DistQueue-Eval benchmark:")
        for m in RECOMMENDED_MODELS:
            print(f"  - {m}")
        sys.exit(0)

    if args.verify_refs:
        passed = run_verify_refs()
        sys.exit(0 if passed else 1)

    if args.model:
        evaluate_model(args.model, args.task)
        sys.exit(0)

    parser.print_help()
    sys.exit(0)

if __name__ == "__main__":
    main()
