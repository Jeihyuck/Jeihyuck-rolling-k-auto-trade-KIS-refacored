"""Hard process boundary for a complete trading tick."""
from __future__ import annotations

import multiprocessing as mp
import os
import queue
import signal
import time
import traceback
import json
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Callable


class TickProcessTimeout(TimeoutError):
    def __init__(self, result: dict):
        super().__init__(result.get("status", "tick process timeout"))
        self.result = result


def _child_entry(result_queue, cancellation, target: Callable, kwargs: dict) -> None:
    try:
        if hasattr(os, "setsid"):
            os.setsid()
        os.environ["US_TICK_CHILD_PID"] = str(os.getpid())
        os.environ["US_TICK_CANCELLATION_ACTIVE"] = "1"
        value = target(**kwargs)
        result_queue.put(("OK", value))
    except BaseException as exc:
        result_queue.put(("ERROR", {"status": "ERROR", "reason": str(exc), "traceback": traceback.format_exc()}))


@dataclass
class TickProcessResult:
    result: dict
    child_pid: int
    duration_sec: float


def run_tick_in_process(target: Callable, *, kwargs: dict, timeout_sec: float,
                        terminate_grace_sec: float = 10.0, mp_context=None) -> TickProcessResult:
    """Run one tick and prove the child is dead before returning or raising."""
    ctx = mp_context or mp.get_context("fork" if "fork" in mp.get_all_start_methods() else "spawn")
    result_queue = ctx.Queue(maxsize=1)
    cancellation = ctx.Event()
    child_kwargs = dict(kwargs)
    child_kwargs["tick_cancellation_event"] = cancellation
    process = ctx.Process(target=_child_entry, args=(result_queue, cancellation, target, child_kwargs), daemon=False)
    started = time.monotonic()
    process.start()
    pid = int(process.pid or 0)
    process.join(max(0.001, timeout_sec))
    if process.is_alive():
        cancellation.set()
        state_path = child_kwargs.get("active_session_state_path")
        if state_path:
            try:
                path = Path(state_path)
                state = json.loads(path.read_text(encoding="utf-8"))
                state["state"] = "CANCELLING"
                tmp = path.with_suffix(path.suffix + ".timeout.tmp")
                tmp.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
                os.replace(tmp, path)
            except Exception:
                pass
        try:
            if hasattr(os, "killpg"):
                os.killpg(pid, signal.SIGTERM)
            else:
                process.terminate()
        except ProcessLookupError:
            pass
        process.join(max(0.0, terminate_grace_sec))
        termination = "SIGTERM"
        if process.is_alive():
            termination = "SIGKILL"
            try:
                if hasattr(os, "killpg"):
                    os.killpg(pid, signal.SIGKILL)
                else:
                    process.kill()
            except ProcessLookupError:
                pass
            process.join(5.0)
        stuck = process.is_alive()
        result_queue.close()
        status = "TICK_TIMEOUT_PROCESS_STUCK" if stuck else "TICK_TIMEOUT_TERMINATED_NO_ORDER"
        raise TickProcessTimeout({"status": status, "reason": "tick_timeout", "child_pid": pid,
                                  "termination_result": termination, "process_alive": stuck,
                                  "duration_sec": time.monotonic() - started})
    try:
        kind, value = result_queue.get(timeout=1.0)
    except queue.Empty:
        kind, value = "ERROR", {"status": "ERROR", "reason": f"tick_child_exit_{process.exitcode}_without_result"}
    finally:
        result_queue.close()
    if kind == "ERROR":
        return TickProcessResult(value, pid, time.monotonic() - started)
    return TickProcessResult(value, pid, time.monotonic() - started)


def fixed_rate_slot(anchor: float, sequence: int, interval_sec: float, now: float) -> tuple[float, int]:
    """Return sleep and skipped slots without ever scheduling catch-up bursts."""
    scheduled = anchor + sequence * interval_sec
    if interval_sec <= 0 or now <= scheduled:
        return max(0.0, scheduled - now), 0
    skipped = int((now - scheduled) // interval_sec) + 1
    next_scheduled = scheduled + skipped * interval_sec
    return max(0.0, next_scheduled - now), skipped
