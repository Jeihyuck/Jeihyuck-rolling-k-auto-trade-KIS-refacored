"""Hard process boundary for a complete trading tick."""
from __future__ import annotations

import multiprocessing as mp
import os
import queue
import signal
import time
import traceback
import json
import logging
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)


class TickChildUnverifiedError(RuntimeError):
    """An order-capable child may still be alive; session must stay fenced."""


class TickProcessTimeout(TimeoutError):
    def __init__(self, result: dict):
        super().__init__(result.get("status", "tick process timeout"))
        self.result = result


def _child_entry(result_connection, cancellation, target: Callable, kwargs: dict) -> None:
    """Send a single structured result through a one-way Pipe.

    Unlike multiprocessing.Queue, the reader consumes bytes while the worker
    runs, so large serialized results cannot deadlock the parent's join().
    """
    try:
        if hasattr(os, "setsid"):
            os.setsid()
        os.environ["US_TICK_CHILD_PID"] = str(os.getpid())
        os.environ["US_TICK_CANCELLATION_ACTIVE"] = "1"
        value = target(**kwargs)
        result_connection.send(("OK", value))
    except BaseException as exc:
        try:
            result_connection.send(("ERROR", {
                "status": "ERROR", "reason": str(exc),
                "traceback": traceback.format_exc(),
            }))
        except (BrokenPipeError, OSError, EOFError):
            pass
    finally:
        result_connection.close()


@dataclass
class TickProcessResult:
    result: dict
    child_pid: int
    duration_sec: float


def run_tick_in_process(target: Callable, *, kwargs: dict, timeout_sec: float,
                        terminate_grace_sec: float = 10.0, mp_context=None) -> TickProcessResult:
    """Run an isolated tick; never infer broker non-submission from worker death."""
    ctx = mp_context or mp.get_context("fork" if "fork" in mp.get_all_start_methods() else "spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    cancellation = ctx.Event()
    child_kwargs = dict(kwargs)
    child_kwargs["tick_cancellation_event"] = cancellation
    process = ctx.Process(
        target=_child_entry, args=(sender, cancellation, target, child_kwargs),
        daemon=False,
    )
    started = time.monotonic()
    try:
        process.start()
    except BaseException:
        receiver.close()
        sender.close()
        raise
    sender.close()  # The parent must not hold a writer reference across child exit.
    pid = int(process.pid or 0)
    deadline = started + max(0.001, timeout_sec)
    payload = None
    try:
        # Consume the complete result BEFORE joining, including large results.
        while time.monotonic() < deadline:
            remaining = deadline - time.monotonic()
            if receiver.poll(min(0.1, max(0, remaining))):
                try:
                    payload = receiver.recv()
                except (EOFError, OSError, ValueError) as exc:
                    payload = ("ERROR", {
                        "status": "ERROR", "reason": "tick_result_ipc_failed",
                        "detail": type(exc).__name__,
                        "broker_submission_truth": "UNVERIFIED",
                    })
                break
            if not process.is_alive():
                # The child may have just sent its last bytes before exit.
                if receiver.poll(0.02):
                    try:
                        payload = receiver.recv()
                    except (EOFError, OSError, ValueError):
                        payload = ("ERROR", {
                            "status": "ERROR", "reason": "tick_result_ipc_failed",
                            "broker_submission_truth": "UNVERIFIED",
                        })
                break
        if payload is not None:
            process.join(max(0.0, deadline - time.monotonic()))
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
            raise TickProcessTimeout({
                "status": ("TICK_TIMEOUT_PROCESS_STUCK" if stuck else
                           "TICK_TIMEOUT_TERMINATED_SUBMIT_UNVERIFIED"),
                "reason": "tick_timeout", "child_pid": pid,
                "termination_result": termination, "process_alive": stuck,
                "broker_submission_truth": "UNVERIFIED",
                "duration_sec": time.monotonic() - started,
            })
        if payload is None:
            payload = ("ERROR", {
                "status": "ERROR",
                "reason": f"tick_child_exit_{process.exitcode}_without_result",
                "broker_submission_truth": "UNVERIFIED",
            })
        kind, value = payload
        if kind == "ERROR":
            return TickProcessResult(value, pid, time.monotonic() - started)
        return TickProcessResult(value, pid, time.monotonic() - started)
    finally:
        # SIGINT/SIGTERM from the parent session raises SystemExit while
        # receiver.poll() is running. The old finally only closed the pipe,
        # leaving the independently session-grouped, order-capable child alive
        # AFTER the parent released its session lock. Unconditionally stop an
        # active child before returning control to session finalization.
        if process.is_alive():
            cancellation.set()
            state_path = child_kwargs.get("active_session_state_path")
            if state_path:
                try:
                    state_path_obj = Path(state_path)
                    state = json.loads(state_path_obj.read_text(encoding="utf-8"))
                    state["state"] = "CANCELLING"
                    state["broker_submission_truth"] = "UNVERIFIED"
                    tmp = state_path_obj.with_suffix(state_path_obj.suffix + ".parent_signal.tmp")
                    tmp.write_text(json.dumps(state, sort_keys=True), encoding="utf-8")
                    os.replace(tmp, state_path_obj)
                except Exception:
                    pass
            try:
                if hasattr(os, "killpg") and pid > 0:
                    os.killpg(pid, signal.SIGTERM)
                else:
                    process.terminate()
            except ProcessLookupError:
                # The child may not yet have called setsid(); a missing
                # process group does not prove the process is gone.
                if process.is_alive():
                    process.terminate()
            process.join(max(0.0, terminate_grace_sec))
            if process.is_alive():
                try:
                    if hasattr(os, "killpg") and pid > 0:
                        os.killpg(pid, signal.SIGKILL)
                    else:
                        process.kill()
                except ProcessLookupError:
                    if process.is_alive():
                        process.kill()
                process.join(5.0)
            if process.is_alive():
                state_path = child_kwargs.get("active_session_state_path")
                if state_path:
                    from trader.us.utils.session_guard import mark_us_session_child_unverified_fence
                    try:
                        mark_us_session_child_unverified_fence(
                            Path(state_path).parent.name,
                            str(child_kwargs.get("session") or ""),
                            run_id=str(child_kwargs.get("session_run_id") or ""),
                            pid=pid, reason="child_alive_after_parent_shutdown",
                            env=str(child_kwargs.get("env") or ""),
                        )
                    except Exception:
                        # The session runner independently catches this typed
                        # error and withholds CLOSED/lock release even if the
                        # filesystem rejected the first durable fence write.
                        logger.exception("[US_TICK_PROCESS][FENCE_WRITE_FAILED]")
                raise TickChildUnverifiedError(
                    f"tick_child_process_stuck_after_parent_exit pid={pid} "
                    "broker_submission_truth=UNVERIFIED"
                )
        receiver.close()


def fixed_rate_slot(anchor: float, sequence: int, interval_sec: float, now: float) -> tuple[float, int]:
    """Return sleep and skipped slots without ever scheduling catch-up bursts."""
    scheduled = anchor + sequence * interval_sec
    if interval_sec <= 0 or now <= scheduled:
        return max(0.0, scheduled - now), 0
    skipped = int((now - scheduled) // interval_sec) + 1
    next_scheduled = scheduled + skipped * interval_sec
    return max(0.0, next_scheduled - now), skipped
