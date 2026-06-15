"""
Pattern analysis for stream READ_FAIL events (used with --test-stream).

``logical_frame_idx`` is the ODE loop counter at failure time, not the
container's frame index; time-based deltas are usually more meaningful.
"""

from __future__ import annotations

import statistics
from typing import List, Sequence, Tuple

# (elapsed_monotonic_sec_since_start, logical_frame_idx, read_attempt_number)
FailureEvent = Tuple[float, int, int]


def _truncate_list(values: Sequence, max_show: int = 24) -> Tuple[str, int]:
    n = len(values)
    if n <= max_show:
        return str(list(values)), n
    head = list(values[: max_show // 2])
    tail = list(values[-(max_show // 2) :])
    return f"{head} ... ({n - len(head) - len(tail)} omitted) ... {tail}", n


def format_failure_pattern_report(
    events: Sequence[FailureEvent],
    reads_ok: int,
    runtime_sec: float,
) -> List[str]:
    """
    Build log lines (without leading timestamps) for the test-stream file.

    Args:
        events: ordered failure tuples from StreamDiagnostics.
        reads_ok: successful read count (for context).
        runtime_sec: wall runtime of the run.
    """
    lines: List[str] = []
    n = len(events)
    lines.append(
        "[ANALYSIS] "
        f"read_fail_events={n} reads_ok={reads_ok} runtime_sec={runtime_sec:.2f} "
        "(logical_frame_idx = ODE counter at failure, not necessarily muxer frame number)"
    )

    if n == 0:
        lines.append("[ANALYSIS] pattern=no_failures nothing_to_correlate")
        return lines

    if n == 1:
        e0 = events[0]
        lines.append(
            "[ANALYSIS] pattern=single_failure "
            f"elapsed_run_sec={e0[0]:.2f} logical_frame_idx={e0[1]} read_attempt_number={e0[2]} "
            "(need at least 2 failures for delta_t / delta_idx statistics)"
        )
        return lines

    delta_t: List[float] = [events[i][0] - events[i - 1][0] for i in range(1, n)]
    delta_idx: List[int] = [events[i][1] - events[i - 1][1] for i in range(1, n)]
    delta_attempt: List[int] = [events[i][2] - events[i - 1][2] for i in range(1, n)]

    dt_min, dt_max = min(delta_t), max(delta_t)
    dt_mean = statistics.mean(delta_t)
    dt_stdev = statistics.stdev(delta_t) if len(delta_t) > 1 else 0.0
    cv_t = (dt_stdev / dt_mean) if dt_mean > 1e-6 else 0.0

    di_min, di_max = min(delta_idx), max(delta_idx)
    di_mean = statistics.mean(delta_idx)
    di_stdev = statistics.stdev(delta_idx) if len(delta_idx) > 1 else 0.0
    cv_i = (di_stdev / di_mean) if abs(di_mean) > 1e-6 else 0.0

    da_min, da_max = min(delta_attempt), max(delta_attempt)
    da_mean = statistics.mean(delta_attempt)

    dt_str, _ = _truncate_list(delta_t)
    di_str, _ = _truncate_list(delta_idx)
    da_str, _ = _truncate_list(delta_attempt)

    lines.append(
        "[ANALYSIS] time_between_failures_sec "
        f"deltas={dt_str} min={dt_min:.2f} max={dt_max:.2f} mean={dt_mean:.2f} stdev={dt_stdev:.2f} cv={cv_t:.2f}"
    )
    lines.append(
        "[ANALYSIS] logical_frame_idx_gap_between_failures "
        f"deltas={di_str} min={di_min} max={di_max} mean={di_mean:.1f} stdev={di_stdev:.1f} cv={cv_i:.2f}"
    )
    lines.append(
        "[ANALYSIS] read_attempt_gap_between_failures "
        f"deltas={da_str} min={da_min} max={da_max} mean={da_mean:.1f}"
    )

    # Heuristic labels (not statistical tests)
    if cv_t < 0.25 and dt_mean > 5.0:
        time_hint = "roughly_periodic_in_time (low CV and mean gap > 5s) — check network bursts, encoder GOP, cron, other clients"
    elif cv_t < 0.35:
        time_hint = "somewhat_regular_in_time — compare with server logs / parallel ffmpeg"
    else:
        time_hint = "irregular_in_time — likely random packet loss or load spikes"

    if abs(di_mean) > 10 and cv_i < 0.35:
        idx_hint = "idx_gaps_fairly_stable — may reflect steady processing FPS × time between failures (not raw stream frame index)"
    else:
        idx_hint = "idx_gaps_variable — mixed with detection skips / polygon idle / variable inference time"

    lines.append(f"[ANALYSIS] interpretation time={time_hint}")
    lines.append(f"[ANALYSIS] interpretation idx={idx_hint}")
    lines.append(
        "[ANALYSIS] tip run `ffmpeg -rtsp_transport tcp -loglevel info -i <url> -f null -` in parallel; "
        "if clean on tcp while Reaction fails, set OPENCV_FFMPEG_CAPTURE_OPTIONS=rtsp_transport;tcp"
    )

    return lines
