import os
from datetime import datetime
from typing import List, Optional


class StreamDiagnostics:
    """Collect and persist stream read/reconnect diagnostics for one run.

    Metrics (see [END] line):
    - read_attempts: how many times the main loop called ``camera.read()`` (each loop = one attempt).
    - reads_ok: ``read()`` returned success (``ret`` true); same as total_frames_read.
    - reads_failed: number of failed ``read()`` calls (each failure is one event).
    - reopen_attempts: how many times we logged a stream reopen try (can be >1 if multiple reconnect cycles).
    - failed_at_logical_frame_idx: ``fr_count`` from ODE at each failure (logical frame counter when the
      bad read happened; not necessarily the container's frame number).
    - avg_read_fps: reads_ok / wall-clock seconds for the whole run (includes waits, Kafka, model, etc.).
    - total_runtime_min: wall time from [START] to [END] in minutes.
    """

    def __init__(self, enabled: bool, path: Optional[str], stats_every_n_reads: int = 100):
        self.enabled = enabled and bool(path)
        self.path = os.path.abspath(path) if path else None
        self.stats_every_n_reads = int(stats_every_n_reads)

        self.total_read_attempts = 0
        self.total_read_success = 0
        self.total_read_failures = 0
        self.total_reopen_attempts = 0
        self.failed_read_indexes: List[int] = []

    def _write(self, message: str):
        if not self.enabled or not self.path:
            return
        ts = datetime.utcnow().isoformat() + "Z"
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(f"{ts} | {message}\n")

    def start(self, source_path: Optional[str], input_mode: Optional[str], stream_mode: bool):
        self._write(
            "[START] "
            f"source={source_path} input_mode={input_mode} "
            f"stream_mode={stream_mode} stats_every={self.stats_every_n_reads}"
        )

    def read_attempt(self):
        self.total_read_attempts += 1

    def read_failure(self, frame_idx: int, consecutive: int, max_consecutive: int):
        self.total_read_failures += 1
        self.failed_read_indexes.append(frame_idx)
        self._write(
            "[READ_FAIL] "
            f"logical_frame_idx={frame_idx} read_attempt_number={self.total_read_attempts} "
            f"consecutive_bad_reads={consecutive}/{max_consecutive}"
        )

    def reopen_attempt(self, attempt: int, max_attempts: int):
        self.total_reopen_attempts += 1
        self._write(
            "[REOPEN_ATTEMPT] "
            f"attempt={attempt}/{max_attempts} total_reopen_attempts={self.total_reopen_attempts}"
        )

    def reopen_success(self):
        self._write("[REOPEN_SUCCESS] stream reopened")

    def file_end_or_fail(self):
        self._write("[FILE_END_OR_FAIL] stopping detection loop")

    def stream_end(self):
        self._write("[STREAM_END] exceeded retry tolerance; exiting")

    def read_success(self, recovered_after: int):
        self.total_read_success += 1
        if recovered_after > 0:
            self._write(f"[RECOVERED] after_consecutive_failures={recovered_after}")

    def end(self, total_detected_objects: int = 0, total_runtime_sec: float = 0.0):
        avg_read_fps = (self.total_read_success / total_runtime_sec) if total_runtime_sec > 0 else 0.0
        total_runtime_min = float(total_runtime_sec) / 60.0 if total_runtime_sec > 0 else 0.0
        self._write(
            "[END] "
            f"read_attempts={self.total_read_attempts} reads_ok={self.total_read_success} "
            f"reads_failed={self.total_read_failures} reopen_attempts={self.total_reopen_attempts} "
            f"failed_at_logical_frame_idx={self.failed_read_indexes} "
            f"avg_read_fps={avg_read_fps:.2f} total_frames_read={self.total_read_success} "
            f"total_detected_objects={int(total_detected_objects)} "
            f"total_runtime_min={total_runtime_min:.2f}"
        )
