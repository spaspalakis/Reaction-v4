import os
from datetime import datetime
from typing import List, Optional


class StreamDiagnostics:
    """Collect and persist stream read/reconnect diagnostics for one run."""

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
            f"frame_idx={frame_idx} attempt_idx={self.total_read_attempts} "
            f"consecutive={consecutive}/{max_consecutive}"
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
        self._write(
            "[END] "
            f"attempts={self.total_read_attempts} success={self.total_read_success} "
            f"fails={self.total_read_failures} reopen_attempts={self.total_reopen_attempts} "
            f"fail_frame_indices={self.failed_read_indexes} "
            f"avg_read_fps={avg_read_fps:.2f} total_frames_read={self.total_read_success} "
            f"total_detected_objects={int(total_detected_objects)} total_runtime_sec={float(total_runtime_sec):.2f}"
        )
