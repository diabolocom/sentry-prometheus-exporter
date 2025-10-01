from __future__ import annotations
import os
import time
import tempfile
import pickle
import fcntl
from typing import Any, Optional


class FileCachePickleSingleFlight:
    """
    Cross-process TTL cache for arbitrary Python objects (via pickle) with POSIX flock.
    - Atomic writer (tmp file + os.replace)
    - Payload format: dict(generated_at: float, data: Any)
    - Single-flight: EX lock ensures only one process recomputes
    """

    def __init__(
        self,
        cache_dir: str,
        key: str,
        ttl_sec: int,
        *,
        allow_stale_on_error: bool = True,
    ):
        self.ttl_sec = int(ttl_sec)
        self.allow_stale_on_error = bool(allow_stale_on_error)
        os.makedirs(cache_dir, exist_ok=True)
        safe_key = "".join(
            c if c.isalnum() or c in ("-", "_", ".") else "_" for c in key
        )
        self.cache_path = os.path.join(cache_dir, f"{safe_key}.pkl")
        self.lock_path = os.path.join(cache_dir, f"{safe_key}.lock")

    def get_or_compute(self, compute) -> Any:
        # Fast path: fresh cache (no lock)
        payload = self._read()
        if self._is_fresh(payload):
            return payload["data"]

        # Single-flight across processes
        with _exclusive_lock(self.lock_path):
            # Re-check under lock
            payload = self._read()
            if self._is_fresh(payload):
                return payload["data"]

            try:
                data = compute()  # may be ANY Python object (e.g., list[MetricFamily])
                self._write({"generated_at": time.time(), "data": data})
                return data
            except Exception:
                if self.allow_stale_on_error:
                    stale = self._read()
                    if stale and "data" in stale:
                        return stale["data"]
                raise

    # --- internals ---
    def _is_fresh(self, payload: Optional[dict]) -> bool:
        if not payload:
            return False
        t = payload.get("generated_at")
        return isinstance(t, (int, float)) and (time.time() - float(t) < self.ttl_sec)

    def _read(self) -> Optional[dict]:
        try:
            with open(self.cache_path, "rb") as f:
                return pickle.load(f)
        except FileNotFoundError:
            return None
        except Exception:
            return None  # corrupt? treat as miss

    def _write(self, payload: dict) -> None:
        dir_ = os.path.dirname(self.cache_path)
        fd, tmp_path = tempfile.mkstemp(prefix=".tmp.", dir=dir_)
        try:
            with os.fdopen(fd, "wb") as f:
                pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, self.cache_path)  # atomic on POSIX
        finally:
            try:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
            except Exception:
                pass


class _exclusive_lock:
    """Exclusive flock across processes (Linux/Unix)."""

    def __init__(self, path: str):
        self._path = path
        self._fd = None

    def __enter__(self):
        self._fd = open(self._path, "a+", encoding="utf-8")
        fcntl.flock(self._fd.fileno(), fcntl.LOCK_EX)
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            fcntl.flock(self._fd.fileno(), fcntl.LOCK_UN)
        finally:
            self._fd.close()
