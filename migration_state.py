"""
Persistent migration progress state for resume-after-connection-loss.
- JSON file at .cache/migration_state.json (project root).
- Atomic writes: write to temp file, then os.replace (cross-platform).
- Thread-safe for single migration run.
"""

import json
import os
import threading
import time
from pathlib import Path


def get_state_path():
    """State file path: project_root/.cache/migration_state.json"""
    cache_root = os.environ.get("AOS_MIGRATION_STATE_DIR")
    if cache_root:
        cache_dir = Path(cache_root).expanduser().resolve()
        cache_dir.mkdir(parents=True, exist_ok=True)
        return cache_dir / "migration_state.json"

    root = os.environ.get("AOS_MIGRATION_PROJECT_ROOT")
    if root:
        base = Path(root).resolve()
    else:
        base = Path(__file__).resolve().parent
    cache_dir = base / ".cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / "migration_state.json"


class MigrationState:
    """
    Tracks progress per migration step. Load on startup/retry; save after each checkpoint.
    - update(step_key, **kwargs): merge kwargs into step state and save.
    - get(step_key, key, default=None): get a value from step state.
    - is_completed(step_key): True if status == "completed".
    - clear_step(step_key): remove step state (call when step completes successfully).
    - clear_all(): remove all state (call when starting a brand new run, not retry).
    """

    def __init__(self, path=None):
        self._path = path or get_state_path()
        self._lock = threading.Lock()
        self._data = self._load()

    def _load(self):
        try:
            if self._path.exists():
                with open(self._path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
        return {}

    def _save(self):
        """Atomic write: temp file then os.replace."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(f"{self._path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
        payload = json.dumps(self._data, indent=2)
        max_attempts = 10

        for attempt in range(max_attempts):
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    f.write(payload)
                    f.flush()
                    os.fsync(f.fileno())
                os.replace(tmp, self._path)
                return
            except OSError as e:
                if tmp.exists():
                    try:
                        tmp.unlink()
                    except OSError:
                        pass

                win_err = getattr(e, "winerror", None)
                transient_windows_lock = win_err in (5, 32)
                if transient_windows_lock and attempt < max_attempts - 1:
                    # Typical on Windows synced folders (Dropbox/OneDrive/AV): brief file lock.
                    time.sleep(0.15 * (attempt + 1))
                    continue
                raise

    def update(self, step_key, **kwargs):
        """Merge kwargs into step state and persist."""
        with self._lock:
            if step_key not in self._data:
                self._data[step_key] = {}
            self._data[step_key].update(kwargs)
            self._save()

    def get(self, step_key, key, default=None):
        """Get a value from a step's state."""
        with self._lock:
            step = self._data.get(step_key)
            if step is None:
                return default
            return step.get(key, default)

    def get_step(self, step_key):
        """Return full state for a step (dict)."""
        with self._lock:
            return dict(self._data.get(step_key, {}))

    def is_completed(self, step_key):
        """True if step status is 'completed'."""
        return self.get(step_key, "status") == "completed"

    def clear_step(self, step_key):
        """Remove step state (call when step completes successfully)."""
        with self._lock:
            if step_key in self._data:
                del self._data[step_key]
                self._save()

    def clear_all(self):
        """Remove all state (call when user starts a completely new run)."""
        with self._lock:
            self._data = {}
            self._save()
