"""
Shared helpers for migration modules so asset paths work in both:
- Development (run from project root)
- Frozen app (PyInstaller one-file: assets live next to the exe, not in the bundle)

The launcher (wizard.py or main.py) must set AOS_MIGRATION_PROJECT_ROOT to the directory
that contains the 'assets' folder (exe dir when frozen, script dir when dev).
"""

import os
from pathlib import Path


def get_project_root() -> Path:
    """Directory containing the 'assets' folder. Same on all OSes when run from app or CLI."""
    root = os.environ.get("AOS_MIGRATION_PROJECT_ROOT")
    if root:
        return Path(root).resolve()
    # Fallback when running from source (this file is at project root)
    return Path(__file__).resolve().parent


def get_assets_dir() -> Path:
    """Path to the assets directory (project_root/assets)."""
    return get_project_root() / "assets"
