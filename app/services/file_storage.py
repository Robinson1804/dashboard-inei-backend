from __future__ import annotations

import re
import uuid
from datetime import datetime
from pathlib import Path


def _sanitize_filename(filename: str) -> str:
    """Return filename with spaces replaced by underscores and special chars removed.

    Args:
        filename: Original filename string.

    Returns:
        Sanitized filename safe for filesystem storage.
    """
    name = filename.replace(" ", "_")
    name = re.sub(r"[^\w.\-]", "", name)
    return name


def save_upload(
    raw_bytes: bytes,
    filename: str,
    uploads_dir: Path,
    username: str = "anonymous",
) -> Path:
    """Save raw bytes to a date-and-user-partitioned subdirectory.

    The destination path follows the pattern::

        uploads_dir/{year}/{month:02d}/{username}/{uuid4}_{sanitized_filename}

    Args:
        raw_bytes: File contents to persist.
        filename: Original filename supplied by the uploader.
        uploads_dir: Root directory for all uploaded files.
        username: Username of the uploader (used as subfolder).

    Returns:
        Absolute Path to the saved file.
    """
    now = datetime.now()
    safe_user = _sanitize_filename(username) or "anonymous"
    dest_dir = uploads_dir / str(now.year) / f"{now.month:02d}" / safe_user
    dest_dir.mkdir(parents=True, exist_ok=True)

    safe_name = _sanitize_filename(filename)
    dest_path = dest_dir / f"{uuid.uuid4()}_{safe_name}"
    dest_path.write_bytes(raw_bytes)
    return dest_path


def get_upload_relative_path(full_path: Path, uploads_dir: Path) -> str:
    """Return the path of full_path relative to uploads_dir as a forward-slash string.

    Args:
        full_path: Absolute path returned by :func:`save_upload`.
        uploads_dir: Root directory used when saving the file.

    Returns:
        Relative path string suitable for storing in the database,
        e.g. ``"2026/02/abc123_file.xlsx"``.
    """
    return full_path.relative_to(uploads_dir).as_posix()
