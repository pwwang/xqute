"""Utilities for xqute"""

from __future__ import annotations

import logging
import os
from pathlib import Path, WindowsPath, PosixPath
from typing import Any, Union, Tuple, List

from yunpath import AnyPath, CloudPath
from rich.logging import RichHandler

from .defaults import LOGGER_NAME

PathType = Union[Path, CloudPath, "DualPath", "PathWithSpec"]
CommandType = Union[str, Tuple[str], List[str]]


class DuplicateFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.prev_msg = None

    def filter(self, record):
        message = record.getMessage()
        if message == self.prev_msg:
            return False
        self.prev_msg = message
        return True


logger = logging.getLogger(LOGGER_NAME)
logger.addHandler(RichHandler(show_path=False, omit_repeated_times=False))
logger.addFilter(DuplicateFilter())
logger.setLevel(logging.INFO)


class PathWithSpec(WindowsPath if os.name == "nt" else PosixPath):  # type: ignore[misc]
    # allow to use the `spec` attribute
    spec: Path | CloudPath | None


class DualPath:
    """When running job on remote systems, we need to know
    the specified or original path and the remote (mounted) path.

    The mounted path is the path that is used in the remote system.
    The as-is path is the path that is used in the local system.

    The mounted path should always be in a "local" path format.
    """

    def __init__(
        self,
        path: str | PathType,
        mounted: PathType | None = None,
    ):
        self.path = AnyPath(path)

        if mounted is None:
            mounted = path  # type: ignore

        mounted = AnyPath(mounted)
        if isinstance(mounted, Path):
            self.mounted = PathWithSpec(mounted)
        else:
            self.mounted = mounted  # type: ignore

        setattr(self.mounted, "spec", self.path)

    def __getattr__(self, item: str) -> Any:
        """Delegate the attribute to the path"""
        return getattr(self.path, item)

    def __truediv__(self, other: Any) -> DualPath:
        """Join the path"""
        return DualPath(self.path / other, self.mounted / other)

    def with_name(self, name: str) -> DualPath:
        """Change the name of the path"""
        return DualPath(self.path.with_name(name), self.mounted.with_name(name))

    def with_suffix(self, suffix: str) -> DualPath:
        """Change the suffix of the path"""
        return DualPath(self.path.with_suffix(suffix), self.mounted.with_suffix(suffix))

    def with_stem(self, stem: str) -> DualPath:
        """Change the stem of the path"""
        return DualPath(self.path.with_stem(stem), self.mounted.with_stem(stem))

    def __repr__(self) -> str:
        """repr of the dualpath"""
        return f"DualPath({str(self.path)!r}, mounted={str(self.mounted)!r})"

    def __str__(self) -> str:
        """str of the dualpath"""
        return str(self.path)

    def __eq__(self, other: Any) -> bool:
        """Check if the dualpath is equal to another"""
        if isinstance(other, DualPath):
            return self.path == other.path and self.mounted == other.mounted
        return self.path == other

    @property
    def parent(self) -> DualPath:
        """The parent of the path"""
        return DualPath(self.path.parent, self.mounted.parent)
