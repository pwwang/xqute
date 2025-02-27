"""Provides the DualPath class and MountedPath class.

It is used to represent paths of jobs and it is useful when a job is running in a
remote system (a VM, a container, etc.), where we need to mounted the paths into
the remote system.

The DualPath class is a wrapper around the Path class, which provides two paths:
- The path that is used in the framework, which should always be accessible to this
    framework, either local or cloud.
- The mounted path, which is the path that is used in the remote system. It should
    be mounted to the remote system, so that the remote system can access the files
    in the path.
- The mounted path is a subclass of Path, so that it can be used as a Path object.
- The mounted path has a spec attribute, which is the path that is used in the
    framework. It is used to get the path that is used in the framework.
"""

from __future__ import annotations

import os
from pathlib import Path, WindowsPath, PosixPath
from typing import Any, Union

from yunpath import AnyPath, CloudPath

PathType = Union[Path, CloudPath, "DualPath", "MountedPath"]


class MountedPath(WindowsPath if os.name == "nt" else PosixPath):  # type: ignore[misc]
    """A class to represent a mounted path, with a spec attribute to retrieve the
    path that is used in the framework (specified by the user).
    """
    spec: Path | CloudPath | None


class DualPath:
    """A class to represent a path with a mounted path"""

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
            self.mounted = MountedPath(mounted)
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
