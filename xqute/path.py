"""Provides the SpecPath and MountedPath classes.

It is used to represent paths of jobs and it is useful when a job is running in a
remote system (a VM, a container, etc.), where we need to mount the paths into
the remote system (MountedPath).

But in the system where this framework is running, we need to use the paths
(specified directly) that are used in the framework, where we also need to carry
the information of the mounted path (SpecPath).
"""

from __future__ import annotations

import sys
from abc import ABC
from pathlib import Path
from typing import Any

from yunpath import AnyPath, CloudPath, GSPath, AzureBlobPath, S3Path

LocalPath = type(Path())

__all__ = ["SpecPath", "MountedPath"]


class MountedPath(ABC):
    """A router class to instantiate the correct path based on the path type
    for the mounted path.
    """

    def __new__(
        cls,
        path: str | Path | CloudPath,
        spec: str | Path | CloudPath | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> MountedLocalPath | MountedCloudPath:

        if cls is MountedPath:
            path = AnyPath(path)
            if isinstance(path, GSPath):
                mounted_class = MountedGSPath
            elif isinstance(path, AzureBlobPath):  # pragma: no cover
                mounted_class = MountedAzureBlobPath
            elif isinstance(path, S3Path):  # pragma: no cover
                mounted_class = MountedS3Path
            else:
                mounted_class = MountedLocalPath

            return mounted_class.__new__(mounted_class, path, spec, *args, **kwargs)

        return super().__new__(cls)  # pragma: no cover

    @property
    def spec(self) -> SpecPath:
        return SpecPath(self._spec, mounted=self)

    def is_mounted(self) -> bool:
        # Direct string comparison instead of using equality operator
        return str(self._spec) != str(self)

    def __repr__(self):
        # Check if spec is different by string comparison rather than using is_mounted()
        if self.is_mounted():
            return f"{type(self).__name__}('{self}', spec='{self._spec}')"
        else:
            return f"{type(self).__name__}('{self}')"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, (Path, CloudPath)):
            return False

        if isinstance(other, MountedPath):
            return str(self) == str(other) and str(self.spec) == str(other.spec)

        return str(self) == str(other)


class MountedLocalPath(MountedPath, LocalPath):
    """A class to represent a mounted local path"""

    def __new__(
        cls,
        path: str | Path,
        spec: str | Path | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        if sys.version_info >= (3, 12):
            obj = object.__new__(cls)
        else:  # pragma: no cover
            obj = cls._from_parts((path, *args))

        spec = spec or obj
        if isinstance(spec, (Path, CloudPath)):
            obj._spec = spec
        else:
            obj._spec = AnyPath(spec)

        return obj

    def __init__(
        self,
        path: str | Path,
        spec: str | Path | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        if sys.version_info >= (3, 12):
            # For python 3.9, object initalized by ._from_parts()
            LocalPath.__init__(self, path, *args, **kwargs)

    def with_segments(self, *pathsegments) -> MountedPath:
        if sys.version_info >= (3, 12):
            new_path = LocalPath(*pathsegments)
            pathsegments = [str(p) for p in pathsegments]
            new_spec = AnyPath(self._spec).with_segments(*pathsegments)

            return MountedPath(new_path, spec=new_spec)

        raise NotImplementedError(  # pragma: no cover
            "'with_segments' needs Python 3.10 or higher"
        )

    def with_name(self, name):
        new_path = LocalPath.with_name(self, name)
        new_spec = AnyPath(self._spec).with_name(name)

        return MountedPath(new_path, spec=new_spec)

    def with_suffix(self, suffix):
        new_path = LocalPath.with_suffix(self, suffix)
        new_spec = AnyPath(self._spec).with_suffix(suffix)

        return MountedPath(new_path, spec=new_spec)

    def joinpath(self, *pathsegments) -> MountedPath:
        new_path = LocalPath.joinpath(self, *pathsegments)
        new_spec = AnyPath(self._spec).joinpath(*pathsegments)

        return MountedPath(new_path, spec=new_spec)

    def __truediv__(self, key):
        # it was not implemented with .with_segments()
        return self.joinpath(key)

    @property
    def parent(self):
        new_path = LocalPath.parent.fget(self)
        new_spec = AnyPath(self._spec).parent

        return MountedPath(new_path, spec=new_spec)


class MountedCloudPath(MountedPath, CloudPath):
    """A class to represent a mounted cloud path"""

    def __new__(
        cls,
        path: str | Path | CloudPath,
        spec: str | Path | CloudPath | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        obj = object.__new__(cls)
        spec = spec or obj
        if isinstance(spec, (Path, CloudPath)):
            obj._spec = spec
        else:
            obj._spec = AnyPath(spec)

        return obj

    def __init__(
        self,
        path: str | Path | CloudPath,
        spec: str | Path | CloudPath | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(path, *args, **kwargs)

    def __truediv__(self, other):
        # it was not implemented with .with_segments()
        out = CloudPath.joinpath(self, other)
        spec = AnyPath(self._spec).joinpath(other)
        return MountedPath(out, spec=spec)

    def with_name(self, name):
        out = CloudPath.with_name(self, name)
        spec = AnyPath(self._spec).with_name(name)
        return MountedPath(out, spec=spec)

    def with_suffix(self, suffix):
        out = CloudPath.with_suffix(self, suffix)
        spec = AnyPath(self._spec).with_suffix(suffix)
        return MountedPath(out, spec=spec)

    def with_segments(self, *pathsegments):
        out = CloudPath.with_segments(self, *pathsegments)
        spec = AnyPath(self._spec).with_segments(*pathsegments)
        return MountedPath(out, spec=spec)

    def with_stem(self, stem):
        out = CloudPath.with_stem(self, stem)
        spec = AnyPath(self._spec).with_stem(stem)
        return MountedPath(out, spec=spec)

    def joinpath(self, *pathsegments):
        out = CloudPath.joinpath(self, *pathsegments)
        spec = AnyPath(self._spec).joinpath(*pathsegments)
        return MountedPath(out, spec=spec)

    @property
    def parent(self):
        out = CloudPath.parent.fget(self)
        spec = AnyPath(self._spec).parent
        return MountedPath(out, spec=spec)


class MountedGSPath(MountedCloudPath, GSPath):
    """A class to represent a mounted Google Cloud Storage path"""


class MountedAzureBlobPath(MountedCloudPath, AzureBlobPath):
    """A class to represent a mounted Azure Blob Storage path"""


class MountedS3Path(MountedCloudPath, S3Path):
    """A class to represent a mounted Amazon S3 path"""


class SpecPath(ABC):
    """A router class to instantiate the correct path based on the path type
    for the spec path.
    """

    def __new__(
        cls,
        path: str | Path | CloudPath,
        mounted: str | Path | CloudPath | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> SpecLocalPath | SpecCloudPath:
        if cls is SpecPath:
            path = AnyPath(path)
            if isinstance(path, GSPath):
                spec_class = SpecGSPath
            elif isinstance(path, AzureBlobPath):  # pragma: no cover
                spec_class = SpecAzureBlobPath
            elif isinstance(path, S3Path):  # pragma: no cover
                spec_class = SpecS3Path
            else:
                spec_class = SpecLocalPath

            return spec_class.__new__(spec_class, path, mounted, *args, **kwargs)

        return super().__new__(cls)  # pragma: no cover

    @property
    def mounted(self) -> MountedPath:
        # Make sure we handle the case where _mounted might not be set
        return MountedPath(self._mounted, spec=self)

    def __repr__(self) -> str:
        if self.mounted.is_mounted():
            return f"{type(self).__name__}('{self}', mounted='{self._mounted}')"
        else:
            return f"{type(self).__name__}('{self}')"

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, (Path, CloudPath)):
            return False

        if isinstance(other, SpecPath):
            return str(self) == str(other) and str(self.mounted) == str(other.mounted)

        return str(self) == str(other)


class SpecLocalPath(SpecPath, LocalPath):
    """A class to represent a spec local path"""

    def __new__(
        cls,
        path: str | Path,
        mounted: str | Path | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        if sys.version_info >= (3, 12):
            obj = object.__new__(cls)
        else:  # pragma: no cover
            obj = cls._from_parts((path, *args))

        mounted = mounted or obj
        if isinstance(mounted, (Path, CloudPath)):
            obj._mounted = mounted
        else:
            obj._mounted = AnyPath(mounted)

        return obj

    def __init__(
        self,
        path: str | Path,
        mounted: str | Path | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        if sys.version_info >= (3, 12):
            # For python 3.9, object initalized by ._from_parts()
            LocalPath.__init__(self, path, *args, **kwargs)

    def with_segments(self, *pathsegments) -> SpecPath:
        new_path = LocalPath(*pathsegments)
        pathsegments = [str(p) for p in pathsegments]
        new_mounted = AnyPath(self._mounted).with_segments(*pathsegments)

        return SpecPath(new_path, mounted=new_mounted)

    def with_name(self, name) -> SpecPath:
        new_path = LocalPath.with_name(self, name)
        new_mounted = AnyPath(self._mounted).with_name(name)

        return SpecPath(new_path, mounted=new_mounted)

    def with_suffix(self, suffix) -> SpecPath:
        new_path = LocalPath.with_suffix(self, suffix)
        new_mounted = AnyPath(self._mounted).with_suffix(suffix)

        return SpecPath(new_path, mounted=new_mounted)

    def with_stem(self, stem) -> SpecPath:
        new_path = LocalPath.with_stem(self, stem)
        new_mounted = AnyPath(self._mounted).with_stem(stem)

        return SpecPath(new_path, mounted=new_mounted)

    def joinpath(self, *pathsegments) -> SpecPath:
        new_path = LocalPath.joinpath(self, *pathsegments)
        new_mounted = AnyPath(self._mounted).joinpath(*pathsegments)

        return SpecPath(new_path, mounted=new_mounted)

    def __truediv__(self, key):
        # it was not implemented with .with_segments()
        return self.joinpath(key)

    @property
    def parent(self) -> SpecPath:
        new_path = LocalPath.parent.fget(self)
        new_mounted = AnyPath(self._mounted).parent

        return SpecPath(new_path, mounted=new_mounted)


class SpecCloudPath(SpecPath, CloudPath):
    """A class to represent a spec cloud path"""

    def __new__(
        cls,
        path: str | Path,
        mounted: str | Path | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        # Fix the debugging print statements which could cause confusion
        obj = object.__new__(cls)
        mounted = mounted or obj
        if isinstance(mounted, (Path, CloudPath)):
            obj._mounted = mounted
        else:
            obj._mounted = AnyPath(mounted)
        return obj

    def __init__(
        self,
        path: str | Path,
        mounted: str | Path | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        super().__init__(path, *args, **kwargs)

    def __truediv__(self, other):
        # Get the new path and mounted path
        out = CloudPath.joinpath(self, other)
        mounted = AnyPath(self._mounted).joinpath(other)

        return SpecPath(out, mounted=mounted)

    def with_name(self, name):
        out = CloudPath.with_name(self, name)
        mounted = AnyPath(self._mounted).with_name(name)
        return SpecPath(out, mounted=mounted)

    def with_suffix(self, suffix):
        out = CloudPath.with_suffix(self, suffix)
        mounted = AnyPath(self._mounted).with_suffix(suffix)
        return SpecPath(out, mounted=mounted)

    def with_segments(self, *pathsegments):
        out = CloudPath.with_segments(self, *pathsegments)
        mounted = AnyPath(self._mounted).with_segments(*pathsegments)
        return SpecPath(out, mounted=mounted)

    def with_stem(self, stem):
        out = CloudPath.with_stem(self, stem)
        mounted = AnyPath(self._mounted).with_stem(stem)
        return SpecPath(out, mounted=mounted)

    def joinpath(self, *pathsegments):
        out = CloudPath.joinpath(self, *pathsegments)
        mounted = AnyPath(self._mounted).joinpath(*pathsegments)
        return SpecPath(out, mounted=mounted)

    @property
    def parent(self):
        out = CloudPath.parent.fget(self)
        mounted = AnyPath(self._mounted).parent
        return SpecPath(out, mounted=mounted)


class SpecGSPath(SpecCloudPath, GSPath):
    """A class to represent a spec Google Cloud Storage path"""


class SpecAzureBlobPath(SpecCloudPath, AzureBlobPath):
    """A class to represent a spec Azure Blob Storage path"""


class SpecS3Path(SpecCloudPath, S3Path):
    """A class to represent a spec Amazon S3 path"""
