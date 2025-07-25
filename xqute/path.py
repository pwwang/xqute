"""Provides the SpecPath and MountedPath classes.

It is used to represent paths of jobs and it is useful when a job is running in a
remote system (a VM, a container, etc.), where we need to mount the paths into
the remote system (MountedPath).

But in the system where this framework is running, we need to use the paths
(specified directly) that are used in the framework, where we also need to carry
the information of the mounted path (SpecPath).

The module provides two main abstract base classes:
- `MountedPath`: Represents a path as it appears in the remote execution environment.
- `SpecPath`: Represents a path as it appears in the local environment where the
framework runs.

Both classes have implementations for local paths and various cloud storage paths,
including:
- Google Cloud Storage
- Azure Blob Storage
- Amazon S3

These classes maintain the relationship between the local and remote path
representations, allowing transparent path operations while preserving both path
contexts.
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

    This abstract base class serves as a factory that creates appropriate mounted path
    instances based on the input path type. It represents a path as it exists in a
    remote execution environment (e.g., container, VM) while maintaining a reference to
    the corresponding path in the local environment.

    Attributes:
        _spec: The corresponding path in the local environment (SpecPath).

    Examples:
        >>> # Create a mounted path with corresponding spec path
        >>> mounted_path = MountedPath(
        >>>   "/container/data/file.txt", spec="/local/data/file.txt"
        >>> )
        >>> str(mounted_path)
        '/container/data/file.txt'
        >>> str(mounted_path.spec)
        '/local/data/file.txt'

        >>> # Create a GCS mounted path
        >>> gs_path = MountedPath("gs://bucket/file.txt", spec="/local/file.txt")
        >>> type(gs_path)
        <class 'xqute.path.MountedGSPath'>
    """

    def __new__(  # type: ignore
        cls,
        path: str | Path | CloudPath,
        spec: str | Path | CloudPath | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> MountedLocalPath | MountedCloudPath:
        """Factory method to create the appropriate MountedPath subclass instance.

        Args:
            path: The path string or object representing the mounted path location.
            spec: The path string or object representing the corresponding spec path.
                If None, the mounted path itself will be used as the spec path.
            *args: Additional positional arguments passed to the path constructor.
            **kwargs: Additional keyword arguments passed to the path constructor.

        Returns:
            An instance of the appropriate MountedPath subclass based on the path type:
            - MountedGSPath for Google Cloud Storage paths
            - MountedAzureBlobPath for Azure Blob Storage paths
            - MountedS3Path for Amazon S3 paths
            - MountedLocalPath for local filesystem paths
        """

        if cls is MountedPath:
            path = AnyPath(path)  # type: ignore
            if isinstance(path, GSPath):
                mounted_class = MountedGSPath
            elif isinstance(path, AzureBlobPath):  # pragma: no cover
                mounted_class = MountedAzureBlobPath  # type: ignore
            elif isinstance(path, S3Path):  # pragma: no cover
                mounted_class = MountedS3Path  # type: ignore
            else:
                mounted_class = MountedLocalPath  # type: ignore

            return mounted_class.__new__(mounted_class, path, spec, *args, **kwargs)

        return super().__new__(cls)  # type: ignore # pragma: no cover

    @property
    def spec(self) -> SpecPath:
        """Get the corresponding spec path in the local environment.

        Returns:
            SpecPath: The path as it appears in the local environment.
        """
        return SpecPath(self._spec, mounted=self)  # type: ignore

    def is_mounted(self) -> bool:
        """Check if this path is actually mounted (different from spec path).

        Returns:
            bool: True if the mounted path is different from the spec path, False
            otherwise.
        """
        # Direct string comparison instead of using equality operator
        return str(self._spec) != str(self)

    def __repr__(self):
        """Generate a string representation of the MountedPath.

        Returns:
            str: A string showing the class name, path, and spec path (if different).
        """
        # Check if spec is different by string comparison rather than using is_mounted()
        if self.is_mounted():
            return f"{type(self).__name__}('{self}', spec='{self._spec}')"
        else:
            return f"{type(self).__name__}('{self}')"

    def __eq__(self, other: Any) -> bool:
        """Check equality with another path object.

        Two MountedPath objects are equal if they have the same path string
        and the same spec path string.

        Args:
            other: Another object to compare with.

        Returns:
            bool: True if the paths are equal, False otherwise.
        """
        if not isinstance(other, (Path, CloudPath)):
            return False

        if isinstance(other, MountedPath):
            return str(self) == str(other) and str(self.spec) == str(other.spec)

        return str(self) == str(other)

    def __hash__(self) -> int:
        """Generate a hash for the MountedPath.

        Returns:
            int: A hash value based on the path string and spec path string.
        """
        return hash((str(self), str(self.spec)))


class MountedLocalPath(MountedPath, LocalPath):  # type: ignore
    """A class to represent a mounted local path

    This class represents a path in a local filesystem as it appears in a remote
    execution environment, while maintaining a reference to its corresponding
    path in the framework's environment.

    Attributes:
        _spec: The corresponding path in the local environment.

    Examples:
        >>> mounted_path = MountedLocalPath("/container/data/file.txt",
        ...                               spec="/local/data/file.txt")
        >>> str(mounted_path)
        '/container/data/file.txt'
        >>> str(mounted_path.spec)
        '/local/data/file.txt'
        >>> mounted_path.name
        'file.txt'
    """

    def __new__(
        cls,
        path: str | Path,
        spec: str | Path | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        """Create a new MountedLocalPath instance.

        Args:
            path: The path string or object representing the mounted local path.
            spec: The path string or object representing the corresponding spec path.
                If None, the mounted path itself will be used as the spec path.
            *args: Additional positional arguments passed to the path constructor.
            **kwargs: Additional keyword arguments passed to the path constructor.

        Returns:
            A new MountedLocalPath instance.
        """
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
        """Initialize a MountedLocalPath instance.

        Args:
            path: The path string or object representing the mounted local path.
            spec: The path string or object representing the corresponding spec path.
                If None, the mounted path itself will be used as the spec path.
            *args: Additional positional arguments passed to the path constructor.
            **kwargs: Additional keyword arguments passed to the path constructor.
        """
        if sys.version_info >= (3, 12):
            # For python 3.9, object initalized by ._from_parts()
            LocalPath.__init__(self, path, *args, **kwargs)

    def with_segments(self, *pathsegments) -> MountedPath:
        """Create a new path by replacing all segments with the given segments.

        Args:
            *pathsegments: The path segments to use in the new path.

        Returns:
            MountedPath: A new mounted path with the specified segments.

        Raises:
            NotImplementedError: If Python version is lower than 3.10.
        """
        if sys.version_info >= (3, 12):
            new_path = LocalPath(*pathsegments)
            pathsegments = tuple(str(p) for p in pathsegments)
            new_spec = AnyPath(self._spec).with_segments(*pathsegments)

            return MountedPath(new_path, spec=new_spec)

        raise NotImplementedError(  # pragma: no cover
            "'with_segments' needs Python 3.10 or higher"
        )

    def with_name(self, name):
        """Return a new path with the name changed.

        Args:
            name: The new name for the path.

        Returns:
            MountedPath: A new mounted path with the name changed in both
                the mounted path and spec path.
        """
        new_path = LocalPath.with_name(self, name)
        new_spec = AnyPath(self._spec).with_name(name)

        return MountedPath(new_path, spec=new_spec)

    def with_suffix(self, suffix):
        """Return a new path with the suffix changed.

        Args:
            suffix: The new suffix for the path.

        Returns:
            MountedPath: A new mounted path with the suffix changed in both
                the mounted path and spec path.
        """
        new_path = LocalPath.with_suffix(self, suffix)
        new_spec = AnyPath(self._spec).with_suffix(suffix)

        return MountedPath(new_path, spec=new_spec)

    def joinpath(self, *pathsegments) -> MountedPath:
        """Join path components to this path.

        Args:
            *pathsegments: The path segments to append to this path.

        Returns:
            MountedPath: A new mounted path with the segments appended to both
                the mounted path and spec path.
        """
        new_path = LocalPath.joinpath(self, *pathsegments)
        new_spec = AnyPath(self._spec).joinpath(*pathsegments)

        return MountedPath(new_path, spec=new_spec)

    def __truediv__(self, key):
        """Implement the / operator for paths.

        Args:
            key: The path segment to append to this path.

        Returns:
            MountedPath: A new mounted path with the segment appended.
        """
        # it was not implemented with .with_segments()
        return self.joinpath(key)

    @property
    def parent(self):
        """Get the parent directory of this path.

        Returns:
            MountedPath: A new mounted path representing the parent directory
                of both the mounted path and spec path.
        """
        new_path = LocalPath.parent.fget(self)
        new_spec = AnyPath(self._spec).parent

        return MountedPath(new_path, spec=new_spec)


class MountedCloudPath(MountedPath, CloudPath):
    """A class to represent a mounted cloud path

    This class represents a cloud storage path as it appears in a remote
    execution environment, while maintaining a reference to its corresponding
    path in the framework's environment.

    Attributes:
        _spec: The corresponding path in the local environment.

    Examples:
        >>> mounted_path = MountedPath("gs://bucket/file.txt",
        ...                          spec="gs://local-bucket/file.txt")
        >>> str(mounted_path)
        'gs://bucket/file.txt'
        >>> str(mounted_path.spec)
        'gs://local-bucket/file.txt'
    """

    def __new__(
        cls,
        path: str | Path | CloudPath,
        spec: str | Path | CloudPath | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        """Create a new MountedCloudPath instance.

        Args:
            path: The path string or object representing the mounted cloud path.
            spec: The path string or object representing the corresponding spec path.
                If None, the mounted path itself will be used as the spec path.
            *args: Additional positional arguments passed to the path constructor.
            **kwargs: Additional keyword arguments passed to the path constructor.

        Returns:
            A new MountedCloudPath instance.
        """
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
        """Initialize a MountedCloudPath instance.

        Args:
            path: The path string or object representing the mounted cloud path.
            spec: The path string or object representing the corresponding spec path.
                If None, the mounted path itself will be used as the spec path.
            *args: Additional positional arguments passed to the path constructor.
            **kwargs: Additional keyword arguments passed to the path constructor.
        """
        super().__init__(path, *args, **kwargs)  # type: ignore

    def __truediv__(self, other):
        """Implement the / operator for cloud paths.

        Args:
            other: The path segment to append to this path.

        Returns:
            MountedPath: A new mounted cloud path with the segment appended.
        """
        # it was not implemented with .with_segments()
        out = CloudPath.joinpath(self, other)
        spec = AnyPath(self._spec).joinpath(other)
        return MountedPath(out, spec=spec)

    def with_name(self, name):
        """Return a new path with the name changed.

        Args:
            name: The new name for the path.

        Returns:
            MountedPath: A new mounted path with the name changed in both
                the mounted path and spec path.
        """
        out = CloudPath.with_name(self, name)
        spec = AnyPath(self._spec).with_name(name)
        return MountedPath(out, spec=spec)

    def with_suffix(self, suffix):
        """Return a new path with the suffix changed.

        Args:
            suffix: The new suffix for the path.

        Returns:
            MountedPath: A new mounted path with the suffix changed in both
                the mounted path and spec path.
        """
        out = CloudPath.with_suffix(self, suffix)
        spec = AnyPath(self._spec).with_suffix(suffix)
        return MountedPath(out, spec=spec)

    def with_segments(self, *pathsegments):
        """Create a new path by replacing all segments with the given segments.

        Args:
            *pathsegments: The path segments to use in the new path.

        Returns:
            MountedPath: A new mounted path with the specified segments.
        """
        out = CloudPath.with_segments(self, *pathsegments)
        spec = AnyPath(self._spec).with_segments(*pathsegments)
        return MountedPath(out, spec=spec)

    def with_stem(self, stem):
        """Return a new path with the stem changed.

        The stem is the filename without the suffix.

        Args:
            stem: The new stem for the path.

        Returns:
            MountedPath: A new mounted path with the stem changed in both
                the mounted path and spec path.
        """
        out = CloudPath.with_stem(self, stem)
        spec = AnyPath(self._spec).with_stem(stem)
        return MountedPath(out, spec=spec)

    def joinpath(self, *pathsegments):
        """Join path components to this path.

        Args:
            *pathsegments: The path segments to append to this path.

        Returns:
            MountedPath: A new mounted path with the segments appended to both
                the mounted path and spec path.
        """
        out = CloudPath.joinpath(self, *pathsegments)
        spec = AnyPath(self._spec).joinpath(*pathsegments)
        return MountedPath(out, spec=spec)

    @property
    def parent(self):
        """Get the parent directory of this path.

        Returns:
            MountedPath: A new mounted path representing the parent directory
                of both the mounted path and spec path.
        """
        out = CloudPath.parent.fget(self)
        spec = AnyPath(self._spec).parent
        return MountedPath(out, spec=spec)


class MountedGSPath(MountedCloudPath, GSPath):
    """A class to represent a mounted Google Cloud Storage path

    This class represents a Google Cloud Storage path as it appears in a remote
    execution environment, while maintaining a reference to its corresponding
    path in the framework's environment.

    Examples:
        >>> mounted_path = MountedPath("gs://bucket/file.txt",
        ...                          spec="gs://local-bucket/file.txt")
        >>> isinstance(mounted_path, MountedGSPath)
        True
    """


class MountedAzureBlobPath(MountedCloudPath, AzureBlobPath):
    """A class to represent a mounted Azure Blob Storage path

    This class represents an Azure Blob Storage path as it appears in a remote
    execution environment, while maintaining a reference to its corresponding
    path in the framework's environment.

    Examples:
        >>> mounted_path = MountedPath("az://container/blob",
        ...                          spec="az://local-container/blob")
        >>> isinstance(mounted_path, MountedAzureBlobPath)
        True
    """


class MountedS3Path(MountedCloudPath, S3Path):
    """A class to represent a mounted Amazon S3 path

    This class represents an Amazon S3 path as it appears in a remote
    execution environment, while maintaining a reference to its corresponding
    path in the framework's environment.

    Examples:
        >>> mounted_path = MountedPath("s3://bucket/key",
        ...                          spec="s3://local-bucket/key")
        >>> isinstance(mounted_path, MountedS3Path)
        True
    """


class SpecPath(ABC):
    """A router class to instantiate the correct path based on the path type
    for the spec path.

    This abstract base class serves as a factory that creates appropriate spec path
    instances based on the input path type. It represents a path in the local
    environment where the framework runs, while maintaining a reference to the
    corresponding path in the remote execution environment.

    Attributes:
        _mounted: The corresponding path in the remote execution environment.

    Examples:
        >>> # Create a spec path with corresponding mounted path
        >>> spec_path = SpecPath(
        >>>   "/local/data/file.txt", mounted="/container/data/file.txt"
        >>> )
        >>> str(spec_path)
        '/local/data/file.txt'
        >>> str(spec_path.mounted)
        '/container/data/file.txt'

        >>> # Create a GCS spec path
        >>> gs_path = SpecPath(
        >>>   "gs://bucket/file.txt", mounted="gs://container-bucket/file.txt"
        >>> )
        >>> type(gs_path)
        <class 'xqute.path.SpecGSPath'>
    """

    def __new__(  # type: ignore
        cls,
        path: str | Path | CloudPath,
        mounted: str | Path | CloudPath | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> SpecLocalPath | SpecCloudPath:
        """Factory method to create the appropriate SpecPath subclass instance.

        Args:
            path: The path string or object representing the spec path.
            mounted: The path string or object representing the corresponding mounted
                path. If None, the spec path itself will be used as the mounted path.
            *args: Additional positional arguments passed to the path constructor.
            **kwargs: Additional keyword arguments passed to the path constructor.

        Returns:
            An instance of the appropriate SpecPath subclass based on the path type:
            - SpecGSPath for Google Cloud Storage paths
            - SpecAzureBlobPath for Azure Blob Storage paths
            - SpecS3Path for Amazon S3 paths
            - SpecLocalPath for local filesystem paths
        """
        if cls is SpecPath:
            path = AnyPath(path)  # type: ignore
            if isinstance(path, GSPath):
                spec_class = SpecGSPath
            elif isinstance(path, AzureBlobPath):  # pragma: no cover
                spec_class = SpecAzureBlobPath  # type: ignore
            elif isinstance(path, S3Path):  # pragma: no cover
                spec_class = SpecS3Path  # type: ignore
            else:
                spec_class = SpecLocalPath

            return spec_class.__new__(
                spec_class, path, mounted, *args, **kwargs  # type: ignore
            )

        return super().__new__(cls)  # type: ignore # pragma: no cover

    @property
    def mounted(self) -> MountedPath:
        """Get the corresponding mounted path in the remote environment.

        Returns:
            MountedPath: The path as it appears in the remote execution environment.
        """
        # Make sure we handle the case where _mounted might not be set
        return MountedPath(self._mounted, spec=self)  # type: ignore

    def __repr__(self) -> str:
        """Generate a string representation of the SpecPath.

        Returns:
            str: A string showing the class name, path, and mounted path (if different).
        """
        if self.mounted.is_mounted():
            return f"{type(self).__name__}('{self}', mounted='{self._mounted}')"
        else:
            return f"{type(self).__name__}('{self}')"

    def __eq__(self, other: Any) -> bool:
        """Check equality with another path object.

        Two SpecPath objects are equal if they have the same path string
        and the same mounted path string.

        Args:
            other: Another object to compare with.

        Returns:
            bool: True if the paths are equal, False otherwise.
        """
        if not isinstance(other, (Path, CloudPath)):
            return False

        if isinstance(other, SpecPath):
            return str(self) == str(other) and str(self.mounted) == str(other.mounted)

        return str(self) == str(other)

    def __hash__(self) -> int:
        """Generate a hash for the SpecPath.

        Returns:
            int: A hash value based on the path string and mounted path string.
        """
        return hash((str(self), str(self.mounted)))


class SpecLocalPath(SpecPath, LocalPath):  # type: ignore
    """A class to represent a spec local path

    This class represents a path in the local filesystem as it appears in the
    framework's environment, while maintaining a reference to its corresponding
    path in the remote execution environment.

    Attributes:
        _mounted: The corresponding path in the remote execution environment.

    Examples:
        >>> spec_path = SpecLocalPath("/local/data/file.txt",
        ...                         mounted="/container/data/file.txt")
        >>> str(spec_path)
        '/local/data/file.txt'
        >>> str(spec_path.mounted)
        '/container/data/file.txt'
        >>> spec_path.name
        'file.txt'
    """

    def __new__(
        cls,
        path: str | Path,
        mounted: str | Path | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        """Create a new SpecLocalPath instance.

        Args:
            path: The path string or object representing the spec local path.
            mounted: The path string or object representing the corresponding mounted
                path. If None, the spec path itself will be used as the mounted path.
            *args: Additional positional arguments passed to the path constructor.
            **kwargs: Additional keyword arguments passed to the path constructor.

        Returns:
            A new SpecLocalPath instance.
        """
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
        """Initialize a SpecLocalPath instance.

        Args:
            path: The path string or object representing the spec local path.
            mounted: The path string or object representing the corresponding mounted
                path. If None, the spec path itself will be used as the mounted path.
            *args: Additional positional arguments passed to the path constructor.
            **kwargs: Additional keyword arguments passed to the path constructor.
        """
        if sys.version_info >= (3, 12):
            # For python 3.9, object initalized by ._from_parts()
            LocalPath.__init__(self, path, *args, **kwargs)

    def with_segments(self, *pathsegments) -> SpecPath:
        """Create a new path by replacing all segments with the given segments.

        Args:
            *pathsegments: The path segments to use in the new path.

        Returns:
            SpecPath: A new spec path with the specified segments.
        """
        new_path = LocalPath(*pathsegments)
        pathsegments = [str(p) for p in pathsegments]
        new_mounted = AnyPath(self._mounted).with_segments(*pathsegments)

        return SpecPath(new_path, mounted=new_mounted)

    def with_name(self, name) -> SpecPath:
        """Return a new path with the name changed.

        Args:
            name: The new name for the path.

        Returns:
            SpecPath: A new spec path with the name changed in both
                the spec path and mounted path.
        """
        new_path = LocalPath.with_name(self, name)
        new_mounted = AnyPath(self._mounted).with_name(name)

        return SpecPath(new_path, mounted=new_mounted)

    def with_suffix(self, suffix) -> SpecPath:
        """Return a new path with the suffix changed.

        Args:
            suffix: The new suffix for the path.

        Returns:
            SpecPath: A new spec path with the suffix changed in both
                the spec path and mounted path.
        """
        new_path = LocalPath.with_suffix(self, suffix)
        new_mounted = AnyPath(self._mounted).with_suffix(suffix)

        return SpecPath(new_path, mounted=new_mounted)

    def with_stem(self, stem) -> SpecPath:
        """Return a new path with the stem changed.

        The stem is the filename without the suffix.

        Args:
            stem: The new stem for the path.

        Returns:
            SpecPath: A new spec path with the stem changed in both
                the spec path and mounted path.
        """
        new_path = LocalPath.with_stem(self, stem)
        new_mounted = AnyPath(self._mounted).with_stem(stem)

        return SpecPath(new_path, mounted=new_mounted)

    def joinpath(self, *pathsegments) -> SpecPath:
        """Join path components to this path.

        Args:
            *pathsegments: The path segments to append to this path.

        Returns:
            SpecPath: A new spec path with the segments appended to both
                the spec path and mounted path.
        """
        new_path = LocalPath.joinpath(self, *pathsegments)
        new_mounted = AnyPath(self._mounted).joinpath(*pathsegments)

        return SpecPath(new_path, mounted=new_mounted)

    def __truediv__(self, key):
        """Implement the / operator for paths.

        Args:
            key: The path segment to append to this path.

        Returns:
            SpecPath: A new spec path with the segment appended.
        """
        # it was not implemented with .with_segments()
        return self.joinpath(key)

    @property
    def parent(self) -> SpecPath:
        """Get the parent directory of this path.

        Returns:
            SpecPath: A new spec path representing the parent directory
                of both the spec path and mounted path.
        """
        new_path = LocalPath.parent.fget(self)
        new_mounted = AnyPath(self._mounted).parent

        return SpecPath(new_path, mounted=new_mounted)


class SpecCloudPath(SpecPath, CloudPath):
    """A class to represent a spec cloud path

    This class represents a cloud storage path as it appears in the local
    environment where the framework runs, while maintaining a reference to its
    corresponding path in the remote execution environment.

    Attributes:
        _mounted: The corresponding path in the remote execution environment.

    Examples:
        >>> spec_path = SpecPath("gs://bucket/file.txt",
        ...                    mounted="gs://container-bucket/file.txt")
        >>> str(spec_path)
        'gs://bucket/file.txt'
        >>> str(spec_path.mounted)
        'gs://container-bucket/file.txt'
    """

    def __new__(
        cls,
        path: str | Path,
        mounted: str | Path | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        """Create a new SpecCloudPath instance.

        Args:
            path: The path string or object representing the spec cloud path.
            mounted: The path string or object representing the corresponding mounted
                path. If None, the spec path itself will be used as the mounted path.
            *args: Additional positional arguments passed to the path constructor.
            **kwargs: Additional keyword arguments passed to the path constructor.

        Returns:
            A new SpecCloudPath instance.
        """
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
        """Initialize a SpecCloudPath instance.

        Args:
            path: The path string or object representing the spec cloud path.
            mounted: The path string or object representing the corresponding mounted
                path. If None, the spec path itself will be used as the mounted path.
            *args: Additional positional arguments passed to the path constructor.
            **kwargs: Additional keyword arguments passed to the path constructor.
        """
        super().__init__(path, *args, **kwargs)  # type: ignore[arg-type]

    def __truediv__(self, other):
        """Implement the / operator for cloud paths.

        Args:
            other: The path segment to append to this path.

        Returns:
            SpecPath: A new spec cloud path with the segment appended.
        """
        # Get the new path and mounted path
        out = CloudPath.joinpath(self, other)
        mounted = AnyPath(self._mounted).joinpath(other)

        return SpecPath(out, mounted=mounted)

    def with_name(self, name):
        """Return a new path with the name changed.

        Args:
            name: The new name for the path.

        Returns:
            SpecPath: A new spec path with the name changed in both
                the spec path and mounted path.
        """
        out = CloudPath.with_name(self, name)
        mounted = AnyPath(self._mounted).with_name(name)
        return SpecPath(out, mounted=mounted)

    def with_suffix(self, suffix):
        """Return a new path with the suffix changed.

        Args:
            suffix: The new suffix for the path.

        Returns:
            SpecPath: A new spec path with the suffix changed in both
                the spec path and mounted path.
        """
        out = CloudPath.with_suffix(self, suffix)
        mounted = AnyPath(self._mounted).with_suffix(suffix)
        return SpecPath(out, mounted=mounted)

    def with_segments(self, *pathsegments):
        """Create a new path by replacing all segments with the given segments.

        Args:
            *pathsegments: The path segments to use in the new path.

        Returns:
            SpecPath: A new spec path with the specified segments.
        """
        out = CloudPath.with_segments(self, *pathsegments)
        mounted = AnyPath(self._mounted).with_segments(*pathsegments)
        return SpecPath(out, mounted=mounted)

    def with_stem(self, stem):
        """Return a new path with the stem changed.

        The stem is the filename without the suffix.

        Args:
            stem: The new stem for the path.

        Returns:
            SpecPath: A new spec path with the stem changed in both
                the spec path and mounted path.
        """
        out = CloudPath.with_stem(self, stem)
        mounted = AnyPath(self._mounted).with_stem(stem)
        return SpecPath(out, mounted=mounted)

    def joinpath(self, *pathsegments):
        """Join path components to this path.

        Args:
            *pathsegments: The path segments to append to this path.

        Returns:
            SpecPath: A new spec path with the segments appended to both
                the spec path and mounted path.
        """
        out = CloudPath.joinpath(self, *pathsegments)
        mounted = AnyPath(self._mounted).joinpath(*pathsegments)
        return SpecPath(out, mounted=mounted)

    @property
    def parent(self):
        """Get the parent directory of this path.

        Returns:
            SpecPath: A new spec path representing the parent directory
                of both the spec path and mounted path.
        """
        out = CloudPath.parent.fget(self)
        mounted = AnyPath(self._mounted).parent
        return SpecPath(out, mounted=mounted)


class SpecGSPath(SpecCloudPath, GSPath):
    """A class to represent a spec Google Cloud Storage path

    This class represents a Google Cloud Storage path as it appears in the
    local environment where the framework runs, while maintaining a reference
    to its corresponding path in the remote execution environment.

    Examples:
        >>> spec_path = SpecPath("gs://bucket/file.txt",
        ...                    mounted="gs://container-bucket/file.txt")
        >>> isinstance(spec_path, SpecGSPath)
        True
    """


class SpecAzureBlobPath(SpecCloudPath, AzureBlobPath):
    """A class to represent a spec Azure Blob Storage path

    This class represents an Azure Blob Storage path as it appears in the
    local environment where the framework runs, while maintaining a reference
    to its corresponding path in the remote execution environment.

    Examples:
        >>> spec_path = SpecPath("az://container/blob",
        ...                    mounted="az://remote-container/blob")
        >>> isinstance(spec_path, SpecAzureBlobPath)
        True
    """


class SpecS3Path(SpecCloudPath, S3Path):
    """A class to represent a spec Amazon S3 path

    This class represents an Amazon S3 path as it appears in the
    local environment where the framework runs, while maintaining a reference
    to its corresponding path in the remote execution environment.

    Examples:
        >>> spec_path = SpecPath("s3://bucket/key",
        ...                    mounted="s3://remote-bucket/key")
        >>> isinstance(spec_path, SpecS3Path)
        True
    """
