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

from typing import Any

import os
from pathlib import Path
from panpath import PanPath, LocalPath, CloudPath, GSPath, AzurePath, S3Path

from .defaults import DEFAULT_CLOUD_FSPATH

__all__ = ["SpecPath", "MountedPath"]


class MountedPath(PanPath):
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

        >>> # Serialize and deserialize a mounted path
        >>> import pickle
        >>> mounted_path = MountedPath("/container/data/file.txt",
        ...                            spec="/local/data/file.txt")
        >>> serialized = pickle.dumps(mounted_path)
        >>> restored = pickle.loads(serialized)
        >>> str(restored) == str(mounted_path)
        True
        >>> str(restored.spec) == str(mounted_path.spec)
        True
    """

    def __new__(  # type: ignore
        cls,
        path: str | Path,
        spec: str | Path | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> MountedPath:
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
            - MountedAzurePath for Azure Blob Storage paths
            - MountedS3Path for Amazon S3 paths
            - MountedLocalPath for local filesystem paths
        """

        if cls is MountedPath:
            path = PanPath(path)  # type: ignore
            if isinstance(path, GSPath):
                mounted_class = MountedGSPath
            elif isinstance(path, AzurePath):
                mounted_class = MountedAzurePath  # type: ignore
            elif isinstance(path, S3Path):
                mounted_class = MountedS3Path  # type: ignore
            else:
                mounted_class = MountedLocalPath  # type: ignore

            obj = mounted_class(path, *args, **kwargs)
            obj._spec = PanPath(spec) if spec is not None else obj
            return obj

        # Ensure the underlying Path initialization receives the path so
        # internal parts like `_parts` are populated on older Python versions.
        return super().__new__(cls, path, *args, **kwargs)  # type: ignore

    async def get_fspath(self) -> str:
        """Get the corresponding local filesystem path and copy from cloud.

        Returns:
            PanPath: The path as it appears in the local filesystem.
        """
        return self.__fspath__()

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
        if not isinstance(other, Path):
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

    def __reduce__(self):
        """Support for pickling and serialization.

        Returns a tuple of (callable, args, state) so that the
        underlying path is reconstructed from its string, and the
        spec relationship is restored via state.
        """
        return (type(self), (str(self),), {"_spec": str(self._spec)})

    def __setstate__(self, state: dict[str, Any]) -> None:
        """Restore internal state after unpickling."""
        spec_str = state.get("_spec")
        self._spec = PanPath(spec_str) if spec_str is not None else self

    def with_name(self, name):
        """Return a new path with the name changed.

        Args:
            name: The new name for the path.

        Returns:
            MountedPath: A new mounted path with the name changed in both
                the mounted path and spec path.
        """
        new_path = LocalPath.with_name(self, name)
        new_spec = PanPath(str(self._spec)).with_name(name)

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
        new_spec = PanPath(str(self._spec)).with_suffix(suffix)

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
        new_spec = PanPath(str(self._spec)).joinpath(*pathsegments)

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
        new_spec = PanPath(str(self._spec)).parent

        return MountedPath(new_path, spec=new_spec)


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


class MountedCloudPath(MountedPath, CloudPath):
    """A class to represent a mounted cloud path

    This class represents a cloud storage path as it appears in a remote
    execution environment, while maintaining a reference to its corresponding
    path in the framework's environment.

    Attributes:
        _spec: The corresponding path in the local environment.

    Examples:
        >>> mounted_path = MountedPath("gs://bucket/file.txt",
        ...    spec="gs://local-bucket/file.txt")
        >>> str(mounted_path)
        'gs://bucket/file.txt'
        >>> str(mounted_path.spec)
        'gs://local-bucket/file.txt'
    """

    def __fspath__(self) -> str:
        """Return the filesystem path representation.

        Returns:
            str: The filesystem path as a string.
        """
        cloud_fspath = os.getenv("XQUTE_CLOUD_FSPATH", DEFAULT_CLOUD_FSPATH)
        parts = [
            cloud_fspath,
            self.parts[0].replace(":", ""),
            *self.parts[1:],
        ]
        return os.path.join(*parts)

    async def get_fspath(self) -> str:
        """Get the corresponding local filesystem path and copy from cloud.

        Returns:
            PanPath: The path as it appears in the local filesystem.
        """
        p = PanPath(self.__fspath__())
        await p.parent.a_mkdir(parents=True, exist_ok=True)

        if await self.a_is_dir():
            await self.a_copytree(p)
        else:
            await self.a_copy(p)

        return str(p)


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


class MountedAzurePath(MountedCloudPath, AzurePath):
    """A class to represent a mounted Azure Blob Storage path

    This class represents an Azure Blob Storage path as it appears in a remote
    execution environment, while maintaining a reference to its corresponding
    path in the framework's environment.

    Examples:
        >>> mounted_path = MountedPath("az://container/blob",
        ...                          spec="az://local-container/blob")
        >>> isinstance(mounted_path, MountedAzurePath)
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


class SpecPath(PanPath):
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
        path: str | Path,
        *args: Any,
        mounted: str | Path | None = None,
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
            - SpecAzurePath for Azure Blob Storage paths
            - SpecS3Path for Amazon S3 paths
            - SpecLocalPath for local filesystem paths
        """
        if cls is SpecPath:
            path = PanPath(path)  # type: ignore
            if isinstance(path, GSPath):
                spec_class = SpecGSPath
            elif isinstance(path, AzurePath):
                spec_class = SpecAzurePath  # type: ignore
            elif isinstance(path, S3Path):
                spec_class = SpecS3Path  # type: ignore
            else:
                spec_class = SpecLocalPath

            obj = spec_class(path, *args, **kwargs)  # type: ignore
            obj._mounted = PanPath(mounted) if mounted is not None else obj
            return obj

        # Ensure Path internals are initialized with the provided path
        return super().__new__(cls, path, *args, **kwargs)  # type: ignore

    async def get_fspath(self) -> str:
        """Get the corresponding local filesystem path and copy from cloud.

        Returns:
            PanPath: The path as it appears in the local filesystem.
        """
        return self.__fspath__()

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
        if not isinstance(other, Path):
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

    def with_name(self, name) -> SpecPath:
        """Return a new path with the name changed.

        Args:
            name: The new name for the path.

        Returns:
            SpecPath: A new spec path with the name changed in both
                the spec path and mounted path.
        """
        new_path = LocalPath.with_name(self, name)
        new_mounted = PanPath(str(self._mounted)).with_name(name)

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
        new_mounted = PanPath(str(self._mounted)).with_suffix(suffix)

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
        new_mounted = PanPath(str(self._mounted)).with_stem(stem)

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
        new_mounted = PanPath(str(self._mounted)).joinpath(*pathsegments)

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
        new_mounted = PanPath(str(self._mounted)).parent

        return SpecPath(new_path, mounted=new_mounted)


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

    def __fspath__(self) -> str:
        """Return the filesystem path representation.

        Returns:
            str: The filesystem path as a string.
        """
        cloud_fspath = os.getenv("XQUTE_CLOUD_FSPATH", DEFAULT_CLOUD_FSPATH)
        parts = [
            cloud_fspath,
            self.parts[0].replace(":", ""),
            *self.parts[1:],
        ]
        return os.path.join(*parts)

    async def get_fspath(self) -> str:
        """Get the corresponding local filesystem path and copy from cloud.

        Returns:
            PanPath: The path as it appears in the local filesystem.
        """
        p = PanPath(self.__fspath__())
        await p.parent.a_mkdir(parents=True, exist_ok=True)

        if await self.a_is_dir():
            await self.a_copytree(p)
        else:
            await self.a_copy(p)

        return str(p)


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


class SpecAzurePath(SpecCloudPath, AzurePath):
    """A class to represent a spec Azure Blob Storage path

    This class represents an Azure Blob Storage path as it appears in the
    local environment where the framework runs, while maintaining a reference
    to its corresponding path in the remote execution environment.

    Examples:
        >>> spec_path = SpecPath("az://container/blob",
        ...                    mounted="az://remote-container/blob")
        >>> isinstance(spec_path, SpecAzurePath)
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
