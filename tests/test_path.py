import pytest  # noqa: F401
import sys
from pathlib import Path

from yunpath import CloudPath, GSPath, GSClient
from xqute.path import (
    LocalPath,
    SpecPath,
    SpecLocalPath,
    SpecCloudPath,
    SpecGSPath,
    # SpecAzureBlobPath,
    # SpecS3Path,
    MountedPath,
    MountedLocalPath,
    MountedCloudPath,
    MountedGSPath,
    # MountedAzureBlobPath,
    # MountedS3Path,
)


def test_mountedpath_is_hashable():
    """Test that MountedPath is hashable."""
    p = MountedPath("/path/to/file")
    assert isinstance(p, Path)
    assert isinstance(p, MountedPath)
    assert isinstance(p, MountedLocalPath)

    d = {p: "value"}
    assert d[p] == "value"


def test_mountedlocalpath():
    p = MountedPath("/path/to/file")
    assert p.name == "file"
    assert p.suffix == ""
    assert p.stem == "file"
    assert p.absolute()

    assert p != 1

    assert p == Path("/path/to/file")
    assert isinstance(p, Path)
    assert isinstance(p, MountedPath)
    assert isinstance(p, MountedLocalPath)
    assert not isinstance(p, CloudPath)
    assert str(p) == "/path/to/file"
    assert repr(p) == "MountedLocalPath('/path/to/file')"

    assert not p.is_mounted()

    spec = p.spec
    assert spec == p
    assert isinstance(spec, Path)
    assert isinstance(spec, SpecPath)
    assert isinstance(spec, SpecLocalPath)
    assert not isinstance(spec, CloudPath)

    assert spec.mounted == p

    p1 = p / "file2"
    assert p1 == Path("/path/to/file/file2")
    assert isinstance(p1, Path)
    assert isinstance(p1, MountedPath)
    assert isinstance(p1, MountedLocalPath)
    assert not isinstance(p1, CloudPath)
    assert str(p1) == "/path/to/file/file2"

    p11 = p.joinpath("file2")
    assert p11 == Path("/path/to/file/file2")
    assert isinstance(p11, Path)
    assert isinstance(p11, MountedPath)
    assert isinstance(p11, MountedLocalPath)
    assert not isinstance(p11, CloudPath)
    assert str(p11) == "/path/to/file/file2"

    spec1 = p1.spec
    assert spec1 == p1
    assert isinstance(spec1, Path)
    assert isinstance(spec1, SpecPath)
    assert isinstance(spec1, SpecLocalPath)
    assert not isinstance(spec1, CloudPath)

    p2 = p.with_name("file3.txt")
    assert p2 == Path("/path/to/file3.txt")
    assert p2.spec == p2

    p3 = p2.with_suffix(".ext")
    assert p3 == Path("/path/to/file3.ext")
    assert p3.spec == p3

    if sys.version_info >= (3, 12):
        p4 = p3.with_segments("dir1", "dir2")
        assert p4 == Path("dir1/dir2")
        assert p4.spec == p4

    p5 = p2.with_stem("file4")
    assert p5 == Path("/path/to/file4.txt")
    assert p5.spec == p5

    p6 = p.joinpath("dir1", "dir2")
    assert p6 == Path("/path/to/file/dir1/dir2")
    assert p6.spec == p6

    p7 = p.parent
    assert p7 == Path("/path/to")
    assert p7.spec == p7

    assert p7.spec.mounted.spec.mounted.spec.mounted.spec.mounted == p7


def test_mountedlocalpath_with_spec():
    p = MountedPath("/path/to/file", spec="/path/to/spec")
    assert p.name == "file"
    assert p.suffix == ""
    assert p.stem == "file"
    assert p.absolute()

    assert p == Path("/path/to/file")
    assert isinstance(p, Path)
    assert isinstance(p, MountedPath)
    assert isinstance(p, MountedLocalPath)
    assert not isinstance(p, CloudPath)
    assert str(p) == "/path/to/file"
    assert repr(p) == "MountedLocalPath('/path/to/file', spec='/path/to/spec')"

    assert p.is_mounted()

    p1 = p / "file2"
    assert p1 == Path("/path/to/file/file2")
    assert p1.is_mounted()
    assert p1.spec == Path("/path/to/spec/file2")
    assert isinstance(p1, Path)
    assert isinstance(p1, MountedPath)
    assert isinstance(p1, MountedLocalPath)
    assert not isinstance(p1, CloudPath)
    assert str(p1) == "/path/to/file/file2"
    assert repr(p1) == (
        "MountedLocalPath('/path/to/file/file2', spec='/path/to/spec/file2')"
    )

    p2 = p.with_name("file3.txt")
    assert p2 == Path("/path/to/file3.txt")
    assert p2.spec == Path("/path/to/file3.txt")

    p3 = p2.with_suffix(".ext")
    assert p3 == Path("/path/to/file3.ext")
    assert p3.spec == Path("/path/to/file3.ext")

    if sys.version_info >= (3, 12):
        p4 = p3.with_segments("dir1", "dir2")
        assert p4 == Path("dir1/dir2")
        assert p4.spec == Path("dir1/dir2")

    p5 = p2.with_stem("file4")
    assert p5 == Path("/path/to/file4.txt")
    assert p5.spec == Path("/path/to/file4.txt")

    p6 = p.joinpath("dir1", "dir2")
    assert p6 == Path("/path/to/file/dir1/dir2")
    assert p6.spec == Path("/path/to/spec/dir1/dir2")

    p7 = p.parent
    assert p7 == Path("/path/to")
    assert p7.spec == Path("/path/to")

    assert p7.spec.mounted.spec.mounted.spec.mounted.spec.mounted == p7


def test_mountedcloudpath():
    client = GSClient()
    p = MountedPath("gs://bucket/path/to/file", client=client)
    assert p.name == "file"
    assert p.suffix == ""
    assert p.stem == "file"

    assert p == GSPath("gs://bucket/path/to/file")
    assert isinstance(p, GSPath)
    assert isinstance(p, MountedPath)
    assert isinstance(p, MountedCloudPath)
    assert isinstance(p, MountedGSPath)
    assert isinstance(p, CloudPath)
    assert not isinstance(p, Path)
    assert str(p) == "gs://bucket/path/to/file"
    assert repr(p) == "MountedGSPath('gs://bucket/path/to/file')"

    assert not p.is_mounted()

    spec = p.spec
    assert isinstance(spec, GSPath)
    assert isinstance(spec, SpecPath)
    assert isinstance(spec, SpecCloudPath)
    assert isinstance(spec, SpecGSPath)
    assert isinstance(spec, CloudPath)
    assert not isinstance(spec, Path)
    assert spec.client == p.client == client
    assert not isinstance(spec, Path)

    assert spec.mounted == p
    assert spec.mounted.client == p.client == client

    p1 = p / "file2"
    assert p1.client == p.client == client
    assert p1 == GSPath("gs://bucket/path/to/file/file2")
    assert isinstance(p1, GSPath)
    assert isinstance(p1, MountedPath)
    assert isinstance(p1, MountedCloudPath)
    assert isinstance(p1, MountedGSPath)
    assert isinstance(p1, CloudPath)
    assert not isinstance(p1, Path)
    assert str(p1) == "gs://bucket/path/to/file/file2"

    spec1 = p1.spec
    assert spec1.client == p1.client == client
    assert spec1 == p1
    assert isinstance(spec1, GSPath)
    assert isinstance(spec1, SpecPath)
    assert isinstance(spec1, SpecCloudPath)
    assert isinstance(spec1, SpecGSPath)
    assert isinstance(spec1, CloudPath)
    assert not isinstance(spec1, Path)

    p2 = p.with_name("file3.txt")
    assert p2.client == p.client == client
    assert p2 == GSPath("gs://bucket/path/to/file3.txt")
    assert str(p2) == "gs://bucket/path/to/file3.txt"
    assert p2.spec == p2

    p3 = p2.with_suffix(".ext")
    assert p3.client == p2.client == client
    assert p3 == GSPath("gs://bucket/path/to/file3.ext")
    assert p3.spec == p3

    if sys.version_info >= (3, 12):
        p4 = p3.with_segments("dir1", "dir2")
        assert p4.client == p3.client == client
        assert p4 == GSPath("gs://dir1/dir2")
        assert p4.spec == p4

    p5 = p2.with_stem("file4")
    assert p5.client == p2.client == client
    assert p5 == GSPath("gs://bucket/path/to/file4.txt")
    assert p5.spec == p5

    p6 = p.joinpath("dir1", "dir2")
    assert p6.client == p.client == client
    assert p6 == GSPath("gs://bucket/path/to/file/dir1/dir2")
    assert p6.spec == p6

    p7 = p.parent
    assert p7.client == p.client == client
    assert p7 == GSPath("gs://bucket/path/to")
    assert p7.spec == p7

    assert p7.spec.mounted.spec.mounted.spec.mounted.spec.mounted == p7


def test_mountedcloudpath_with_spec():
    p = MountedPath("gs://bucket/path/to/file", spec="gs://bucket/path/to/spec")
    assert p.name == "file"
    assert p.suffix == ""
    assert p.stem == "file"

    assert p == GSPath("gs://bucket/path/to/file")
    assert isinstance(p, CloudPath)
    assert isinstance(p, MountedPath)
    assert isinstance(p, MountedCloudPath)
    assert isinstance(p, MountedGSPath)
    assert isinstance(p, CloudPath)
    assert not isinstance(p, Path)
    assert str(p) == "gs://bucket/path/to/file"
    assert (
        repr(p)
        == "MountedGSPath('gs://bucket/path/to/file', spec='gs://bucket/path/to/spec')"
    )

    assert p.is_mounted()

    spec = p.spec
    assert isinstance(spec, GSPath)
    assert isinstance(spec, SpecPath)
    assert isinstance(spec, SpecCloudPath)
    assert isinstance(spec, SpecGSPath)
    assert isinstance(spec, CloudPath)
    assert not isinstance(spec, Path)
    assert str(spec) == "gs://bucket/path/to/spec"

    assert spec.mounted == p

    p1 = p / "file2"
    assert p1 == GSPath("gs://bucket/path/to/file/file2")
    assert p1.is_mounted()
    assert p1.spec == GSPath("gs://bucket/path/to/spec/file2")
    assert isinstance(p1, GSPath)
    assert isinstance(p1, MountedPath)
    assert isinstance(p1, MountedCloudPath)
    assert isinstance(p1, MountedGSPath)
    assert isinstance(p1, CloudPath)
    assert not isinstance(p1, Path)
    assert str(p1) == "gs://bucket/path/to/file/file2"
    assert repr(p1) == (
        "MountedGSPath('gs://bucket/path/to/file/file2', "
        "spec='gs://bucket/path/to/spec/file2')"
    )

    p2 = p.with_name("file3.txt")
    assert p2 == GSPath("gs://bucket/path/to/file3.txt")
    assert p2.spec == GSPath("gs://bucket/path/to/file3.txt")

    p3 = p2.with_suffix(".ext")
    assert p3 == GSPath("gs://bucket/path/to/file3.ext")
    assert p3.spec == GSPath("gs://bucket/path/to/file3.ext")

    if sys.version_info >= (3, 12):
        p4 = p3.with_segments("dir1", "dir2")
        assert p4 == GSPath("gs://dir1/dir2")
        assert p4.spec == GSPath("gs://dir1/dir2")

    p5 = p2.with_stem("file4")
    assert p5 == GSPath("gs://bucket/path/to/file4.txt")
    assert p5.spec == GSPath("gs://bucket/path/to/file4.txt")

    p6 = p.joinpath("dir1", "dir2")
    assert p6 == GSPath("gs://bucket/path/to/file/dir1/dir2")
    assert p6.spec == GSPath("gs://bucket/path/to/spec/dir1/dir2")

    p7 = p.parent
    assert p7 == GSPath("gs://bucket/path/to")
    assert p7.spec == GSPath("gs://bucket/path/to")

    assert p7.spec.mounted.spec.mounted.spec.mounted.spec.mounted == p7


def test_specpath_is_hashable():
    """Test that SpecPath is hashable."""
    p = SpecPath("/path/to/file")
    assert isinstance(p, Path)
    assert isinstance(p, SpecPath)
    assert isinstance(p, SpecLocalPath)

    d = {p: "value"}
    assert d[p] == "value"


def test_speclocalpath():
    p = SpecPath("/path/to/file")
    assert p.name == "file"
    assert p.suffix == ""
    assert p.stem == "file"

    assert p != 1

    assert p == Path("/path/to/file")
    assert isinstance(p, Path)
    assert isinstance(p, SpecPath)
    assert isinstance(p, SpecLocalPath)
    assert isinstance(p, LocalPath)
    assert not isinstance(p, CloudPath)
    assert str(p) == "/path/to/file"
    assert repr(p) == "SpecLocalPath('/path/to/file')"

    mounted = p.mounted
    assert mounted == p
    assert isinstance(mounted, Path)
    assert isinstance(mounted, MountedPath)
    assert isinstance(mounted, MountedLocalPath)
    assert isinstance(mounted, LocalPath)
    assert not isinstance(mounted, CloudPath)

    assert mounted.spec == p

    # Test path operations
    p1 = p / "file2"
    assert p1 == Path("/path/to/file/file2")
    assert isinstance(p1, Path)
    assert isinstance(p1, SpecPath)
    assert isinstance(p1, SpecLocalPath)
    assert isinstance(p1, LocalPath)
    assert not isinstance(p1, CloudPath)
    assert str(p1) == "/path/to/file/file2"

    mounted1 = p1.mounted
    assert mounted1 == p1
    assert isinstance(mounted1, Path)
    assert isinstance(mounted1, MountedPath)
    assert isinstance(mounted1, MountedLocalPath)
    assert isinstance(mounted1, LocalPath)
    assert not isinstance(mounted1, CloudPath)

    # Test other path operations
    p2 = p.with_name("file3.txt")
    assert p2 == Path("/path/to/file3.txt")
    assert p2.mounted == p2

    p3 = p2.with_suffix(".ext")
    assert p3 == Path("/path/to/file3.ext")
    assert p3.mounted == p3

    if sys.version_info >= (3, 12):
        p4 = p3.with_segments("dir1", "dir2")
        assert p4 == Path("dir1/dir2")
        assert p4.mounted == p4

    p5 = p2.with_stem("file4")
    assert p5 == Path("/path/to/file4.txt")
    assert p5.mounted == p5

    p6 = p.joinpath("dir1", "dir2")
    assert p6 == Path("/path/to/file/dir1/dir2")
    assert p6.mounted == p6

    p7 = p.parent
    assert p7 == Path("/path/to")
    assert p7.mounted == p7

    assert p7.mounted.spec.mounted.spec.mounted.spec.mounted.spec.mounted == p7


def test_speclocalpath_with_mounted():
    p = SpecPath("/path/to/spec", mounted="/path/to/file")
    assert p.name == "spec"
    assert p.suffix == ""
    assert p.stem == "spec"

    assert p == Path("/path/to/spec")
    assert isinstance(p, Path)
    assert isinstance(p, SpecPath)
    assert not isinstance(p, CloudPath)
    assert str(p) == "/path/to/spec"
    assert repr(p) == "SpecLocalPath('/path/to/spec', mounted='/path/to/file')"

    mounted = p.mounted
    assert mounted == Path("/path/to/file")
    assert mounted != p  # Different path
    assert isinstance(mounted, Path)
    assert isinstance(mounted, MountedPath)
    assert not isinstance(mounted, CloudPath)

    assert mounted.spec == p

    # Test path division preserves relationship
    p1 = p / "file2"
    assert p1 == Path("/path/to/spec/file2")
    mounted1 = p1.mounted
    assert mounted1 == Path("/path/to/file/file2")
    assert mounted1.spec == p1

    p2 = p.with_name("file3.txt")
    assert p2 == Path("/path/to/file3.txt")
    assert p2.mounted == Path("/path/to/file3.txt")

    p3 = p2.with_suffix(".ext")
    assert p3 == Path("/path/to/file3.ext")
    assert p3.mounted == Path("/path/to/file3.ext")

    if sys.version_info >= (3, 12):
        p4 = p3.with_segments("dir1", "dir2")
        assert p4 == Path("dir1/dir2")
        assert p4.mounted == Path("dir1/dir2")

    p5 = p2.with_stem("file4")
    assert p5 == Path("/path/to/file4.txt")
    assert p5.mounted == Path("/path/to/file4.txt")

    p6 = p.joinpath("dir1", "dir2")
    assert p6 == Path("/path/to/spec/dir1/dir2")
    assert p6.mounted == Path("/path/to/file/dir1/dir2")

    p7 = p.parent
    assert p7 == Path("/path/to")
    assert p7.mounted == Path("/path/to")

    assert p7.mounted.spec.mounted.spec.mounted.spec.mounted.spec.mounted == p7


def test_speccloudpath():
    p = SpecPath("gs://bucket/path/to/file")
    assert p.name == "file"
    assert p.suffix == ""
    assert p.stem == "file"

    assert p == GSPath("gs://bucket/path/to/file")
    assert isinstance(p, CloudPath)
    assert isinstance(p, SpecPath)
    assert not isinstance(p, Path)
    assert str(p) == "gs://bucket/path/to/file"
    assert repr(p) == "SpecGSPath('gs://bucket/path/to/file')"

    mounted = p.mounted
    assert mounted == p
    assert mounted.client == p.client
    assert isinstance(mounted, CloudPath)
    assert isinstance(mounted, MountedPath)
    assert not isinstance(mounted, Path)

    assert mounted.spec == p

    # Test path operations
    p1 = p / "file2"
    assert p1.client == p.client
    assert p1 == GSPath("gs://bucket/path/to/file/file2")
    assert isinstance(p1, CloudPath)
    assert isinstance(p1, SpecPath)
    assert not isinstance(p1, Path)
    assert str(p1) == "gs://bucket/path/to/file/file2"

    mounted1 = p1.mounted
    assert mounted1.client == p1.client
    assert mounted1 == p1
    assert isinstance(mounted1, CloudPath)
    assert isinstance(mounted1, MountedPath)

    # Test other path operations
    p2 = p.with_name("file3.txt")
    assert p2.client == p.client
    assert p2 == GSPath("gs://bucket/path/to/file3.txt")
    assert p2.mounted == p2

    p3 = p2.with_suffix(".ext")
    assert p3.client == p2.client
    assert p3 == GSPath("gs://bucket/path/to/file3.ext")
    assert p3.mounted == p3

    if sys.version_info >= (3, 12):
        p4 = p3.with_segments("dir1", "dir2")
        assert p4.client == p3.client
        assert p4 == GSPath("gs://dir1/dir2")
        assert p4.mounted == p4

    p5 = p2.with_stem("file4")
    assert p5.client == p2.client
    assert p5 == GSPath("gs://bucket/path/to/file4.txt")
    assert p5.mounted == p5

    p6 = p.joinpath("dir1", "dir2")
    assert p6.client == p.client
    assert p6 == GSPath("gs://bucket/path/to/file/dir1/dir2")
    assert p6.mounted == p6

    p7 = p.parent
    assert p7.client == p.client
    assert p7 == GSPath("gs://bucket/path/to")
    assert p7.mounted == p7

    assert p7.mounted.spec.mounted.spec.mounted.spec.mounted.spec.mounted == p7


def test_speccloudpath_with_mounted():
    p = SpecPath("gs://bucket/path/to/spec", mounted="gs://bucket/path/to/file")
    assert p.name == "spec"
    assert p.suffix == ""
    assert p.stem == "spec"

    assert p == GSPath("gs://bucket/path/to/spec")
    assert isinstance(p, CloudPath)
    assert isinstance(p, SpecPath)
    assert not isinstance(p, Path)
    assert str(p) == "gs://bucket/path/to/spec"
    assert repr(p) == (
        "SpecGSPath('gs://bucket/path/to/spec', mounted='gs://bucket/path/to/file')"
    )
    assert p.mounted.is_mounted()

    mounted = p.mounted
    assert mounted == GSPath("gs://bucket/path/to/file")
    assert mounted != p  # Different path
    assert isinstance(mounted, CloudPath)
    assert isinstance(mounted, MountedPath)
    assert not isinstance(mounted, Path)

    assert mounted.spec == p

    # Test path division preserves relationship
    p1 = p / "file2"
    assert p1 == GSPath("gs://bucket/path/to/spec/file2")
    assert p1.mounted.is_mounted()

    mounted1 = p1.mounted
    assert mounted1 == GSPath("gs://bucket/path/to/file/file2")
    assert mounted1.spec == p1
    assert mounted1.client == p1.client

    # Test other path operations
    p2 = p.with_name("file3.txt")
    assert p2 == GSPath("gs://bucket/path/to/file3.txt")
    assert p2.mounted == GSPath("gs://bucket/path/to/file3.txt")

    p3 = p2.with_suffix(".ext")
    assert p3 == GSPath("gs://bucket/path/to/file3.ext")
    assert p3.mounted == GSPath("gs://bucket/path/to/file3.ext")

    if sys.version_info >= (3, 12):
        p4 = p3.with_segments("dir1", "dir2")
        assert p4 == GSPath("gs://dir1/dir2")
        assert p4.mounted == GSPath("gs://dir1/dir2")

    p5 = p2.with_stem("file4")
    assert p5 == GSPath("gs://bucket/path/to/file4.txt")
    assert p5.mounted == GSPath("gs://bucket/path/to/file4.txt")

    p6 = p.joinpath("dir1", "dir2")
    assert p6 == GSPath("gs://bucket/path/to/spec/dir1/dir2")
    assert p6.mounted == GSPath("gs://bucket/path/to/file/dir1/dir2")

    p7 = p.parent
    assert p7 == GSPath("gs://bucket/path/to")
    assert p7.mounted == GSPath("gs://bucket/path/to")

    assert p7.mounted.spec.mounted.spec.mounted.spec.mounted.spec.mounted == p7


def test_specpath_mixed_path_types():
    # Test spec path with different mounted path type
    p = SpecPath("/path/to/spec", mounted="gs://bucket/path/to/file")
    assert p.name == "spec"
    assert p.suffix == ""
    assert p.stem == "spec"

    assert isinstance(p, Path)
    assert p == Path("/path/to/spec")
    assert p.mounted.is_mounted()

    mounted = p.mounted
    assert isinstance(mounted, CloudPath)
    assert mounted == GSPath("gs://bucket/path/to/file")
    assert mounted.spec == p

    # Test path division preserves relationships
    p1 = p / "file2"
    mounted1 = p1.mounted
    assert p1 == Path("/path/to/spec/file2")
    assert mounted1 == GSPath("gs://bucket/path/to/file/file2")
    assert mounted1.spec == p1
    assert p1.mounted.is_mounted()
    assert mounted.client == mounted1.client

    p2 = p.with_name("file3.txt")
    assert p2 == Path("/path/to/file3.txt")
    assert p2.mounted == GSPath("gs://bucket/path/to/file3.txt")

    assert p2.mounted.client == p.mounted.client

    p3 = p2.with_suffix(".ext")
    assert p3 == Path("/path/to/file3.ext")
    assert p3.mounted == GSPath("gs://bucket/path/to/file3.ext")

    if sys.version_info >= (3, 12):
        p4 = p3.with_segments("dir1", "dir2")
        assert p4 == Path("dir1/dir2")
        assert p4.mounted == GSPath("gs://dir1/dir2")

    p5 = p2.with_stem("file4")
    assert p5 == Path("/path/to/file4.txt")
    assert p5.mounted == GSPath("gs://bucket/path/to/file4.txt")

    p6 = p.joinpath("dir1", "dir2")
    assert p6 == Path("/path/to/spec/dir1/dir2")
    assert p6.mounted == GSPath("gs://bucket/path/to/file/dir1/dir2")

    p7 = p.parent
    assert p7 == Path("/path/to")
    assert p7.mounted == GSPath("gs://bucket/path/to")

    assert p7.mounted.spec.mounted.spec.mounted.spec.mounted.spec == p7


def test_mountedpath_mixed_path_types():
    # Test mounted path with different spec path type
    m = MountedPath("/path/to/file", spec="gs://bucket/path/to/spec")
    assert m.name == "file"
    assert m.suffix == ""
    assert m.stem == "file"

    assert isinstance(m, Path)
    assert m == Path("/path/to/file")
    assert m.is_mounted()

    spec = m.spec
    assert isinstance(spec, CloudPath)
    assert spec == GSPath("gs://bucket/path/to/spec")
    assert spec.mounted == m

    # Test path division preserves relationships
    m1 = m / "file2"
    spec1 = m1.spec
    assert m1 == Path("/path/to/file/file2")
    assert spec1 == GSPath("gs://bucket/path/to/spec/file2")
    assert spec1.mounted == m1
    assert m1.is_mounted()
    assert spec.client == spec1.client

    m2 = m.with_name("file3.txt")
    assert m2 == Path("/path/to/file3.txt")
    assert m2.spec == GSPath("gs://bucket/path/to/file3.txt")

    assert m2.spec.client == m.spec.client

    m3 = m2.with_suffix(".ext")
    assert m3 == Path("/path/to/file3.ext")
    assert m3.spec == GSPath("gs://bucket/path/to/file3.ext")

    if sys.version_info >= (3, 12):
        m4 = m3.with_segments("dir1", "dir2")
        assert m4 == Path("dir1/dir2")
        assert m4.spec == GSPath("gs://dir1/dir2")

    m5 = m2.with_stem("file4")
    assert m5 == Path("/path/to/file4.txt")
    assert m5.spec == GSPath("gs://bucket/path/to/file4.txt")

    m6 = m.joinpath("dir1", "dir2")
    assert m6 == Path("/path/to/file/dir1/dir2")
    assert m6.spec == GSPath("gs://bucket/path/to/spec/dir1/dir2")

    m7 = m.parent
    assert m7 == Path("/path/to")
    assert m7.spec == GSPath("gs://bucket/path/to")

    assert m7.spec.mounted.spec.mounted.spec.mounted.spec.mounted == m7


def test_specpath_mixed_path_types2():
    p = SpecPath("gs://bucket/path/to/file", mounted="/path/to/mounted")
    assert p.name == "file"
    assert p.suffix == ""
    assert p.stem == "file"

    assert isinstance(p, SpecPath)
    assert isinstance(p, SpecCloudPath)
    assert isinstance(p, SpecGSPath)
    assert isinstance(p, CloudPath)
    assert p == GSPath("gs://bucket/path/to/file")
    assert p.mounted == Path("/path/to/mounted")
    assert p.mounted.is_mounted()

    mounted = p.mounted
    assert isinstance(mounted, Path)
    assert isinstance(mounted, MountedPath)
    assert isinstance(mounted, MountedLocalPath)
    assert not isinstance(mounted, CloudPath)
    assert mounted == Path("/path/to/mounted")
    assert mounted.spec == p

    # Test path division preserves relationships
    p1 = p / "file2"
    mounted1 = p1.mounted
    assert isinstance(p1, SpecPath)
    assert isinstance(p1, SpecCloudPath)
    assert isinstance(p1, SpecGSPath)
    assert isinstance(p1, CloudPath)
    assert p1 == GSPath("gs://bucket/path/to/file/file2")
    assert mounted1 == Path("/path/to/mounted/file2")
    assert mounted1.spec == p1
    assert p1.mounted.is_mounted()
    assert p1.client == p.client

    p2 = p.with_name("file3.txt")
    assert isinstance(p2, SpecPath)
    assert isinstance(p2, SpecCloudPath)
    assert isinstance(p2, SpecGSPath)
    assert isinstance(p2, CloudPath)
    assert p2 == GSPath("gs://bucket/path/to/file3.txt")
    assert p2.mounted == Path("/path/to/file3.txt")

    p3 = p2.with_suffix(".ext")
    assert isinstance(p3, SpecPath)
    assert isinstance(p3, SpecCloudPath)
    assert isinstance(p3, SpecGSPath)
    assert isinstance(p3, CloudPath)
    assert p3 == GSPath("gs://bucket/path/to/file3.ext")
    assert p3.mounted == Path("/path/to/file3.ext")

    if sys.version_info >= (3, 12):
        p4 = p3.with_segments("dir1", "dir2")
        assert isinstance(p4, SpecPath)
        assert isinstance(p4, SpecCloudPath)
        assert isinstance(p4, SpecGSPath)
        assert isinstance(p4, CloudPath)
        assert p4 == GSPath("gs://dir1/dir2")
        assert p4.mounted == Path("dir1/dir2")

    p5 = p2.with_stem("file4")
    assert isinstance(p5, SpecPath)
    assert isinstance(p5, SpecCloudPath)
    assert isinstance(p5, SpecGSPath)
    assert isinstance(p5, CloudPath)
    assert p5 == GSPath("gs://bucket/path/to/file4.txt")
    assert p5.mounted == Path("/path/to/file4.txt")

    p6 = p.joinpath("dir1", "dir2")
    assert isinstance(p6, SpecPath)
    assert isinstance(p6, SpecCloudPath)
    assert isinstance(p6, SpecGSPath)
    assert isinstance(p6, CloudPath)
    assert p6 == GSPath("gs://bucket/path/to/file/dir1/dir2")
    assert p6.mounted == Path("/path/to/mounted/dir1/dir2")

    p7 = p.parent
    assert isinstance(p7, SpecPath)
    assert isinstance(p7, SpecCloudPath)
    assert isinstance(p7, SpecGSPath)
    assert isinstance(p7, CloudPath)
    assert p7 == GSPath("gs://bucket/path/to")
    assert p7.mounted == Path("/path/to")

    assert p7.mounted.spec.mounted.spec.mounted.spec.mounted.spec == p7


def test_mountedpath_mixed_path_types2():
    m = MountedPath("gs://bucket/path/to/file", spec="/path/to/spec")
    assert m.name == "file"
    assert m.suffix == ""
    assert m.stem == "file"

    assert isinstance(m, MountedPath)
    assert isinstance(m, MountedCloudPath)
    assert isinstance(m, MountedGSPath)
    assert isinstance(m, CloudPath)
    assert m == GSPath("gs://bucket/path/to/file")
    assert m.spec == Path("/path/to/spec")
    assert m.is_mounted()

    spec = m.spec
    assert isinstance(spec, Path)
    assert isinstance(spec, SpecPath)
    assert isinstance(spec, SpecLocalPath)
    assert not isinstance(spec, CloudPath)
    assert spec == Path("/path/to/spec")
    assert spec.mounted == m

    # Test path division preserves relationships
    m1 = m / "file2"
    spec1 = m1.spec
    assert isinstance(m1, MountedPath)
    assert isinstance(m1, MountedCloudPath)
    assert isinstance(m1, MountedGSPath)
    assert isinstance(m1, CloudPath)
    assert m1 == GSPath("gs://bucket/path/to/file/file2")
    assert spec1 == Path("/path/to/spec/file2")
    assert spec1.mounted == m1
    assert m1.is_mounted()
    assert m1.client == m.client

    m2 = m.with_name("file3.txt")
    assert isinstance(m2, MountedPath)
    assert isinstance(m2, MountedCloudPath)
    assert isinstance(m2, MountedGSPath)
    assert isinstance(m2, CloudPath)
    assert m2 == GSPath("gs://bucket/path/to/file3.txt")
    assert m2.spec == Path("/path/to/file3.txt")

    m3 = m2.with_suffix(".ext")
    assert isinstance(m3, MountedPath)
    assert isinstance(m3, MountedCloudPath)
    assert isinstance(m3, MountedGSPath)
    assert isinstance(m3, CloudPath)
    assert m3 == GSPath("gs://bucket/path/to/file3.ext")
    assert m3.spec == Path("/path/to/file3.ext")

    if sys.version_info >= (3, 12):
        m4 = m3.with_segments("dir1", "dir2")
        assert isinstance(m4, MountedPath)
        assert isinstance(m4, MountedCloudPath)
        assert isinstance(m4, MountedGSPath)
        assert isinstance(m4, CloudPath)
        assert m4 == GSPath("gs://dir1/dir2")
        assert m4.spec == Path("dir1/dir2")

    m5 = m2.with_stem("file4")
    assert isinstance(m5, MountedPath)
    assert isinstance(m5, MountedCloudPath)
    assert isinstance(m5, MountedGSPath)
    assert isinstance(m5, CloudPath)
    assert m5 == GSPath("gs://bucket/path/to/file4.txt")
    assert m5.spec == Path("/path/to/file4.txt")

    m6 = m.joinpath("dir1", "dir2")
    assert isinstance(m6, MountedPath)
    assert isinstance(m6, MountedCloudPath)
    assert isinstance(m6, MountedGSPath)
    assert isinstance(m6, CloudPath)
    assert m6 == GSPath("gs://bucket/path/to/file/dir1/dir2")
    assert m6.spec == Path("/path/to/spec/dir1/dir2")

    m7 = m.parent
    assert isinstance(m7, MountedPath)
    assert isinstance(m7, MountedCloudPath)
    assert isinstance(m7, MountedGSPath)
    assert isinstance(m7, CloudPath)
    assert m7 == GSPath("gs://bucket/path/to")
    assert m7.spec == Path("/path/to")

    assert m7.spec.mounted.spec.mounted.spec.mounted.spec.mounted == m7


def test_mountedpath_serialization():
    """Test that MountedPath instances can be serialized with pickle."""
    import pickle

    # Test MountedLocalPath without spec
    p1 = MountedPath("/path/to/file")
    serialized1 = pickle.dumps(p1)
    deserialized1 = pickle.loads(serialized1)
    assert str(deserialized1) == str(p1)
    assert str(deserialized1.spec) == str(p1.spec)
    assert isinstance(deserialized1, MountedLocalPath)
    assert not deserialized1.is_mounted()

    # Test MountedLocalPath with spec
    p2 = MountedPath("/path/to/file", spec="/path/to/spec")
    serialized2 = pickle.dumps(p2)
    deserialized2 = pickle.loads(serialized2)
    assert str(deserialized2) == str(p2)
    assert str(deserialized2.spec) == str(p2.spec)
    assert isinstance(deserialized2, MountedLocalPath)
    assert deserialized2.is_mounted()

    # Test MountedGSPath without spec
    p3 = MountedPath("gs://bucket/path/to/file")
    serialized3 = pickle.dumps(p3)
    deserialized3 = pickle.loads(serialized3)
    assert str(deserialized3) == str(p3)
    assert str(deserialized3.spec) == str(p3.spec)
    assert isinstance(deserialized3, MountedGSPath)
    assert not deserialized3.is_mounted()

    # Test MountedGSPath with spec
    p4 = MountedPath("gs://bucket/path/to/file", spec="/path/to/spec")
    serialized4 = pickle.dumps(p4)
    deserialized4 = pickle.loads(serialized4)
    assert str(deserialized4) == str(p4)
    assert str(deserialized4.spec) == str(p4.spec)
    assert isinstance(deserialized4, MountedGSPath)
    assert deserialized4.is_mounted()
