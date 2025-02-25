import pytest  # noqa

from pathlib import Path
from yunpath import AnyPath, GSPath
from xqute.utils import DualPath


def test_dualpath_as_normal_path():
    dp = DualPath("/mnt/remote")
    assert isinstance(dp, DualPath)
    assert isinstance(dp.path, Path)
    assert str(dp) == "/mnt/remote"  # regular method
    assert dp.path == Path("/mnt/remote")
    assert dp.mounted == dp.path
    assert dp.stem == "remote"  # regular method
    assert dp.with_name("new") == Path("/mnt/new")
    assert dp.with_suffix(".txt") == Path("/mnt/remote.txt")
    assert dp.with_stem("xyz") == Path("/mnt/xyz")
    assert dp.parent == Path("/mnt")
    assert repr(dp.parent) == "DualPath('/mnt', mounted='/mnt')"


def test_dualpath():
    dp = DualPath("gs://bucket/remote", mounted="/mnt/remote")
    assert isinstance(dp.path, GSPath)
    assert str(dp) == "gs://bucket/remote"  # regular method
    assert dp == AnyPath("gs://bucket/remote")
    assert dp.mounted == Path("/mnt/remote")
    assert dp.stem == "remote"  # regular method

    p = dp.parent
    assert p == GSPath("gs://bucket")
    assert p.mounted == Path("/mnt")

    dp2 = dp / "sub"
    assert dp2 == GSPath("gs://bucket/remote/sub")
    assert dp2.mounted == Path("/mnt/remote/sub")

    dp3 = dp2.with_name("new")
    assert dp3 == GSPath("gs://bucket/remote/new")
    assert dp3.mounted == Path("/mnt/remote/new")

    dp4 = dp3.with_suffix(".txt")
    assert dp4 == GSPath("gs://bucket/remote/new.txt")
    assert dp4.mounted == Path("/mnt/remote/new.txt")

    dp5 = dp4.with_stem("xyz")
    assert str(dp5) == str(GSPath("gs://bucket/remote/xyz.txt"))
    assert dp5.mounted == Path("/mnt/remote/xyz.txt")


def test_dualpath_defaults_to_mounted_path():
    dp = DualPath("/mnt/remote")
    assert isinstance(dp.path, Path)
    assert str(dp) == "/mnt/remote"  # regular method
    assert dp == Path("/mnt/remote")
    assert dp.mounted == Path("/mnt/remote")
    assert dp.stem == "remote"  # regular method


def test_dualpath_with_cloudpath():
    dp = DualPath("gs://bucket/remote")

    assert isinstance(dp.path, GSPath)
    assert str(dp) == "gs://bucket/remote"  # regular method
    assert dp.mounted == GSPath("gs://bucket/remote")
    assert dp.stem == "remote"  # regular method
    assert dp.mounted.spec is dp.path
