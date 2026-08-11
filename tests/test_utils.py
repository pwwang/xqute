import pytest

from pathlib import Path
from xqute.utils import sanitize_mounts


async def test_sanitize_mounts_named_file():
    sans_mounts, named_mounts_dict = await sanitize_mounts(
        [f"thisfile={__file__}"],
        "/mnt/disks",
    )
    assert len(sans_mounts) == 1
    assert sans_mounts[0][0] == Path(__file__).parent
    assert (
        sans_mounts[0][1]
        == Path("/mnt/disks/NAMED_MOUNTS") / "thisfile" / Path(__file__).parent.name
    )
    assert len(named_mounts_dict) == 1
    assert (
        str(named_mounts_dict["thisfile"])
        == "/mnt/disks/NAMED_MOUNTS/thisfile/tests/test_utils.py"
    )


async def test_sanitize_mounts_named_dir():
    sans_mounts, named_mounts_dict = await sanitize_mounts(
        [f"thisdir={Path(__file__).parent}"],
        "/mnt/disks",
    )
    assert len(sans_mounts) == 1
    assert sans_mounts[0][0] == Path(__file__).parent
    assert (
        sans_mounts[0][1]
        == Path("/mnt/disks/NAMED_MOUNTS") / "thisdir"
    )
    assert len(named_mounts_dict) == 1
    assert (
        str(named_mounts_dict["thisdir"])
        == "/mnt/disks/NAMED_MOUNTS/thisdir"
    )


async def test_sanitize_mounts_single_mount():
    sans_mounts, named_mounts_dict = await sanitize_mounts(
        f"{Path(__file__).parent}:{Path(__file__).parent}",
        "/mnt/disks",
    )
    assert len(sans_mounts) == 1
    assert sans_mounts[0][0] == Path(__file__).parent
    assert sans_mounts[0][1] == Path(__file__).parent
    assert len(named_mounts_dict) == 0


async def test_sanitize_mounts_invalid_mount():
    with pytest.raises(ValueError):
        await sanitize_mounts(
            "invalid_mount_format",
            "/mnt/disks",
        )


async def test_sanitize_mounts_duplicate_mounts():
    sans_mounts, named_mounts_dict = await sanitize_mounts(
        [
            f"{Path(__file__).parent}:{Path(__file__).parent}",
            f"{Path(__file__).parent}:{Path(__file__).parent}",
        ],
        "/mnt/disks",
    )
    assert len(sans_mounts) == 1
    assert sans_mounts[0][0] == Path(__file__).parent
    assert sans_mounts[0][1] == Path(__file__).parent
    assert len(named_mounts_dict) == 0


async def test_sanitize_mounts_duplicate_mounts_conflicting():
    with pytest.raises(ValueError):
        await sanitize_mounts(
            [
                f"{Path(__file__).parent}:{Path(__file__).parent}",
                f"{Path(__file__).parent}/mocks:{Path(__file__).parent}",
            ],
            "/mnt/disks",
        )


async def test_sanitize_mounts_relative_mounts():
    # longer one is silently removed because it is a subpath of the shorter one
    sans_mounts, named_mounts_dict = await sanitize_mounts(
        [
            f"{Path(__file__).parent}:/mnt/disks/tests",
            f"{Path(__file__).parent}/mocks:/mnt/disks/tests/mocks",
        ],
        "/mnt/disks",
    )
    assert len(sans_mounts) == 1
    assert sans_mounts[0][0] == Path(__file__).parent
    assert str(sans_mounts[0][1]) == "/mnt/disks/tests"
    assert len(named_mounts_dict) == 0


# async def test_sanitize_mounts_named_source_not_exist():
#     with pytest.raises(FileNotFoundError):
#         await sanitize_mounts(
#             ["nonexistent=/mnt/disks/nonexistent"],
#             "/mnt/disks",
#         )


# async def test_sanitize_mounts_source_not_exist():
#     with pytest.raises(FileNotFoundError):
#         await sanitize_mounts(
#             ["/nonexistent:/mnt/disks/nonexistent"],
#             "/mnt/disks",
#         )
