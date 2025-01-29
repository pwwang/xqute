import asyncio
import pytest
import subprocess
from argx import Namespace
from unittest.mock import patch, MagicMock, AsyncMock
from xqute import __version__
from xqute.__main__ import (
    _update_status,
    check_version_and_plugins,
    launch_job,
    main,
    SchedulerXquteInconsistencyWarning,
)
from xqute.defaults import JobStatus

# FILE: xqute/test___main__.py


def test_update_status(tmp_path):
    metadir = tmp_path / "metadir"
    metadir.mkdir()
    status = JobStatus.RUNNING
    _update_status(str(metadir), status)
    assert (metadir / "job.status").read_text() == str(status)


def test_check_version_and_plugins():
    args = MagicMock()
    args.version = "1.0.0"
    args.plugin = ["plugin1:1.0.0", "plugin2:2.0.0"]
    plugin1 = Namespace(name="plugin1", version="1.0.0")
    plugin2 = Namespace(name="plugin2", version="2.0.0")

    with patch("xqute.__main__.__version__", "1.0.0"):
        with patch(
            "xqute.__main__.plugin.get_enabled_plugins",
            return_value={"plugin1": "1.0.0", "plugin2": "2.0.0"},
        ):
            check_version_and_plugins(args)

    with patch("xqute.__main__.__version__", "1.0.0"):
        with patch(
            "xqute.__main__.plugin.get_enabled_plugins",
            return_value={"plugin1": "1.0.0"},
        ):
            with pytest.warns(SchedulerXquteInconsistencyWarning):
                check_version_and_plugins(args)

    with patch("xqute.__main__.__version__", "1.0.0"):
        with patch(
            "xqute.__main__.plugin.get_enabled_plugins",
            return_value={"plugin1": plugin1, "plugin2": plugin2},
        ):
            with pytest.warns(SchedulerXquteInconsistencyWarning):
                check_version_and_plugins(args)


def test_check_version_and_plugins_version_mismatch():
    args = MagicMock()
    args.version = "1.0.0"

    with patch("xqute.__main__.__version__", "2.0.0"):
        with pytest.warns(
            SchedulerXquteInconsistencyWarning,
            match="The xqute on the scheduler system is 2.0.0",
        ):
            check_version_and_plugins(args)

    args.plugin = ["plugin1:1.0.0"]
    with patch("xqute.__main__.__version__", "1.0.0"):
        with patch(
            "xqute.__main__.plugin.get_enabled_plugins",
            return_value={"plugin1": "2.0.0"},
        ):
            with pytest.warns(
                SchedulerXquteInconsistencyWarning,
                match="Xqute plugin version mismatch: plugin1 1.0.0 != 2.0.0",
            ):
                check_version_and_plugins(args)


@pytest.mark.asyncio
async def test_launch_job(tmp_path):
    args = MagicMock()
    args.metadir = str(tmp_path / "metadir")
    args.cmd = ["echo", "hello"]
    (tmp_path / "metadir").mkdir()

    with patch(
        "xqute.__main__.plugin.hooks.on_jobsched_started", new_callable=AsyncMock
    ):
        with patch(
            "xqute.__main__.plugin.hooks.on_jobsched_ended", new_callable=AsyncMock
        ):
            await launch_job(args)

    assert (tmp_path / "metadir" / "job.stdout").read_text() == "hello\n"
    assert (tmp_path / "metadir" / "job.rc").read_text() == "0"
    assert (tmp_path / "metadir" / "job.status").read_text() == str(JobStatus.FINISHED)


@pytest.mark.asyncio
async def test_launch_job_failed(tmp_path):
    metadir = tmp_path / "metadir"
    metadir.mkdir()
    args = Namespace(
        metadir=str(metadir),
        cmd=["bash", "-c", "exit 1"],
        scheduler="local",
        version=__version__,
        plugin=[],
    )

    with pytest.raises(SystemExit):
        await launch_job(args)
    assert (metadir / "job.rc").read_text() != "0"

    args = Namespace(
        metadir=str(metadir),
        cmd=["__NoNeXisT__"],
        scheduler="local",
        version=__version__,
        plugin=[],
    )

    with pytest.raises(SystemExit):
        await launch_job(args)
    assert (metadir / "job.rc").read_text() != "0"


@pytest.mark.asyncio
async def test_launch_job_cancelled(tmp_path):
    args = MagicMock()
    args.metadir = str(tmp_path / "metadir")
    args.cmd = ["echo", "hello"]
    (tmp_path / "metadir").mkdir()

    with patch(
        "xqute.__main__.plugin.hooks.on_jobsched_started", new_callable=AsyncMock
    ):
        with patch(
            "xqute.__main__.plugin.hooks.on_jobsched_ended", new_callable=AsyncMock
        ):
            with patch(
                "asyncio.create_subprocess_exec", side_effect=asyncio.CancelledError
            ):
                with pytest.raises(asyncio.CancelledError):
                    await launch_job(args)

    assert (tmp_path / "metadir" / "job.rc").read_text() == "1"
    assert (tmp_path / "metadir" / "job.status").read_text() == str(JobStatus.FAILED)


@pytest.mark.asyncio
async def test_main(tmp_path):
    metadir = tmp_path / "metadir"
    metadir.mkdir()
    args = Namespace(
        metadir=str(metadir),
        cmd=["echo", "hello"],
        scheduler="local",
        version=__version__,
        plugin=[],
    )

    await main(args)
    assert (metadir / "job.stdout").read_text() == "hello\n"


@pytest.mark.asyncio
async def test_cli(tmp_path):
    metadir = tmp_path / "metadir"
    metadir.mkdir()

    # Use the real command line arguments
    p = subprocess.run(
        [
            "python",
            "-m",
            "xqute",
            "++scheduler",
            "local",
            "++version",
            __version__,
            "++metadir",
            str(metadir),
            "++cmd",
            "echo",
            "hello",
        ]
    )
    assert p.returncode == 0
    assert (metadir / "job.stdout").read_text() == "hello\n"
