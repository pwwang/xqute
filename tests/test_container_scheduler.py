"""Tests for container scheduler"""

import os
import stat
import pytest  # type: ignore
import shlex
import tempfile
from panpath import PanPath
from unittest.mock import patch, MagicMock
from xqute.defaults import JobStatus

from xqute.schedulers.container_scheduler import ContainerScheduler


@pytest.fixture(scope="module")
def mock_bin_path():
    """Fixture to provide path to mock binaries"""
    p = PanPath(__file__).parent / "mocks"
    docker_bin = p / "docker"
    docker_bin.chmod(docker_bin.stat().st_mode | stat.S_IEXEC)
    apptainer_bin = p / "apptainer"
    apptainer_bin.chmod(apptainer_bin.stat().st_mode | stat.S_IEXEC)
    return str(p)


async def test_init_docker(mock_bin_path, temp_workdir):
    """Test initialization with docker"""
    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        workdir=temp_workdir,
        bin=mock_bin_path + "/docker",
    )
    assert scheduler.image == "ubuntu:20.04"
    assert scheduler.bin.endswith("docker")  # type: ignore
    assert isinstance(scheduler.entrypoint, list)
    assert isinstance(scheduler.volumes, list)
    # assert isinstance(scheduler.envs, dict)

    scheduler = ContainerScheduler(
        image="docker://ubuntu:20.04",
        workdir=temp_workdir,
        volumes=["/tmp:/container/path"],
        # envs={"VAR1": "value1", "VAR2": "value2"},
        bin_args=["--privileged", "--network=host"],
    )
    await scheduler.post_init()
    assert scheduler.image == "ubuntu:20.04"
    assert "/tmp:/container/path" in scheduler.volumes
    # assert scheduler.envs["VAR1"] == "value1"
    # assert scheduler.envs["VAR2"] == "value2"
    assert "--privileged" in scheduler.bin_args
    assert "--network=host" in scheduler.bin_args


def test_init_scheduler_using_mount_and_volumes():
    """Test initialization with 'mount' and 'volumes'"""
    expected_msg = (
        "You can't specify both 'mount' and 'volumes' arguments. "
        "Use only one of them."
    )
    with pytest.raises(ValueError, match=expected_msg):
        ContainerScheduler(
            image="ubuntu:20.04",
            workdir="/tmp",
            mount=["/host/path:/container/path"],  # type: ignore
            volumes=["/host/path:/container/path"],  # type: ignore
        )

    expected_msg = (
        "You can't specify both 'mount_as_cwd' and 'volume_as_cwd' arguments. "
        "Use only one of them."
    )
    with pytest.raises(ValueError, match=expected_msg):
        ContainerScheduler(
            image="ubuntu:20.04",
            workdir="/tmp",
            mount_as_cwd="/host/path",  # type: ignore
            volume_as_cwd="/host/path",  # type: ignore
        )

    expected_msg = (
        "You can't specify both 'volume_as_cwd' and 'cwd' arguments. "
        "Use only one of them."
    )
    with pytest.raises(ValueError, match=expected_msg):
        ContainerScheduler(
            image="ubuntu:20.04",
            workdir="/tmp",
            volume_as_cwd="/host/path",
            cwd="/container/path"
        )


async def test_named_volume_handling(temp_workdir):
    """Test handling of named volumes"""
    host_dir = temp_workdir / "dir1"
    await host_dir.a_mkdir(parents=True, exist_ok=True)
    host_file = temp_workdir / "dir2" / "file.txt"
    await host_file.parent.a_mkdir(parents=True, exist_ok=True)
    await host_file.a_write_text("test content")
    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        workdir=temp_workdir,
        volumes=[f"DIR={host_dir}", f"FILE={host_file}"],
    )
    await scheduler.post_init()
    assert len(scheduler.volumes) == 3
    assert (
        scheduler.volumes[0]
        == f"{str(host_dir)}:{ContainerScheduler.DEFAULT_MOUNTED_ROOT}/NAMED_MOUNTS/DIR"
    )
    assert scheduler.volumes[1] == (
        f"{str(host_file.parent)}:{ContainerScheduler.DEFAULT_MOUNTED_ROOT}/"
        "NAMED_MOUNTS/FILE/dir2"
    )
    assert (
        scheduler._path_envs["DIR"]
        == f"{ContainerScheduler.DEFAULT_MOUNTED_ROOT}/NAMED_MOUNTS/DIR"
    )
    assert (
        scheduler._path_envs["FILE"]
        == f"{ContainerScheduler.DEFAULT_MOUNTED_ROOT}/NAMED_MOUNTS/FILE/dir2/file.txt"
    )

    job = await scheduler.create_job(0, ["echo", "Hello"])
    init_cmd = scheduler.jobcmd_init(job)
    assert f"export DIR={shlex.quote(scheduler._path_envs['DIR'])}" in init_cmd
    assert f"export FILE={shlex.quote(scheduler._path_envs['FILE'])}" in init_cmd

    sched = ContainerScheduler(
        image="ubuntu:20.04",
        workdir=temp_workdir,
        volumes=["DATA=/non/existent/path"],
    )
    with pytest.raises(FileNotFoundError):
        await sched.post_init()


async def test_volume_as_cwd_no_workdir():
    """Test that volume_as_cwd without workdir"""
    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        volume_as_cwd="/tmp",
    )
    await scheduler.post_init()
    assert scheduler.cwd == "/mnt/disks/.cwd"
    assert "cd /mnt/disks/.cwd" in scheduler.jobcmd_wrapper_init
    assert str(scheduler.workdir) == "/tmp/.xqute"
    assert str(scheduler.workdir.mounted) == "/mnt/disks/.cwd/.xqute"
    volumes = scheduler.volumes
    assert len(volumes) == 1
    assert volumes[0] == "/tmp:/mnt/disks/.cwd"


async def test_volume_as_cwd_and_workdir(temp_workdir):
    """Test that volume_as_cwd and workdir are handled correctly"""
    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        workdir=temp_workdir,
        volume_as_cwd="/tmp",
    )
    await scheduler.post_init()
    assert scheduler.cwd == "/mnt/disks/.cwd"
    assert "cd /mnt/disks/.cwd" in scheduler.jobcmd_wrapper_init
    assert scheduler.workdir == temp_workdir
    assert str(scheduler.workdir.mounted) == "/mnt/disks/.xqute"
    volumes = scheduler.volumes
    assert len(volumes) == 2
    assert f"{temp_workdir}:/mnt/disks/.xqute" in volumes
    assert "/tmp:/mnt/disks/.cwd" in volumes


async def test_volume_as_cwd_and_mounted_workdir(temp_workdir):
    """Test that volume_as_cwd and mounted_workdir are handled correctly"""
    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        mounted_workdir="/container/workdir",
        volume_as_cwd="/tmp",
    )
    await scheduler.post_init()
    assert scheduler.cwd == "/mnt/disks/.cwd"
    assert "cd /mnt/disks/.cwd" in scheduler.jobcmd_wrapper_init
    assert str(scheduler.workdir) == "/tmp/.xqute"
    assert str(scheduler.workdir.mounted) == "/container/workdir"
    volumes = scheduler.volumes
    assert len(volumes) == 2
    assert "/tmp/.xqute:/container/workdir" in volumes
    assert "/tmp:/mnt/disks/.cwd" in volumes


def test_init_binary_not_found(temp_workdir):
    """Test initialization with non-existent binary"""
    expected_msg = "Container runtime binary 'docker_not_exist' not found"
    with pytest.raises(ValueError, match=expected_msg):
        ContainerScheduler(
            image="ubuntu:20.04",
            workdir=temp_workdir,
            bin="docker_not_exist",
        )


async def test_jobcmd_shebang_docker(temp_workdir):
    """Test job command shebang generation for docker"""
    scheduler = ContainerScheduler(
        image="ubuntu:20.04", volumes=["/tmp:/container"], workdir=temp_workdir
    )
    await scheduler.post_init()

    job = MagicMock(envs={"TEST_ENV": "test_value"})
    job.workdir = temp_workdir

    shebang = scheduler.jobcmd_shebang(job)

    assert "docker" in shebang
    assert "run --rm" in shebang
    assert "-e TEST_ENV=test_value" in shebang
    assert "-v /tmp:/container" in shebang
    assert "--workdir" in shebang
    assert "ubuntu:20.04" in shebang


async def test_jobcmd_shebang_apptainer(mock_bin_path, temp_workdir):
    """Test job command shebang generation for apptainer"""
    apptainer_bin = PanPath(mock_bin_path) / "apptainer"

    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        bin=str(apptainer_bin),
        # envs={"TEST_ENV": "test_value"},
        volumes=["/tmp:/container"],
        workdir=temp_workdir,
    )
    await scheduler.post_init()
    job = MagicMock(envs={"TEST_ENV": "test_value"})
    job.workdir = temp_workdir

    shebang = scheduler.jobcmd_shebang(job)

    assert "apptainer run " in shebang
    assert "--env TEST_ENV=test_value" in shebang
    assert "--bind /tmp:/container" in shebang
    assert "--pwd" in shebang
    assert "ubuntu:20.04" in shebang


def test_jobcmd_shebang_with_entrypoint_list(mock_bin_path, temp_workdir):
    """Test job command shebang with entrypoint as list"""
    with patch.dict(os.environ, {"PATH": mock_bin_path}):
        scheduler = ContainerScheduler(
            image="ubuntu:20.04", entrypoint=["python3", "-u"], workdir=temp_workdir
        )

        job = MagicMock()
        job.workdir = temp_workdir

        shebang = scheduler.jobcmd_shebang(job)

        assert "python3 -u" in shebang


def test_docker_cwd(mock_bin_path, temp_workdir):
    """Test that Apptainer uses the correct working directory"""
    docker_bin = PanPath(mock_bin_path) / "docker"

    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        bin=str(docker_bin),
        workdir=temp_workdir,
        cwd="/custom/cwd",
    )

    job = MagicMock()
    job.workdir = temp_workdir

    shebang = scheduler.jobcmd_shebang(job)

    assert "--workdir /custom/cwd" in shebang


@pytest.fixture
def temp_workdir():
    """Fixture to provide temporary working directory"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield PanPath(tmpdir)


async def test_scheduler(mock_bin_path, temp_workdir):

    docker_bin = PanPath(mock_bin_path) / "docker"

    host_dir = temp_workdir / "host"
    mounted_dir = temp_workdir / "mounted"
    host_dir.mkdir(parents=True, exist_ok=True)
    mounted_dir.symlink_to(host_dir)

    scheduler = ContainerScheduler(
        bin=str(docker_bin),
        image="ubuntu:20.04",
        workdir=host_dir,
        mounted_workdir=mounted_dir,
    )
    job = await scheduler.create_job(0, ["echo", "1"])
    wrapt_script = str((await scheduler.wrapped_job_script(job)).mounted)
    assert wrapt_script == str(mounted_dir / "0" / "job.wrapped.container-docker")

    pid = await scheduler.submit_job(job)
    assert isinstance(pid, int)


async def test_submission_failure(temp_workdir):

    host_dir = temp_workdir / "host"
    mounted_dir = temp_workdir / "mounted"
    host_dir.mkdir(parents=True, exist_ok=True)
    mounted_dir.symlink_to(host_dir)

    scheduler = ContainerScheduler(
        bin="false",
        image="ubuntu:20.04",
        workdir=host_dir,
        mounted_workdir=mounted_dir,
    )
    job = await scheduler.create_job(0, ["echo", "1"])

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert await job.get_status(True) == JobStatus.FAILED
    assert "Failed to submit job" in await job.stderr_file.a_read_text()


async def test_using_cwd():
    """Test that using cwd is handled correctly"""
    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        mounted_workdir="/tmp/workdir",
        mount="/tmp:/container",
        cwd="/container/cwd",
    )
    await scheduler.post_init()
    assert scheduler.cwd == "/container/cwd"
    assert "cd /container/cwd" in scheduler.jobcmd_wrapper_init
    assert str(scheduler.workdir) == "/tmp/cwd/.xqute"
    assert str(scheduler.workdir.mounted) == "/tmp/workdir"
    volumes = scheduler.volumes
    assert len(volumes) == 2
    assert volumes[0] == "/tmp:/container"
    assert volumes[1] == "/tmp/cwd/.xqute:/tmp/workdir"


async def test_using_cwd_but_mount_not_found():
    """Test that using cwd with a non-existent mount raises an error"""
    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        mount="/tmp:/container",
        cwd="/nonexisting/cwd",
    )
    with pytest.raises(ValueError):
        await scheduler.post_init()
