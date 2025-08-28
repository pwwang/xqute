"""Tests for container scheduler"""
import os
import stat
import pytest  # type: ignore
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from xqute.defaults import JobStatus

from xqute.schedulers.container_scheduler import ContainerScheduler


@pytest.fixture(scope="module")
def mock_bin_path():
    """Fixture to provide path to mock binaries"""
    p = Path(__file__).parent / "mocks"
    docker_bin = p / "docker"
    docker_bin.chmod(docker_bin.stat().st_mode | stat.S_IEXEC)
    apptainer_bin = p / "apptainer"
    apptainer_bin.chmod(apptainer_bin.stat().st_mode | stat.S_IEXEC)
    return str(p)


def test_init_docker(mock_bin_path, temp_workdir):
    """Test initialization with docker"""
    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        workdir=temp_workdir,
        bin=mock_bin_path + "/docker",
    )
    assert scheduler.image == "ubuntu:20.04"
    assert scheduler.bin.endswith("docker")
    assert isinstance(scheduler.entrypoint, list)
    assert isinstance(scheduler.volumes, list)
    assert isinstance(scheduler.envs, dict)

    scheduler = ContainerScheduler(
        image="docker://ubuntu:20.04",
        workdir=temp_workdir,
        volumes=["/host/path:/container/path"],
        envs={"VAR1": "value1", "VAR2": "value2"},
        bin_args=["--privileged", "--network=host"],
    )
    assert scheduler.image == "ubuntu:20.04"
    assert "/host/path:/container/path" in scheduler.volumes
    assert scheduler.envs["VAR1"] == "value1"
    assert scheduler.envs["VAR2"] == "value2"
    assert "--privileged" in scheduler.bin_args
    assert "--network=host" in scheduler.bin_args


def test_init_binary_not_found(temp_workdir):
    """Test initialization with non-existent binary"""
    expected_msg = "Container runtime binary 'docker_not_exist' not found"
    with pytest.raises(ValueError, match=expected_msg):
        ContainerScheduler(
            image="ubuntu:20.04",
            workdir=temp_workdir,
            bin="docker_not_exist",
        )


def test_jobcmd_shebang_docker(temp_workdir):
    """Test job command shebang generation for docker"""
    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        envs={"TEST_ENV": "test_value"},
        volumes=["/host:/container"],
        workdir=temp_workdir
    )

    job = MagicMock()
    job.workdir = temp_workdir

    shebang = scheduler.jobcmd_shebang(job)

    assert "docker" in shebang
    assert "run --rm" in shebang
    assert "-e TEST_ENV=test_value" in shebang
    assert "-v /host:/container" in shebang
    assert "--workdir" in shebang
    assert "ubuntu:20.04" in shebang


def test_jobcmd_shebang_apptainer(mock_bin_path, temp_workdir):
    """Test job command shebang generation for apptainer"""
    apptainer_bin = Path(mock_bin_path) / "apptainer"

    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        bin=str(apptainer_bin),
        envs={"TEST_ENV": "test_value"},
        volumes=["/host:/container"],
        workdir=temp_workdir
    )

    job = MagicMock()
    job.workdir = temp_workdir

    shebang = scheduler.jobcmd_shebang(job)

    assert "apptainer run " in shebang
    assert "--env TEST_ENV=test_value" in shebang
    assert "--bind /host:/container" in shebang
    assert "--pwd" in shebang
    assert "ubuntu:20.04" in shebang


def test_jobcmd_shebang_with_entrypoint_list(mock_bin_path, temp_workdir):
    """Test job command shebang with entrypoint as list"""
    with patch.dict(os.environ, {"PATH": mock_bin_path}):
        scheduler = ContainerScheduler(
            image="ubuntu:20.04",
            entrypoint=["python3", "-u"],
            workdir=temp_workdir
        )

        job = MagicMock()
        job.workdir = temp_workdir

        shebang = scheduler.jobcmd_shebang(job)

        assert "python3 -u" in shebang


def test_docker_cwd(mock_bin_path, temp_workdir):
    """Test that Apptainer uses the correct working directory"""
    docker_bin = Path(mock_bin_path) / "docker"

    scheduler = ContainerScheduler(
        image="ubuntu:20.04",
        bin=str(docker_bin),
        workdir=temp_workdir,
        cwd="/custom/cwd"
    )

    job = MagicMock()
    job.workdir = temp_workdir

    shebang = scheduler.jobcmd_shebang(job)

    assert "--workdir /custom/cwd" in shebang


@pytest.fixture
def temp_workdir():
    """Fixture to provide temporary working directory"""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.mark.asyncio
async def test_scheduler(mock_bin_path, temp_workdir):

    docker_bin = Path(mock_bin_path) / "docker"

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
    job = scheduler.create_job(0, ["echo", 1])
    wrapt_script = str(scheduler.wrapped_job_script(job).mounted)
    assert wrapt_script == str(mounted_dir / "0" / "job.wrapped.docker")

    pid = await scheduler.submit_job(job)
    assert isinstance(pid, int)


@pytest.mark.asyncio
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
    job = scheduler.create_job(0, ["echo", 1])

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert job.status == JobStatus.FAILED
    assert "Failed to submit job" in job.stderr_file.read_text()
