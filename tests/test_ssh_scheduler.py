import os
import stat
import pytest
from pathlib import Path

from xqute.schedulers.ssh_scheduler import SshScheduler
from xqute.defaults import JobStatus

MOCKS = Path(__file__).parent / "mocks"


def setup_module():
    ssh = str(MOCKS / "ssh")

    for cmd in (ssh,):
        st = os.stat(str(cmd))
        os.chmod(str(cmd), st.st_mode | stat.S_IEXEC)


@pytest.mark.asyncio
async def test_job(tmp_path):
    scheduler = SshScheduler(tmp_path, servers={"myserver": {"keyfile": "id_rsa"}})
    job = scheduler.create_job(0, ["echo", 1])
    assert (
        scheduler.wrapped_job_script(job)
        == tmp_path / "0" / "job.wrapped.ssh"
    )

    script = scheduler.wrap_job_script(job)
    assert "#!" in script


@pytest.mark.asyncio
async def test_scheduler(tmp_path):
    ssh = str(MOCKS / "ssh")

    scheduler = SshScheduler(
        ssh=ssh,
        servers={"myserver": {"keyfile": "id_rsa", "user": "me"}},
        workdir=tmp_path,
    )
    job = scheduler.create_job(0, ["echo", 1])
    assert (await scheduler.submit_job(job)).endswith("@me@myserver:22")
    # trigger skipping re-connect
    assert (await scheduler.submit_job(job)).endswith("@me@myserver:22")
    job.jid = "1234@me@myserver:22"
    await scheduler.kill_job(job)
    if job.jid_file.is_file():
        os.unlink(job.jid_file)
    assert await scheduler.job_is_running(job) is False

    job.jid_file.write_text("")
    assert await scheduler.job_is_running(job) is False
    job.jid_file.write_text("0@me@myserver:22")
    assert await scheduler.job_is_running(job) is True
    job.jid_file.write_text("0@me@other:22")
    assert await scheduler.job_is_running(job) is False


@pytest.mark.asyncio
async def test_submission_failure(tmp_path):
    ssh = str(MOCKS / "nosuch_ssh")

    scheduler = SshScheduler(tmp_path, ssh=ssh, servers={"myserver": {}})
    job = scheduler.create_job(0, ["echo", 1])

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert job.status == JobStatus.FAILED
    assert "Failed to submit job" in job.stderr_file.read_text()


@pytest.mark.asyncio
async def test_submission_failure_with_server_list(tmp_path):
    ssh = str(MOCKS / "nosuch_ssh")

    scheduler = SshScheduler(tmp_path, ssh=ssh, servers=["myserver"])
    job = scheduler.create_job(0, ["echo", 1])

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert job.status == JobStatus.FAILED
    assert "Failed to submit job" in job.stderr_file.read_text()


@pytest.mark.asyncio
async def test_connection_failure(tmp_path):
    ssh = str(MOCKS / "ssh")

    scheduler = SshScheduler(
        ssh=ssh,
        servers={"myserverx": {"port": 44, "keyfile": "id_rsa", "user": "me"}},
        workdir=tmp_path,
    )
    job = scheduler.create_job(0, ["echo", 1])
    server = scheduler.servers["me@myserverx:44"]
    # in case previous connection file exists
    scheduler.servers["me@myserverx:44"].disconnect()
    # port will make it fail in mock
    # await server.connect()
    assert not server.is_connected
    scheduler.servers["me@myserverx:44"].disconnect()
    with pytest.raises(RuntimeError):
        await scheduler.submit_job(job)


def test_no_servers(tmp_path):
    with pytest.raises(ValueError):
        SshScheduler(tmp_path, servers={})


@pytest.mark.asyncio
async def test_immediate_submission_failure(tmp_path):
    ssh = str(MOCKS / "ssh")

    class BadSshScheduler(SshScheduler):
        def wrapped_job_script(self, job):
            wrapt_script = job.metadir / f"job.wrapped.{self.name}"
            wrapt_script.write_text("sleep 1; bad_non_existent_command")
            return wrapt_script

    scheduler = BadSshScheduler(tmp_path, ssh=ssh, servers=["myserver"])
    job = scheduler.create_job(0, ["echo", 1])
    job.stderr_file.unlink(missing_ok=True)
    job.stdout_file.unlink(missing_ok=True)

    with pytest.raises(RuntimeError, match="Failed to submit job"):
        await scheduler.submit_job(job)


@pytest.mark.asyncio
async def test_immediate_submission_failure2(tmp_path):
    """No stdout/stderr files generated but submission finished"""
    ssh = str(MOCKS / "ssh")

    class BadSshScheduler(SshScheduler):
        def wrapped_job_script(self, job):
            wrapt_script = job.metadir / f"job.wrapped.{self.name}"
            wrapt_script.write_text("echo 1")
            return wrapt_script

    scheduler = BadSshScheduler(tmp_path, ssh=ssh, servers=["myserver"])
    job = scheduler.create_job(0, ["echo", 1])
    job.stderr_file.unlink(missing_ok=True)
    job.stdout_file.unlink(missing_ok=True)

    with pytest.raises(RuntimeError, match="Failed to submit job"):
        await scheduler.submit_job(job)
