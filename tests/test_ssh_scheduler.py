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


async def test_job(tmp_path):
    scheduler = SshScheduler(tmp_path, servers={"myserver": {"keyfile": "id_rsa"}})
    job = await scheduler.create_job(0, ["echo", 1])
    assert (
        await scheduler.wrapped_job_script(job)
        == tmp_path / "0" / "job.wrapped.ssh"
    )

    script = scheduler.wrap_job_script(job)
    assert "#!" in script


async def test_scheduler(tmp_path):
    ssh = str(MOCKS / "ssh")

    scheduler = SshScheduler(
        ssh=ssh,
        servers={"myserver": {"keyfile": "id_rsa", "user": "me"}},
        workdir=tmp_path,
    )
    job = await scheduler.create_job(0, ["echo", 1])
    assert (await scheduler.submit_job(job)).endswith("@me@myserver:22")
    # trigger skipping re-connect
    assert (await scheduler.submit_job(job)).endswith("@me@myserver:22")
    await job.set_jid("1234@me@myserver:22")
    await scheduler.kill_job(job)
    if await job.jid_file.a_is_file():
        await job.jid_file.a_unlink()
    assert await scheduler.job_is_running(job) is False

    await job.jid_file.a_write_text("")
    assert await scheduler.job_is_running(job) is False
    await job.jid_file.a_write_text("0@me@myserver:22")
    assert await scheduler.job_is_running(job) is True
    await job.jid_file.a_write_text("0@me@other:22")
    assert await scheduler.job_is_running(job) is False


async def test_submission_failure(tmp_path):
    ssh = str(MOCKS / "nosuch_ssh")

    scheduler = SshScheduler(tmp_path, ssh=ssh, servers={"myserver": {}})
    job = await scheduler.create_job(0, ["echo", 1])

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert await job.get_status(True) == JobStatus.FAILED
    assert "Failed to submit job" in await job.stderr_file.a_read_text()


async def test_submission_failure_with_server_list(tmp_path):
    ssh = str(MOCKS / "nosuch_ssh")

    scheduler = SshScheduler(tmp_path, ssh=ssh, servers=["myserver"])
    job = await scheduler.create_job(0, ["echo", 1])

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert await job.get_status(True) == JobStatus.FAILED
    assert "Failed to submit job" in await job.stderr_file.a_read_text()


async def test_connection_failure(tmp_path):
    ssh = str(MOCKS / "ssh")

    scheduler = SshScheduler(
        ssh=ssh,
        servers={"myserverx": {"port": 44, "keyfile": "id_rsa", "user": "me"}},
        workdir=tmp_path,
    )
    job = await scheduler.create_job(0, ["echo", 1])
    server = scheduler.servers["me@myserverx:44"]
    # in case previous connection file exists
    await scheduler.servers["me@myserverx:44"].disconnect()
    # port will make it fail in mock
    # await server.connect()
    assert not await server.is_connected
    await scheduler.servers["me@myserverx:44"].disconnect()
    with pytest.raises(RuntimeError):
        await scheduler.submit_job(job)


def test_no_servers(tmp_path):
    with pytest.raises(ValueError):
        SshScheduler(tmp_path, servers={})


async def test_immediate_submission_failure(tmp_path):
    ssh = str(MOCKS / "ssh")

    class BadSshScheduler(SshScheduler):
        async def wrapped_job_script(self, job, _mounted=False):
            wrapt_script = job.metadir / f"job.wrapped.{self.name}"
            await wrapt_script.a_write_text("sleep 1; bad_non_existent_command")
            return wrapt_script

    scheduler = BadSshScheduler(tmp_path, ssh=ssh, servers=["myserver"])
    job = await scheduler.create_job(0, ["echo", 1])
    await job.stderr_file.a_unlink(missing_ok=True)
    await job.stdout_file.a_unlink(missing_ok=True)

    with pytest.raises(RuntimeError, match="Failed to submit job"):
        await scheduler.submit_job(job)


async def test_immediate_submission_failure2(tmp_path):
    """No stdout/stderr files generated but submission finished"""
    ssh = str(MOCKS / "ssh")

    class BadSshScheduler(SshScheduler):
        async def wrapped_job_script(self, job, _mounted=False):
            wrapt_script = job.metadir / f"job.wrapped.{self.name}"
            await wrapt_script.a_write_text("echo 1")
            return wrapt_script

    scheduler = BadSshScheduler(tmp_path, ssh=ssh, servers=["myserver"])
    job = await scheduler.create_job(0, ["echo", 1])
    await job.stderr_file.a_unlink(missing_ok=True)
    await job.stdout_file.a_unlink(missing_ok=True)

    with pytest.raises(RuntimeError, match="Failed to submit job"):
        await scheduler.submit_job(job)
