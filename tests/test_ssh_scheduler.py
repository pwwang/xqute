import os
import stat
import pytest
from pathlib import Path

from xqute.schedulers.ssh_scheduler import SshJob, SshScheduler
from xqute.defaults import DEFAULT_JOB_METADIR, JobStatus

MOCKS = Path(__file__).parent / "mocks"


def setup_module():
    ssh = str(MOCKS / "ssh")

    for cmd in (ssh, ):
        st = os.stat(str(cmd))
        os.chmod(str(cmd), st.st_mode | stat.S_IEXEC)


@pytest.mark.asyncio
async def test_job():
    job = SshJob(0, ["echo", 1])
    scheduler = SshScheduler(
        forks=1, ssh_servers={"myserver": {'keyfile': 'id_rsa'}}
    )
    assert (
        await job.wrapped_script(scheduler)
        == DEFAULT_JOB_METADIR / "0" / "job.wrapped.ssh"
    )

    wrapt = job.wrap_cmd(scheduler)
    assert "cleanup()" in wrapt


@pytest.mark.asyncio
async def test_scheduler(capsys):
    job = SshJob(0, ["echo", 1])
    ssh = str(MOCKS / "ssh")

    scheduler = SshScheduler(
        1,
        ssh=ssh,
        ssh_servers={
            "myserver": {'keyfile': 'id_rsa', 'user': 'me'}
        },
    )
    assert (await scheduler.submit_job(job)).startswith("me@myserver:22/")
    job.jid = "me@myserver:22/1234"
    await scheduler.kill_job(job)
    if job.jid_file.is_file():
        os.unlink(job.jid_file)
    assert await scheduler.job_is_running(job) is False

    job.jid_file.write_text("")
    assert await scheduler.job_is_running(job) is False
    job.jid_file.write_text("me@myserver:22/0")
    assert await scheduler.job_is_running(job) is True
    job.jid_file.write_text("me@other:22/0")
    assert await scheduler.job_is_running(job) is False


@pytest.mark.asyncio
async def test_submission_failure(capsys):
    job = SshJob(0, ["echo", 1])
    ssh = str(MOCKS / "nosuch_ssh")

    scheduler = SshScheduler(1, ssh=ssh, ssh_servers={"myserver": {}})

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert job.status == JobStatus.FAILED
    assert "Failed to submit job" in job.stderr_file.read_text()


@pytest.mark.asyncio
async def test_connection_failure():
    job = SshJob(0, ["echo", 1])
    ssh = str(MOCKS / "ssh")

    scheduler = SshScheduler(
        1,
        ssh=ssh,
        ssh_servers={
            "myserverx": {'port': 44, 'keyfile': 'id_rsa', 'user': "me"}
        },
    )
    await scheduler.servers["me@myserverx:44"].disconnect()
    with pytest.raises(RuntimeError):
        await scheduler.submit_job(job)


def test_no_servers():
    with pytest.raises(ValueError):
        SshScheduler(1, ssh_servers={})
