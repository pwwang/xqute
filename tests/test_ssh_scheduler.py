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
        forks=1, ssh_conn_timeout=1
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
            "myserver": {'i': 'id_rsa', 'o': ['StrictHostKeyChecking=no']}
        },
    )
    assert (await scheduler.submit_job(job)).startswith("myserver-")
    job.jid = "myserver-1234"
    await scheduler.kill_job(job)
    if job.jid_file.is_file():
        os.unlink(job.jid_file)
    assert await scheduler.job_is_running(job) is False

    job.jid_file.write_text("")
    assert await scheduler.job_is_running(job) is False
    job.jid_file.write_text("myserver-0")
    assert await scheduler.job_is_running(job) is True
    job.jid_file.write_text("nonexist_server-0")
    assert await scheduler.job_is_running(job) is False


@pytest.mark.asyncio
async def test_submission_failure(capsys):
    job = SshJob(0, ["echo", 1])
    ssh = str(MOCKS / "nosuch_ssh")

    scheduler = SshScheduler(1, ssh=ssh)

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert job.status == JobStatus.FAILED
    assert "Failed to submit job" in job.stderr_file.read_text()
