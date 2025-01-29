import os
import stat
import pytest
from pathlib import Path

from xqute.schedulers.sge_scheduler import SgeJob, SgeScheduler
from xqute.defaults import DEFAULT_JOB_METADIR, JobStatus

MOCKS = Path(__file__).parent / "mocks"


@pytest.fixture
def qsub():
    cmd = str(MOCKS / "qsub")
    st = os.stat(cmd)
    os.chmod(cmd, st.st_mode | stat.S_IEXEC)
    return cmd


@pytest.fixture
def qdel():
    cmd = str(MOCKS / "qdel")
    st = os.stat(cmd)
    os.chmod(cmd, st.st_mode | stat.S_IEXEC)
    return cmd


@pytest.fixture
def qstat():
    cmd = str(MOCKS / "qstat")
    st = os.stat(cmd)
    os.chmod(cmd, st.st_mode | stat.S_IEXEC)
    return cmd


@pytest.mark.asyncio
async def test_job():
    job = SgeJob(0, ["echo", 1])
    scheduler = SgeScheduler(
        forks=1, qsub_notify=True, sge_l=["vmem=2G", "gpu=1"], sge_m="abe"
    )
    assert (
        job.wrapped_script(scheduler)
        == Path(DEFAULT_JOB_METADIR) / "0" / "job.wrapped.sge"
    )

    script = job.wrap_script(scheduler)
    assert "#$ -notify" in script
    assert "#$ -l vmem=2G" in script
    assert "#$ -l gpu=1" in script
    assert "#$ -m abe" in script


@pytest.mark.asyncio
async def test_scheduler(capsys, qsub, qdel, qstat):
    job = SgeJob(0, ["echo", 1])

    scheduler = SgeScheduler(1, qsub=qsub, qdel=qdel, qstat=qstat)
    assert await scheduler.submit_job(job) == "613815"
    job.jid = "613815"
    await scheduler.kill_job(job)
    if job.jid_file.is_file():
        os.unlink(job.jid_file)
    assert await scheduler.job_is_running(job) is False

    job.jid_file.write_text("0")
    assert await scheduler.job_is_running(job) is True
    job.jid_file.write_text("1")
    assert await scheduler.job_is_running(job) is False
    job.jid_file.write_text("")
    assert await scheduler.job_is_running(job) is False


@pytest.mark.asyncio
async def test_submission_failure(capsys, qdel, qstat):
    job = SgeJob(0, ["echo", 1])

    scheduler = SgeScheduler(1, qsub="no_such_qsub", qdel=qdel, qstat=qstat)

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert job.status == JobStatus.FAILED
    assert "Failed to submit job" in job.stderr_file.read_text()
