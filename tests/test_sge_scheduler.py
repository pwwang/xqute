import os
import stat
import pytest
from pathlib import Path

from xqute.schedulers.sge_scheduler import SgeJob, SgeScheduler
from xqute.defaults import DEFAULT_JOB_METADIR, JobStatus

MOCKS = Path(__file__).parent / "mocks"


def setup_module():
    qsub = str(MOCKS / "qsub")
    qdel = str(MOCKS / "qdel")
    qstat = str(MOCKS / "qstat")

    for qcmd in (qsub, qdel, qstat):
        st = os.stat(str(qcmd))
        os.chmod(str(qcmd), st.st_mode | stat.S_IEXEC)


@pytest.mark.asyncio
async def test_job():
    job = SgeJob(0, ["echo", 1])
    scheduler = SgeScheduler(
        forks=1, qsub_notify=True, sge_l=["vmem=2G", "gpu=1"], sge_m="abe"
    )
    assert (
        await job.wrapped_script(scheduler)
        == DEFAULT_JOB_METADIR / "0" / "job.wrapped.sge"
    )

    wrapt = job.wrap_cmd(scheduler)
    assert "#$ -notify" in wrapt
    assert "#$ -l vmem=2G" in wrapt
    assert "#$ -l gpu=1" in wrapt
    assert "#$ -m abe" in wrapt


@pytest.mark.asyncio
async def test_scheduler(capsys):
    job = SgeJob(0, ["echo", 1])
    qsub = str(MOCKS / "qsub")
    qdel = str(MOCKS / "qdel")
    qstat = str(MOCKS / "qstat")

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
async def test_submission_failure(capsys):
    job = SgeJob(0, ["echo", 1])
    qsub = str(MOCKS / "no_such_qsub")
    qdel = str(MOCKS / "qdel")
    qstat = str(MOCKS / "qstat")

    scheduler = SgeScheduler(1, qsub=qsub, qdel=qdel, qstat=qstat)

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert job.status == JobStatus.FAILED
    assert "Failed to submit job" in job.stderr_file.read_text()
