import os
import stat
import pytest
from pathlib import Path

from xqute.schedulers.sge_scheduler import SgeJob, SgeScheduler
from xqute.defaults import DEFAULT_JOB_METADIR

MOCKS = Path(__file__).parent / "mocks"


@pytest.mark.asyncio
async def test_job():
    job = SgeJob(0, ["echo", 1])
    scheduler = SgeScheduler(
        forks=1, sge_notify=True, sge_l=["vmem=2G", "gpu=1"], sge_m="abe"
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
    for qcmd in (qsub, qdel, qstat):
        st = os.stat(str(qcmd))
        os.chmod(str(qcmd), st.st_mode | stat.S_IEXEC)

    scheduler = SgeScheduler(1, qsub=qsub, qdel=qdel, qstat=qstat)
    assert await scheduler.submit_job(job) == "613815"
    job.uid = "613815"
    await scheduler.kill_job(job)
    if job.lock_file.is_file():
        os.unlink(job.lock_file)
    assert await scheduler.job_is_running(job) is False

    job.lock_file.write_text("0")
    assert await scheduler.job_is_running(job) is True
    job.lock_file.write_text("1")
    assert await scheduler.job_is_running(job) is False
    job.lock_file.write_text("")
    assert await scheduler.job_is_running(job) is False
