import os
import stat
import pytest
from pathlib import Path

from xqute.schedulers.slurm_scheduler import SlurmJob, SlurmScheduler
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
    job = SlurmJob(0, ["echo", 1])
    scheduler = SlurmScheduler(
        forks=1, slurm_mem="4G", slurm_p="gpu"
    )
    assert (
        await job.wrapped_script(scheduler)
        == DEFAULT_JOB_METADIR / "0" / "job.wrapped.slurm"
    )

    wrapt = job.wrap_cmd(scheduler)
    assert "#SBATCH --mem=4G" in wrapt
    assert "#SBATCH -p gpu" in wrapt


@pytest.mark.asyncio
async def test_scheduler(capsys):
    job = SlurmJob(0, ["echo", 1])
    sbatch = str(MOCKS / "sbatch")
    scancel = str(MOCKS / "scancel")
    squeue = str(MOCKS / "squeue")

    scheduler = SlurmScheduler(1, sbatch=sbatch, scancel=scancel, squeue=squeue)
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
    job = SlurmJob(0, ["echo", 1])
    sbatch = str(MOCKS / "no_such_sbatch")
    scancel = str(MOCKS / "scancel")
    squeue = str(MOCKS / "squeue")

    scheduler = SlurmScheduler(1, sbatch=sbatch, scancel=scancel, squeue=squeue)

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert job.status == JobStatus.FAILED
    assert "Failed to submit job" in job.stderr_file.read_text()
