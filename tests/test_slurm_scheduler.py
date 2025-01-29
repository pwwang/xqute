import os
import stat
import pytest
from pathlib import Path

from xqute.schedulers.slurm_scheduler import SlurmJob, SlurmScheduler
from xqute.defaults import DEFAULT_JOB_METADIR, JobStatus

MOCKS = Path(__file__).parent / "mocks"


def setup_module():
    sbatch = str(MOCKS / "sbatch")
    scancel = str(MOCKS / "scancel")
    squeue = str(MOCKS / "squeue")

    for cmd in (sbatch, scancel, squeue):
        st = os.stat(str(cmd))
        os.chmod(str(cmd), st.st_mode | stat.S_IEXEC)


@pytest.mark.asyncio
async def test_job():
    job = SlurmJob(0, ["echo", 1])
    scheduler = SlurmScheduler(
        forks=1, slurm_mem="4G", sbatch_p="gpu"
    )
    assert (
        job.wrapped_script(scheduler)
        == Path(DEFAULT_JOB_METADIR) / "0" / "job.wrapped.slurm"
    )

    script = job.wrap_script(scheduler)
    assert "#SBATCH --mem=4G" in script
    assert "#SBATCH -p gpu" in script


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
