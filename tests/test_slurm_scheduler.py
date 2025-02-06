import os
import stat
import pytest
from pathlib import Path

from xqute.schedulers.slurm_scheduler import SlurmScheduler
from xqute.defaults import JobStatus

# from .conftest import BUCKET

MOCKS = Path(__file__).parent / "mocks"


def setup_module():
    sbatch = str(MOCKS / "sbatch")
    scancel = str(MOCKS / "scancel")
    squeue = str(MOCKS / "squeue")

    for cmd in (sbatch, scancel, squeue):
        st = os.stat(str(cmd))
        os.chmod(str(cmd), st.st_mode | stat.S_IEXEC)


def test_job(tmp_path):
    scheduler = SlurmScheduler(
        forks=1,
        mem="4G",
        p="gpu",
        workdir=tmp_path,
    )
    job = scheduler.create_job(0, ["echo", 1])
    assert (
        scheduler.wrapped_job_script(job)
        == tmp_path / "0" / "job.wrapped.slurm"
    )

    script = scheduler.wrap_job_script(job)
    assert "#SBATCH --mem=4G" in script
    assert "#SBATCH -p gpu" in script


@pytest.mark.asyncio
async def test_scheduler(tmp_path):
    sbatch = str(MOCKS / "sbatch")
    scancel = str(MOCKS / "scancel")
    squeue = str(MOCKS / "squeue")

    scheduler = SlurmScheduler(tmp_path, sbatch=sbatch, scancel=scancel, squeue=squeue)
    job = scheduler.create_job(0, ["echo", 1])
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
async def test_submission_failure(tmp_path):
    sbatch = str(MOCKS / "no_such_sbatch")
    scancel = str(MOCKS / "scancel")
    squeue = str(MOCKS / "squeue")

    scheduler = SlurmScheduler(tmp_path, sbatch=sbatch, scancel=scancel, squeue=squeue)
    job = scheduler.create_job(0, ["echo", 1])

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert job.status == JobStatus.FAILED
    assert "Failed to submit job" in job.stderr_file.read_text()
