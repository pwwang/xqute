import pytest  # noqa: F401
import os
import stat
from pathlib import Path

from xqute.schedulers.slurm_scheduler import SlurmScheduler
from xqute.defaults import JobStatus

MOCKS = Path(__file__).parent / "mocks"


def setup_module():
    sbatch = str(MOCKS / "sbatch")
    scancel = str(MOCKS / "scancel")
    squeue = str(MOCKS / "squeue")

    for cmd in (sbatch, scancel, squeue):
        st = os.stat(str(cmd))
        os.chmod(str(cmd), st.st_mode | stat.S_IEXEC)


async def test_job(tmp_path):
    scheduler = SlurmScheduler(
        forks=1,
        mem="4G",
        p="gpu",
        workdir=tmp_path,
    )
    job = await scheduler.create_job(0, ["echo", 1])
    assert (
        await scheduler.wrapped_job_script(job)
        == tmp_path / "0" / "job.wrapped.slurm"
    )

    script = scheduler.wrap_job_script(job)
    assert "#SBATCH --mem=4G" in script
    assert "#SBATCH -p gpu" in script


async def test_cwd(tmp_path):
    scheduler = SlurmScheduler(
        forks=1,
        mem="4G",
        p="gpu",
        workdir=tmp_path,
        cwd="/tmp/cwd",
    )
    job = await scheduler.create_job(0, ["echo", 1])

    script = scheduler.wrap_job_script(job)
    assert "#SBATCH --chdir=/tmp/cwd" in script


async def test_scheduler(tmp_path):
    sbatch = str(MOCKS / "sbatch")
    scancel = str(MOCKS / "scancel")
    squeue = str(MOCKS / "squeue")

    scheduler = SlurmScheduler(tmp_path, sbatch=sbatch, scancel=scancel, squeue=squeue)
    job = await scheduler.create_job(0, ["echo", 1])
    assert await scheduler.submit_job(job) == "613815"
    await job.set_jid("613815")
    await scheduler.kill_job(job)
    if await job.jid_file.a_is_file():
        await job.jid_file.a_unlink()
    assert await scheduler.job_is_running(job) is False

    await job.jid_file.a_write_text("0")
    assert await scheduler.job_is_running(job) is True
    await job.jid_file.a_write_text("1")
    assert await scheduler.job_is_running(job) is False
    await job.jid_file.a_write_text("")
    assert await scheduler.job_is_running(job) is False


async def test_submission_failure(tmp_path):
    sbatch = str(MOCKS / "no_such_sbatch")
    scancel = str(MOCKS / "scancel")
    squeue = str(MOCKS / "squeue")

    scheduler = SlurmScheduler(tmp_path, sbatch=sbatch, scancel=scancel, squeue=squeue)
    job = await scheduler.create_job(0, ["echo", 1])

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert await job.get_status(True) == JobStatus.FAILED
    assert "Failed to submit job" in await job.stderr_file.a_read_text()
