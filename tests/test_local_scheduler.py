import asyncio
import pytest
from pathlib import Path

from xqute.defaults import JobStatus
from xqute.schedulers.local_scheduler import LocalScheduler

from .conftest import BUCKET

MOCKS = Path(__file__).parent / "mocks"


def test_with_cloud_workdir():
    scheduler = LocalScheduler(workdir=f"{BUCKET}/xqute_local_test")
    job = scheduler.create_job(0, ["echo", 1])
    assert str(job.metadir) == f"{BUCKET}/xqute_local_test/0"


@pytest.mark.asyncio
async def test_scheduler(tmp_path):
    scheduler = LocalScheduler(tmp_path)
    job = scheduler.create_job(0, ["echo", 1])
    pid = await scheduler.submit_job(job)
    assert isinstance(pid, int)


@pytest.mark.asyncio
async def test_immediate_submission_failure(tmp_path):

    class BadLocalScheduler(LocalScheduler):
        def wrapped_job_script(self, job):
            wrapt_script = job.metadir / f"job.wrapped.{self.name}"
            wrapt_script.write_text("sleep 1; bad_non_existent_command")
            return wrapt_script

    scheduler = BadLocalScheduler(tmp_path)
    job = scheduler.create_job(0, ["echo", 1])
    job.stderr_file.unlink(missing_ok=True)
    job.stdout_file.unlink(missing_ok=True)

    with pytest.raises(
        RuntimeError, match=r"bad_non_existent_command.+not found"
    ):
        await scheduler.submit_job(job)


@pytest.mark.asyncio
async def test_killing_running_jobs(tmp_path):

    scheduler = LocalScheduler(forks=2, workdir=tmp_path)
    job1 = scheduler.create_job(0, ["sleep", "10"])
    job2 = scheduler.create_job(1, ["sleep", "10"])
    await scheduler.submit_job_and_update_status(job1)
    await scheduler.submit_job_and_update_status(job2)

    while job1.status == JobStatus.INIT or job2.status == JobStatus.INIT:
        await asyncio.sleep(.1)
    await scheduler.kill_running_jobs([job1, job2])

    assert job1.status == JobStatus.FINISHED
    assert job2.status == JobStatus.FINISHED
    assert job1.rc != 0
    assert job2.rc != 0


@pytest.mark.asyncio
async def test_cwd(tmp_path):
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    scheduler = LocalScheduler(workdir=tmp_path, cwd=cwd)
    job = scheduler.create_job(0, ["pwd"])
    await scheduler.submit_job_and_update_status(job)
    while job.status == JobStatus.INIT:
        await asyncio.sleep(.1)

    assert job.stdout_file.read_text().strip() == str(cwd)
