import asyncio
import pytest
from pathlib import Path

from xqute.defaults import JobStatus
from xqute.schedulers.local_scheduler import LocalScheduler

from .conftest import BUCKET

MOCKS = Path(__file__).parent / "mocks"


async def test_with_cloud_workdir():
    scheduler = LocalScheduler(workdir=f"{BUCKET}/xqute_local_test")
    assert scheduler.subm_batch == 8
    job = await scheduler.create_job(0, ["echo", 1])
    assert str(job.metadir) == f"{BUCKET}/xqute_local_test/0"


async def test_scheduler(tmp_path):
    scheduler = LocalScheduler(tmp_path)
    job = await scheduler.create_job(0, ["echo", 1])
    pid = await scheduler.submit_job(job)
    assert isinstance(pid, int)


async def test_immediate_submission_failure(tmp_path):

    class BadLocalScheduler(LocalScheduler):
        async def wrapped_job_script(self, job, _mounted=False):
            wrapt_script = job.metadir / f"job.wrapped.{self.name}"
            await wrapt_script.a_write_text("bad_non_existent_command")
            return wrapt_script

    scheduler = BadLocalScheduler(tmp_path)
    job = await scheduler.create_job(0, ["echo", 1])
    await job.stderr_file.a_unlink(missing_ok=True)
    await job.stdout_file.a_unlink(missing_ok=True)

    with pytest.raises(RuntimeError, match=r"bad_non_existent_command.+not found"):
        await scheduler.submit_job(job)


async def test_killing_running_jobs(tmp_path):

    scheduler = LocalScheduler(forks=2, workdir=tmp_path)
    job1 = await scheduler.create_job(0, ["sleep", "10"])
    job2 = await scheduler.create_job(1, ["sleep", "10"])
    await scheduler.submit_job_and_update_status(job1)
    await scheduler.submit_job_and_update_status(job2)

    while (
        await job1.get_status(True) == JobStatus.INIT
        or await job2.get_status(True) == JobStatus.INIT
    ):
        await asyncio.sleep(0.1)
    await scheduler.kill_running_jobs([job1, job2])

    assert await job1.get_status(True) == JobStatus.FINISHED
    assert await job2.get_status(True) == JobStatus.FINISHED
    assert await job1.get_rc() != 0
    assert await job2.get_rc() != 0


async def test_cwd(tmp_path):
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    scheduler = LocalScheduler(workdir=tmp_path, cwd=cwd)
    job = await scheduler.create_job(0, ["pwd"])
    await scheduler.submit_job_and_update_status(job)
    while await job.get_status(True) != JobStatus.FINISHED:
        await asyncio.sleep(0.1)

    assert (await job.stdout_file.a_read_text()).strip() == str(cwd)
