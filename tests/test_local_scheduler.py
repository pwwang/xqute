import asyncio
import pytest

from xqute.defaults import JobStatus
from xqute.schedulers.local_scheduler import LocalJob, LocalScheduler


@pytest.mark.asyncio
async def test_scheduler():
    job = LocalJob(0, ["echo", 1])

    scheduler = LocalScheduler(1)
    pid = await scheduler.submit_job(job)
    assert isinstance(pid, int)


@pytest.mark.asyncio
async def test_scheduler_with_meta_gs():
    job = LocalJob(
        0,
        ["echo", 1],
        metadir="gs://handy-buffer-287000.appspot.com/xqute_local_test",
    )

    scheduler = LocalScheduler(1)
    pid = await scheduler.submit_job(job)

    assert isinstance(pid, int)
    assert job.stdout_file.read_text() == "1\n"


@pytest.mark.asyncio
async def test_immediate_submission_failure():

    class BadLocalJob(LocalJob):
        def wrapped_script(self, scheduler):
            wrapt_script = self.metadir / f"job.wrapped.{scheduler.name}"
            wrapt_script.write_text("sleep 1; bad_non_existent_command")
            return wrapt_script

    job = BadLocalJob(0, ["echo", 1])
    job.stderr_file.unlink(missing_ok=True)
    job.stdout_file.unlink(missing_ok=True)
    scheduler = LocalScheduler(1)

    with pytest.raises(
        RuntimeError, match="bad_non_existent_command: command not found"
    ):
        await scheduler.submit_job(job)


@pytest.mark.asyncio
async def test_killing_running_jobs(caplog):
    job1 = LocalJob(0, ["sleep", "10"])
    job2 = LocalJob(1, ["sleep", "10"])

    scheduler = LocalScheduler(2)
    await scheduler.submit_job_and_update_status(job1)
    await scheduler.submit_job_and_update_status(job2)

    await asyncio.sleep(1)
    await scheduler.kill_running_jobs([job1, job2])

    assert job1.status == JobStatus.FINISHED
    assert job2.status == JobStatus.FINISHED
    assert job1.rc != 0
    assert job2.rc != 0
