import pytest

from xqute.schedulers.local_scheduler import LocalJob, LocalScheduler


@pytest.mark.asyncio
async def test_scheduler():
    job = LocalJob(0, ["echo", 1])

    scheduler = LocalScheduler(1)
    pid = await scheduler.submit_job(job)
    assert isinstance(pid, int)


@pytest.mark.asyncio
async def test_immediate_submission_failure():

    class BadLocalJob(LocalJob):
        async def wrapped_script(self, scheduler):
            wrapt_script = self.metadir / f"job.wrapped.{scheduler.name}"
            wrapt_script.write_text("sleep 1; bad_non_existent_command")
            return wrapt_script

    job = BadLocalJob(0, ["echo", 1])
    job.stderr_file.unlink(missing_ok=True)
    job.stdout_file.unlink(missing_ok=True)
    scheduler = LocalScheduler(1)

    with pytest.raises(RuntimeError, match="Failed to submit job"):
        await scheduler.submit_job(job)
