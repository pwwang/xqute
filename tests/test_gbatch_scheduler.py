import os
import stat
import pytest
from pathlib import Path

from xqute.schedulers.gbatch_scheduler import GBatchJob, GBatchScheduler
from xqute.defaults import DEFAULT_JOB_METADIR, JobStatus

MOCKS = Path(__file__).parent / "mocks"


@pytest.fixture
def gcloud():
    cmd = str(MOCKS / "gcloud")
    st = os.stat(cmd)
    os.chmod(cmd, st.st_mode | stat.S_IEXEC)
    return cmd


@pytest.mark.asyncio
async def test_job():
    job = GBatchJob(0, ["echo", 1])
    scheduler = GBatchScheduler(
        project_id="test-project", location="us-central1", jobname_prefix="jobprefix"
    )
    assert (
        job.wrapped_script(scheduler)
        == Path(DEFAULT_JOB_METADIR) / "0" / "job.wrapped.gbatch"
    )

    script = job.wrap_script(scheduler)
    assert "jobprefix.0" in script


@pytest.mark.asyncio
async def test_scheduler(capsys, gcloud):
    job = GBatchJob(0, ["echo", 1])

    scheduler = GBatchScheduler(
        project_id="test-project", location="us-central1", gcloud=gcloud
    )
    assert await scheduler.submit_job(job) == "jobprefix.1-7a1654ca-211c-40e8-b0fb-8a00"
    job.jid = "jobprefix.1-7a1654ca-211c-40e8-b0fb-8a00"
    await scheduler.kill_job(job)
    if job.jid_file.is_file():
        os.unlink(job.jid_file)
    job._jid = None
    assert await scheduler.job_is_running(job) is False

    job.jid_file.write_text("jobprefix.1-7a1654ca-211c-40e8-b0fb-8a00")
    assert await scheduler.job_is_running(job) is True

    job.jid_file.write_text("wrongjobid")
    assert await scheduler.job_is_running(job) is False


@pytest.mark.asyncio
async def test_submission_failure(capsys):
    job = GBatchJob(0, ["echo", 1])
    gcloud = str(MOCKS / "no_such_gcloud")

    scheduler = GBatchScheduler(
        project_id="test-project", location="us-central1", gcloud=gcloud
    )

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert job.status == JobStatus.FAILED
    assert "Failed to submit job" in job.stderr_file.read_text()
