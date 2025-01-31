import os
import stat
import pytest
from cloudpathlib import AnyPath
from pathlib import Path
from uuid import uuid4

from xqute.schedulers.gbatch_scheduler import GbatchScheduler
from xqute.defaults import JobStatus

MOCKS = Path(__file__).parent / "mocks"
# Make a fixture to get a unique workdir directory each time
WORKDIR = AnyPath("gs://handy-buffer-287000.appspot.com/xqute_gbatch_test")
WORKDIR = WORKDIR / str(uuid4())


def teardown_module():
    WORKDIR.rmtree()


@pytest.fixture
def gcloud():
    cmd = str(MOCKS / "gcloud")
    st = os.stat(cmd)
    os.chmod(cmd, st.st_mode | stat.S_IEXEC)
    return cmd


@pytest.mark.asyncio
async def test_job():
    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        jobname_prefix="jobprefix",
        workdir=WORKDIR,
    )
    job = scheduler.create_job(0, ["echo", 1])
    assert (
        job.wrapped_script(scheduler) == scheduler.workdir / "0" / "job.wrapped.gbatch"
    )

    script = job.wrap_script(scheduler)
    assert "/mnt/.xqute_workdir/0/job.status" in script


@pytest.mark.asyncio
async def test_scheduler(gcloud):

    scheduler = GbatchScheduler(
        project="test-project",
        location="us-central1",
        gcloud=gcloud,
        workdir=WORKDIR,
    )
    job = scheduler.create_job(0, ["echo", 1])
    assert (await scheduler.submit_job(job)).startswith("gbatch-")
    job.jid = "gbatch-36760976-0"
    await scheduler.kill_job(job)
    if job.jid_file.is_file():
        job.jid_file.unlink()
    job._jid = None
    assert not job.jid_file.is_file()
    assert await scheduler.job_is_running(job) is False

    job.jid_file.write_text("gbatch-36760976-0")
    assert await scheduler.job_is_running(job) is True

    job.jid_file.write_text("wrongjobid")
    assert await scheduler.job_is_running(job) is False
    job.jid_file.unlink()


@pytest.mark.asyncio
async def test_submission_failure():
    gcloud = str(MOCKS / "no_such_gcloud")

    scheduler = GbatchScheduler(
        project="test-project", location="us-central1", gcloud=gcloud,
        workdir=WORKDIR,
    )
    job = scheduler.create_job(0, ["echo", 1])

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert job.status == JobStatus.FAILED
    assert "Failed to submit job" in job.stderr_file.read_text()
