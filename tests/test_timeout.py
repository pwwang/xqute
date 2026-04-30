"""Tests for job timeout feature"""

import pytest
from xqute import Xqute, JobStatus


@pytest.mark.asyncio
async def test_timeout_wrapper_script():
    """Verify the wrapper script generation with timeout"""
    xqute = Xqute(
        scheduler="local",
        workdir="./.xqute/timeout_test_wrapper",
        forks=1,
        scheduler_opts={"timeout": 30},
    )
    job = await xqute.scheduler.create_job(0, ["sleep", "1"])
    script = xqute.scheduler.wrap_job_script(job)

    assert 'timeout 30 bash -c \\"$cmd\\"' in script


@pytest.mark.asyncio
async def test_timeout_disabled_wrapper_script():
    """Verify no timeout command when timeout is 0"""
    xqute = Xqute(
        scheduler="local",
        workdir="./.xqute/timeout_test_nowatcher",
        forks=1,
        scheduler_opts={"timeout": 0},
    )
    job = await xqute.scheduler.create_job(0, ["sleep", "1"])
    script = xqute.scheduler.wrap_job_script(job)

    assert 'timeout 0 bash -c' not in script


@pytest.mark.asyncio
async def test_timeout_kills_long_running_job():
    """A job that exceeds timeout should be killed and marked FAILED"""
    xqute = Xqute(
        scheduler="local",
        workdir="./.xqute/timeout_test_kill",
        forks=1,
        scheduler_opts={"timeout": 2},
    )
    await xqute.feed(["sleep", "30"])
    await xqute.run_until_complete()

    job = xqute.jobs[0]
    assert job._status == JobStatus.FAILED
    rc = await job.get_rc()
    assert rc != 0, f"Expected non-zero rc for timed-out job, got {rc}"


@pytest.mark.asyncio
async def test_timeout_does_not_kill_fast_job():
    """A job that finishes before timeout should succeed normally"""
    xqute = Xqute(
        scheduler="local",
        workdir="./.xqute/timeout_test_fast",
        forks=1,
        scheduler_opts={"timeout": 30},
    )
    await xqute.feed(["sleep", "1"])
    await xqute.run_until_complete()

    job = xqute.jobs[0]
    assert job._status == JobStatus.FINISHED
    rc = await job.get_rc()
    assert rc == 0, f"Expected rc=0 for fast job, got {rc}"
