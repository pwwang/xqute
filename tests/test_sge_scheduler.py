import os
import stat
import pytest  # type: ignore
from pathlib import Path

from xqute.schedulers.sge_scheduler import SgeScheduler
from xqute.defaults import JobStatus

MOCKS = Path(__file__).parent / "mocks"


@pytest.fixture
def qsub():
    cmd = str(MOCKS / "qsub")
    st = os.stat(cmd)
    os.chmod(cmd, st.st_mode | stat.S_IEXEC)
    return cmd


@pytest.fixture
def qdel():
    cmd = str(MOCKS / "qdel")
    st = os.stat(cmd)
    os.chmod(cmd, st.st_mode | stat.S_IEXEC)
    return cmd


@pytest.fixture
def qstat():
    cmd = str(MOCKS / "qstat")
    st = os.stat(cmd)
    os.chmod(cmd, st.st_mode | stat.S_IEXEC)
    return cmd


async def test_job(tmp_path):
    scheduler = SgeScheduler(
        forks=1,
        notify=True,
        l=["vmem=2G", "gpu=1"],
        m="abe",
        workdir=tmp_path,
    )
    job = await scheduler.create_job(0, ["echo", 1])
    assert await scheduler.wrapped_job_script(job) == tmp_path / "0" / "job.wrapped.sge"

    script = scheduler.wrap_job_script(job)
    assert "#$ -notify" in script
    assert "#$ -l vmem=2G" in script
    assert "#$ -l gpu=1" in script
    assert "#$ -m abe" in script


async def test_cwd(tmp_path):
    scheduler = SgeScheduler(
        forks=1,
        notify=True,
        l=["vmem=2G", "gpu=1"],
        m="abe",
        workdir=tmp_path,
        cwd="/tmp/cwd",
    )
    job = await scheduler.create_job(0, ["echo", 1])

    script = scheduler.wrap_job_script(job)
    assert "#$ -notify" in script
    assert "#$ -l vmem=2G" in script
    assert "#$ -l gpu=1" in script
    assert "#$ -m abe" in script
    assert "#$ -cwd" not in script
    assert "#$ -wd /tmp/cwd" in script


async def test_scheduler(tmpdir, qsub, qdel, qstat):

    scheduler = SgeScheduler(qsub=qsub, qdel=qdel, qstat=qstat, workdir=tmpdir)
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


async def test_submission_failure(tmp_path, qdel, qstat):

    scheduler = SgeScheduler(
        qsub="no_such_qsub", qdel=qdel, qstat=qstat, workdir=tmp_path
    )
    job = await scheduler.create_job(0, ["echo", 1])

    assert await scheduler.submit_job_and_update_status(job) is None
    assert await scheduler.job_is_running(job) is False
    assert await job.get_status(True) == JobStatus.FAILED
    assert "Failed to submit job" in await job.stderr_file.a_read_text()
