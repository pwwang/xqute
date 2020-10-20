import pytest
import os, stat
from pathlib import Path

from xqute.schedulers.sge_scheduler import *
from xqute.defaults import *

MOCKS = Path(__file__).parent / 'mocks'

@pytest.mark.asyncio
async def test_job():
    job = SgeJob(0, ['echo', 1])
    jobs = []
    scheduler = SgeScheduler(jobs, 1,
                             sge_notify=True,
                             sge_l=['vmem=2G', 'gpu=1'],
                             sge_m='abe')
    await job.wrap_cmd(scheduler)
    assert job._wrapped_cmd == DEFAULT_JOB_METADIR / '0' / 'job.wrapped.sge'

    wrapt = await a_read_text(job._wrapped_cmd)
    assert '#$ -notify' in wrapt
    assert '#$ -l vmem=2G' in wrapt
    assert '#$ -l gpu=1' in wrapt
    assert '#$ -m abe' in wrapt

@pytest.mark.asyncio
async def test_scheduler(capsys):
    job = SgeJob(0, ['echo', 1])
    jobs = []
    qsub=str(MOCKS / 'qsub')
    qdel=str(MOCKS / 'qdel')
    qstat=str(MOCKS / 'qstat')
    for qcmd in (qsub, qdel, qstat):
        st = os.stat(str(qcmd))
        os.chmod(str(qcmd), st.st_mode | stat.S_IEXEC)

    scheduler = SgeScheduler(jobs, 1, qsub=qsub, qdel=qdel, qstat=qstat)
    assert await scheduler.submit_job(job) == '613815'
    job.uid = '613815'
    await scheduler.kill_job(job)
    if job.lock_file.is_file():
        os.unlink(job.lock_file)
    assert await scheduler.job_is_running(job) is False

    await a_write_text(job.lock_file, '0')
    assert await scheduler.job_is_running(job) is True
    await a_write_text(job.lock_file, '1')
    assert await scheduler.job_is_running(job) is False
