from xqute import scheduler
from xqute.plugin import on_job_submitted, on_job_succeeded
import pytest
import os, signal
from xqute import *
from xqute.utils import a_write_text

class EchoPlugin:
    @simplug.impl
    async def on_init(scheduler):
        print('init from echoplugin')

    @simplug.impl
    async def on_job_init(scheduler, job):
        await a_write_text(job.lock_file, '-1')

    @simplug.impl
    async def on_complete(scheduler):
        print('DONE')

class ShutdownPlugin:
    @simplug.impl
    async def on_job_submitted(scheduler, job):
        os.kill(os.getpid(), signal.SIGTERM)

class CancelShutdownPlugin:
    @simplug.impl
    async def on_shutdown(scheduler, consumer):
        print('Cancelling shutdown')
        return False

class JobFailPlugin:
    @simplug.impl
    async def on_job_failed(scheduler, job):
        print('Job Failed: %s' % job)

    @simplug.impl
    async def on_job_succeeded(scheduler, job):
        print('Job Succeeded: %s' % job)

class JobIsRunningPlugin:
    @simplug.impl
    async def on_job_init(scheduler, job):
        await a_write_text(job.lock_file, str(os.getpid()))

simplug.register(EchoPlugin,
                 ShutdownPlugin,
                 CancelShutdownPlugin,
                 JobFailPlugin,
                 JobIsRunningPlugin)

@pytest.fixture
def without_shutdown():
    simplug.disable('shutdownplugin')
    simplug.disable('cancelshutdownplugin')
    yield
    simplug.enable('shutdownplugin')
    simplug.enable('cancelshutdownplugin')

@pytest.fixture
def without_cancel_shutdown_only():
    simplug.disable('cancelshutdownplugin')
    yield
    simplug.enable('cancelshutdownplugin')

@pytest.fixture
def without_jobisrunning():
    simplug.disable('jobisrunningplugin')
    yield
    simplug.enable('jobisrunningplugin')

@pytest.mark.asyncio
async def test_main(without_shutdown, without_jobisrunning):
    xqute = Xqute('local', scheduler_forks=2)
    await xqute.push(['bash', '-c', 'echo 1'])
    await xqute.push(['echo', 2])
    await xqute.run_until_complete()
    assert await xqute.scheduler.jobs[0].rc == 0
    assert xqute.scheduler.jobs[1].name(xqute.scheduler) == 'local.job.1'

@pytest.mark.asyncio
async def test_plugin(capsys, without_shutdown, without_jobisrunning):

    xqute = Xqute('local', scheduler_forks=1)
    await xqute.push(['echo', 2])
    await xqute.push(['sleep', 1])
    await xqute.run_until_complete()

    out = capsys.readouterr().out
    assert 'init from echoplugin' in out
    assert 'DONE' in out

def test_not_init_in_loop():
    with pytest.raises(RuntimeError):
        Xqute()

@pytest.mark.asyncio
async def test_shutdown(caplog, without_cancel_shutdown_only,
                        without_jobisrunning):

    xqute = Xqute(scheduler_forks=2)
    await xqute.push(['sleep', 1])

    await xqute.push(['echo', 2])
    await xqute.run_until_complete()
    assert "Got signal 'SIGTERM'" in caplog.text

@pytest.mark.asyncio
async def test_cancel_shutdown(caplog, capsys, without_jobisrunning):

    xqute = Xqute()
    await xqute.push(['echo', 1])
    await xqute.push(['echo', 2])
    await xqute.run_until_complete()
    assert capsys.readouterr().out.count("Cancelling shutdown") == 2
    assert caplog.text.count("Got signal 'SIGTERM'") == 2

@pytest.mark.asyncio
async def test_job_failed_hook(caplog, capsys, without_shutdown,
                               without_jobisrunning):

    xqute = Xqute(job_error_strategy='retry', job_num_retries=1)
    await xqute.push(['echo1', 1])
    await xqute.push(['echo', 1])
    await xqute.run_until_complete()
    assert 'Job Failed: <LocalJob-0' in capsys.readouterr().out
    assert "/Job-0 Status changed: 'SUBMITTED' -> 'FAILED'" in caplog.text
    assert "/Job-1 Status changed: 'SUBMITTED' -> 'FINISHED'" in caplog.text

@pytest.mark.asyncio
async def test_job_is_running(caplog, without_shutdown):
    xqute = Xqute()
    await xqute.push(['echo', 1])
    await xqute.run_until_complete()
    assert 'Skip submitting' in caplog.text

@pytest.mark.asyncio
async def test_halt(caplog, without_shutdown, without_jobisrunning):
    xqute = Xqute(job_error_strategy='halt')
    await xqute.push(['echo1', 1])
    await xqute.run_until_complete()
    assert 'Pipeline will halt' in caplog.text
