import os
import asyncio
import signal
import pytest
from simplug import NoSuchPlugin
from xqute import Xqute, plugin
from xqute.defaults import JobStatus
from xqute.schedulers.local_scheduler import LocalJob, LocalScheduler
from xqute.utils import a_write_text

Xqute.EMPTY_BUFFER_SLEEP_TIME = 0.1


class EchoPlugin:
    @plugin.impl
    def on_init(xqute):
        print("init from echoplugin")

    @plugin.impl
    async def on_job_init(scheduler, job):
        print(job.jid)
        print(repr(job))
        await a_write_text(job.jid_file, "-1")

    @plugin.impl
    async def on_job_started(scheduler, job):
        print("Job %s started" % job.index)

    @plugin.impl
    async def on_job_polling(scheduler, job):
        print("Job %s polling" % job.index)

    @plugin.impl
    def on_shutdown(xqute, sig):
        print("DONE", sig)

    @plugin.impl
    def on_jobcmd_init(scheduler, job):
        return "echo jobcmd init > {job.metadir}/jobcmd.log"

    @plugin.impl
    def on_jobcmd_prep(scheduler, job):
        return 'echo jobcmd prep >> {job.metadir}/jobcmd.log\ncmd="$cmd"'

    @plugin.impl
    def on_jobcmd_end(scheduler, job):
        return "echo jobcmd end >> {job.metadir}/jobcmd.log"


class CancelShutdownPlugin:
    @plugin.impl
    def on_shutdown(xqute, sig):
        print("Cancelling shutdown")
        xqute.task.cancel()
        return False


class JobFailPlugin:
    @plugin.impl
    async def on_job_failed(scheduler, job):
        print("Job Failed: %s" % job)

    @plugin.impl
    async def on_job_killing(scheduler, job):
        if job.index == 0:
            await scheduler.kill_job(job)
            job.status = JobStatus.FINISHED
            return False

    @plugin.impl
    async def on_job_succeeded(scheduler, job):
        print("Job Succeeded: %s" % job)


class JobIsRunningPlugin:
    @plugin.impl
    async def on_job_init(scheduler, job):
        await a_write_text(job.jid_file, str(os.getpid()))


class JobCancelPlugin:
    @plugin.impl
    async def on_job_submitting(scheduler, job):
        await job.clean()
        job.jid = await scheduler.submit_job(job)
        job.status = JobStatus.SUBMITTED
        return False


@pytest.mark.asyncio
async def test_main(tmp_path):
    with plugin.plugins_context([EchoPlugin]):
        xqute = Xqute(LocalScheduler, scheduler_forks=2, job_metadir=tmp_path)
        await xqute.put(["bash", "-c", "echo 1"])
        await xqute.put(["echo", 2])
        await xqute.run_until_complete()
        assert await xqute.jobs[0].rc == 0

        jobcmd_logfile0 = tmp_path / "0" / "jobcmd.log"
        assert jobcmd_logfile0.is_file()
        assert jobcmd_logfile0.read_text() == "jobcmd init\njobcmd prep\njobcmd end\n"

        jobcmd_logfile1 = tmp_path / "0" / "jobcmd.log"
        assert jobcmd_logfile1.is_file()
        assert jobcmd_logfile1.read_text() == "jobcmd init\njobcmd prep\njobcmd end\n"


@pytest.mark.asyncio
async def test_plugin(tmp_path, capsys):
    with plugin.plugins_context([EchoPlugin, JobFailPlugin]):
        xqute = Xqute("local", scheduler_forks=1, job_metadir=tmp_path)
        await xqute.put("echo 2")
        await xqute.put(["sleep", 3])
        await xqute.run_until_complete()

        out = capsys.readouterr().out
        assert "init from echoplugin" in out
        assert "Job 1 started" in out
        assert out.count("Job 1 polling") > 1
        assert "DONE" in out
        print(out)


def test_not_init_in_loop():
    with pytest.raises(RuntimeError):
        Xqute()


@pytest.mark.asyncio
async def test_shutdown(tmp_path, caplog):
    with plugin.plugins_context(
        [EchoPlugin, JobFailPlugin]
    ):
        xqute = Xqute(scheduler_forks=2, job_metadir=tmp_path)
        await xqute.put(["sleep", 1])
        await xqute.put(["echo", 2])
        asyncio.get_event_loop().call_later(0.5, xqute.cancel, signal.SIGTERM)
        await xqute.run_until_complete()
        assert "Got signal 'SIGTERM'" in caplog.text


@pytest.mark.asyncio
async def test_cancel_shutdown(tmp_path, caplog, capsys):
    with plugin.plugins_context(
        [EchoPlugin, CancelShutdownPlugin, JobFailPlugin]
    ):
        xqute = Xqute(job_metadir=tmp_path)
        await xqute.put(["sleep", 1])
        await xqute.put(["echo", 2])
        asyncio.get_event_loop().call_later(0.5, xqute.cancel, signal.SIGTERM)
        await xqute.run_until_complete()
        assert capsys.readouterr().out.count("Cancelling shutdown") == 1
        assert caplog.text.count("Got signal 'SIGTERM'") == 1


@pytest.mark.asyncio
# async def test_job_failed_hook(tmp_path, caplog, capsys):
async def test_job_failed_hook(tmp_path):
    with plugin.plugins_context([JobFailPlugin]):
        xqute = Xqute(
            job_error_strategy="retry",
            job_num_retries=1,
            job_metadir=tmp_path,
        )
        await xqute.put(["echo1", 1])
        await xqute.put(["echo", 1])
        await xqute.run_until_complete()
        # assert "Job Failed: <LocalJob-0" in capsys.readouterr().out
        # assert "/Job-0 Status changed: 'SUBMITTED' -> 'FAILED'" in caplog.text
        # assert (
        #     "/Job-1 Status changed: 'SUBMITTED' -> 'FINISHED'" in caplog.text
        # )

        # should clean retry directories
        xqute = Xqute(
            job_error_strategy="retry", job_num_retries=1, job_metadir=tmp_path
        )
        await xqute.put(["echo1", 1])
        await xqute.put(["echo", 1])
        await xqute.run_until_complete()


@pytest.mark.asyncio
async def test_job_is_running(tmp_path, caplog):
    with plugin.plugins_context([JobIsRunningPlugin]):
        xqute = Xqute(job_metadir=tmp_path)
        await xqute.put(["echo", 1])
        loop = asyncio.get_event_loop()
        loop.call_later(2.0, xqute.cancel)
        await xqute.run_until_complete()
        assert "Skip submitting" in caplog.text


@pytest.mark.asyncio
async def test_halt(tmp_path, caplog):
    await asyncio.sleep(1)
    with plugin.plugins_context([JobFailPlugin]):
        xqute = Xqute(
            job_error_strategy="halt", job_metadir=tmp_path, scheduler_forks=3
        )
        await xqute.put(LocalJob(0, ["sleep", 10], tmp_path, False, 1))
        await xqute.put(["echo1", 1])
        await xqute.put(["sleep", 3])
        await xqute.run_until_complete()
        assert "Pipeline will halt" in caplog.text


@pytest.mark.asyncio
async def test_cancel_submitting(tmp_path, caplog):
    xqute = Xqute(job_metadir=tmp_path, plugins=[JobCancelPlugin])
    await xqute.put(LocalJob(0, "echo 1", tmp_path))
    await xqute.run_until_complete()
    assert "Job 0 submitted" not in caplog.text


@pytest.mark.asyncio
async def test_plugin_context():
    with pytest.raises(NoSuchPlugin):
        Xqute(plugins=["+a", "-b"])

    xqute = Xqute(plugins=["-a", "-b"])
    await xqute.run_until_complete()
