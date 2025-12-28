import os
import sys
import asyncio
import signal
import pytest
from panpath import PanPath
from simplug import NoSuchPlugin
from xqute import Xqute, plugin
from xqute.defaults import JobStatus
from xqute.schedulers.local_scheduler import LocalScheduler
from .conftest import BUCKET

Xqute.EMPTY_BUFFER_SLEEP_TIME = 0.1


class EchoPlugin:
    @plugin.impl
    def on_init(xqute):
        print("init from echoplugin")

    @plugin.impl
    async def on_job_init(scheduler, job):
        print(await job.get_jid())
        print(repr(job))
        await job.jid_file.a_write_text("-1")

    @plugin.impl
    async def on_job_started(scheduler, job):
        print("Job %s started" % job.index)

    @plugin.impl
    async def on_job_polling(scheduler, job, counter):
        print("Job %s polling" % job.index)

    @plugin.impl
    def on_shutdown(xqute, sig):
        print("DONE", sig)

    # @plugin.impl
    # async def on_jobsched_started(args):
    #     Path(args.metadir).joinpath("jobsched.log").write_text("jobsched started")

    # @plugin.impl
    # async def on_jobsched_ended(args, rc):
    #     Path(args.metadir).joinpath("jobsched.log").write_text("jobsched ended")


class CancelShutdownPlugin:
    @plugin.impl
    def on_shutdown(xqute, sig):
        print("Cancelling shutdown")
        xqute._tasks.cancel()
        return False


class JobFailPlugin:
    @plugin.impl
    async def on_job_failed(scheduler, job):
        print("Job Failed: %s" % job)

    @plugin.impl
    async def on_job_killing(scheduler, job):
        if job.index == 0:
            await scheduler.kill_job(job)
            await job.set_status(JobStatus.FINISHED)
            return False

    @plugin.impl
    async def on_job_succeeded(scheduler, job):
        print("Job Succeeded: %s" % job)


class JobIsRunningPlugin:
    @plugin.impl
    async def on_job_init(scheduler, job):
        await job.jid_file.a_write_text(str(os.getpid()))


class JobCancelPlugin:
    @plugin.impl
    async def on_job_submitting(scheduler, job):
        await job.clean()
        await job.set_jid(await scheduler.submit_job(job))
        await job.set_status(JobStatus.SUBMITTED)
        return False


async def test_main(tmp_path, capsys):
    with plugin.plugins_context([EchoPlugin]):
        xqute = Xqute(LocalScheduler, forks=2, workdir=tmp_path)
        await xqute.feed(["bash", "-c", "echo 1"])
        await xqute.feed(["echo", 2])
        await xqute.run_until_complete()
        assert await xqute.jobs[0].get_rc() == 0

        await xqute.stop_feeding()
        out = capsys.readouterr().out
        assert "keep_feeding mode was not started" in out


async def test_keep_feeding(tmp_path):
    xqute = Xqute(LocalScheduler, forks=2, workdir=tmp_path)
    await xqute.run_until_complete(keep_feeding=True)
    for i in range(2):
        await xqute.feed(["echo", i])
    await xqute.stop_feeding()


async def test_del_without_stop_feeding(tmp_path, caplog):
    xqute = Xqute(LocalScheduler, forks=2, workdir=tmp_path)
    await xqute.run_until_complete(keep_feeding=True)
    for i in range(2):
        await xqute.feed(["echo", i])

    xqute.__del__()
    assert "Did you forget to call 'await xqute.stop_feeding()'?" in caplog.text


async def test_xqute_cloud_workdir(request):
    # generate a unique request id based on sys.executable and the python version
    requestid = (
        hash((request.node.name, sys.executable, sys.version_info)) & 0xFFFFFFFF
    )
    workdir = f"{BUCKET}/xqute_local_test.{requestid}"
    xqute = Xqute(LocalScheduler, workdir=workdir)
    await xqute.feed(["echo", 1])
    job = await xqute.scheduler.create_job(1, ["echo", 1])
    await xqute.feed(job)
    await xqute.run_until_complete()
    assert await xqute.jobs[0].get_rc() == 0
    assert await xqute.jobs[1].get_rc() == 0
    await PanPath(workdir).a_rmtree()


async def test_plugin(tmp_path, capsys):
    with plugin.plugins_context([EchoPlugin, JobFailPlugin]):
        xqute = Xqute("local", forks=1, workdir=tmp_path)
        await xqute.feed("echo 2")
        await xqute.feed(["sleep", 5])
        await xqute.run_until_complete()

        out = capsys.readouterr().out
        assert "init from echoplugin" in out
        assert "Job 1 started" in out
        assert out.count("Job 1 polling") > 1
        assert "DONE" in out


def test_not_init_in_loop():
    with pytest.raises(RuntimeError):
        Xqute()


async def test_shutdown(tmp_path, caplog):
    with plugin.plugins_context([EchoPlugin, JobFailPlugin]):
        xqute = Xqute(forks=2, workdir=tmp_path)
        await xqute.feed(["sleep", 1])
        await xqute.feed(["echo", 2])
        asyncio.get_event_loop().call_later(0.5, xqute.cancel, signal.SIGTERM)
        await xqute.run_until_complete()
        assert "Got signal 'SIGTERM'" in caplog.text


async def test_cancel_shutdown(tmp_path, caplog, capsys):
    with plugin.plugins_context([EchoPlugin, CancelShutdownPlugin, JobFailPlugin]):
        xqute = Xqute(workdir=tmp_path)
        await xqute.feed(["sleep", 1])
        await xqute.feed(["echo", 2])
        asyncio.get_running_loop().call_later(0.5, xqute.cancel, signal.SIGTERM)
        await xqute.run_until_complete()
        assert capsys.readouterr().out.count("Cancelling shutdown") == 1
        assert caplog.text.count("Got signal 'SIGTERM'") == 1


async def test_job_failed_hook(tmp_path, caplog, capsys):
    with plugin.plugins_context([JobFailPlugin]):
        xqute = Xqute(
            error_strategy="retry",
            num_retries=1,
            workdir=tmp_path,
        )
        await xqute.feed(["echo1", 1])
        await xqute.feed(["echo", 1])
        await xqute.run_until_complete()
        assert "Job Failed: <Job-0" in capsys.readouterr().out
        assert "/Job-0 Status changed: 'SUBMITTED' -> 'RUNNING'" in caplog.text
        assert "/Job-0 Status changed: 'RUNNING' -> 'FAILED'" in caplog.text
        assert "/Job-0 Status changed: 'FAILED' -> 'RETRYING'" in caplog.text
        assert "/Job-0 Status changed: 'RETRYING' -> 'SUBMITTED'" in caplog.text
        assert "/Job-1 Status changed: 'SUBMITTED' -> 'RUNNING'" in caplog.text
        assert "/Job-1 Status changed: 'RUNNING' -> 'FINISHED'" in caplog.text

        # should clean retry directories
        xqute = Xqute(
            error_strategy="retry", num_retries=1, workdir=tmp_path
        )
        await xqute.feed(["echo1", 1])
        await xqute.feed(["echo", 1])
        await xqute.run_until_complete()


async def test_job_is_running(tmp_path, caplog):
    with plugin.plugins_context([JobIsRunningPlugin]):
        xqute = Xqute(workdir=tmp_path)
        await xqute.feed(["echo", 1])
        loop = asyncio.get_event_loop()
        loop.call_later(2.0, xqute.cancel)
        await xqute.run_until_complete()
        assert "Skip submitting" in caplog.text


async def test_job_set_rc_jid(tmp_path):
    xqute = Xqute(workdir=tmp_path)
    await xqute.feed(["bash", "-c", "exit 3"])
    await xqute.run_until_complete()
    job = xqute.jobs[0]
    assert await job.get_rc() == 3

    await job.set_rc(5)
    assert await job.get_rc() == 5

    await job.set_jid(12345)
    job._jid = None  # force reload
    assert await job.get_jid() == "12345"


async def test_halt(tmp_path, caplog):
    await asyncio.sleep(1)
    with plugin.plugins_context([JobFailPlugin]):
        xqute = Xqute(
            error_strategy="halt", workdir=tmp_path, forks=3
        )
        await xqute.feed(["sleep", 10])
        await xqute.feed(["echo1", 1])
        await xqute.feed(["sleep", 3])
        await xqute.run_until_complete()
        assert "Pipeline will halt" in caplog.text


async def test_cancel_submitting(tmp_path, caplog):
    xqute = Xqute(workdir=tmp_path, plugins=[JobCancelPlugin])
    await xqute.feed("echo 1")
    await xqute.run_until_complete()
    assert "Job 0 submitted" not in caplog.text


async def test_plugin_context():
    with pytest.raises(NoSuchPlugin):
        Xqute(plugins=["+a", "-b"])

    xqute = Xqute(plugins=["-a", "-b"])
    await xqute.run_until_complete()


async def test_put_job_with_envs(tmp_path):
    xqute = Xqute(workdir=tmp_path)
    await xqute.feed("echo $MYVAR", envs={"MYVAR": "123"})

    job2 = await xqute.scheduler.create_job(1, "echo $MYVAR", envs={"MYVAR": "111"})
    await xqute.feed(job2, envs={"MYVAR": "456"})
    await xqute.run_until_complete()
    job = xqute.jobs[0]
    assert await job.get_rc() == 0
    assert (await job.stdout_file.a_read_text()).strip() == "123"

    assert await job2.get_rc() == 0
    assert (await job2.stdout_file.a_read_text()).strip() == "456"
