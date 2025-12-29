"""The scheduler to run jobs locally"""

import asyncio
import os
import shlex

from ..job import Job
from ..scheduler import Scheduler


def _pid_exists(pid: int) -> bool:
    """Check if a process with a given pid exists"""
    try:
        os.kill(pid, 0)
    except Exception:  # pragma: no cover
        return False
    return True


class LocalScheduler(Scheduler):
    """The local scheduler

    Attributes:
        name: The name of the scheduler
        job_class: The job class
    """

    name = "local"

    async def submit_job(self, job: Job) -> int:
        """Submit a job locally

        Args:
            job: The job

        Returns:
            The process id
        """
        job_script = await self.wrapped_job_script(job)
        wrapt_script_path = await job_script.get_fspath()
        # In case the process exits very quickly
        if not await job.jid_file.a_exists():
            await job.jid_file.a_write_text("0")

        proc = await asyncio.create_subprocess_exec(
            *shlex.split(self.jobcmd_shebang(job)),
            wrapt_script_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            start_new_session=True,
            # Changing the working directory here may cause wrapped_job_script to fail
            # to be found, so we don't set cwd here.
            # The cwd is changed in the wrapper script instead.
            # cwd=self.cwd
        )

        # wait for a while to make sure the process is running
        # this is to avoid the real command is not run when proc is recycled too early
        # this happens for python < 3.12
        await asyncio.sleep(0.1)

        if await job.stdout_file.a_exists():  # pragma: no cover
            # job submitted successfully and already started very soon
            return proc.pid

        if proc.returncode is not None and proc.returncode != 0:
            # The process has already finished and no stdout/stderr files are
            # generated
            # Something went wrong with the wrapper script?
            stderr = await proc.stdout.read()
            raise RuntimeError(
                f"Failed to submit job #{job.index} (rc={proc.returncode}): "
                f"{stderr.decode()}\n"
                f"Command: {self.jobcmd_shebang(job)} "
                f"{wrapt_script_path}\n"
            )

        # don't await for the results, as this will run the real command
        return proc.pid

    async def kill_job(self, job: Job):
        """Kill a job asynchronously

        Args:
            job: The job
        """
        try:
            os.killpg(int(await job.get_jid()), 9)
        except Exception:  # pragma: no cover
            pass

    async def job_is_running(self, job: Job) -> bool:
        """Tell if a job is really running, not only the job.jid_file

        In case where the jid file is not cleaned when job is done.

        Args:
            job: The job

        Returns:
            True if it is, otherwise False
        """
        try:
            jid = int((await job.jid_file.a_read_text()).strip())
        except (ValueError, TypeError, FileNotFoundError):
            return False

        if jid <= 0:
            return False

        return _pid_exists(jid)

    @property
    def jobcmd_wrapper_init(self) -> str:
        """The init script for the job command wrapper"""
        wrapper_init = super().jobcmd_wrapper_init
        wrapper_init += "\n"
        # give some time for xqute to update job status to submitted first
        wrapper_init += "sleep 1\n"
        return wrapper_init

    def jobcmd_prep(self, job) -> str:
        """The job command preparation"""
        codes = super().jobcmd_prep(job)
        codes += "\n"
        # give some time for xqute to update job status to running
        codes += "sleep 1\n"
        return codes
