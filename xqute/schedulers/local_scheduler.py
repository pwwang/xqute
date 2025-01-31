"""The scheduler to run jobs locally"""
import asyncio
import os
from typing import Type
from cloudpathlib import CloudPath

from ..job import Job
from ..scheduler import Scheduler
from ..utils import localize, chmodx


def _pid_exists(pid: int) -> bool:
    """Check if a process with a given pid exists"""
    try:
        os.kill(pid, 0)
    except Exception:
        return False
    return True


class LocalJob(Job):
    """Local job"""


class LocalScheduler(Scheduler):
    """The local scheduler

    Attributes:
        name: The name of the scheduler
        job_class: The job class
    """

    name = "local"
    job_class: Type[Job] = LocalJob

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if isinstance(self.workdir, CloudPath):
            raise ValueError("'local' scheduler does not support cloud path as workdir")

    async def submit_job(self, job: Job) -> int:
        """Submit a job locally

        Args:
            job: The job

        Returns:
            The process id
        """
        wrapt_script = chmodx(localize(job.wrapped_script(self)))
        proc = await asyncio.create_subprocess_exec(
            wrapt_script,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        # wait for a while to make sure the process is running
        # this is to avoid the real command is not run when proc is recycled too early
        # this happens for python < 3.12
        while not job.stderr_file.exists() or not job.stdout_file.exists():
            if proc.returncode is not None:
                # The process has already finished and no stdout/stderr files are
                # generated
                # Something went wrong with the wrapper script?
                stdout = await proc.stdout.read()
                stderr = await proc.stderr.read()
                job.stdout_file.write_bytes(stdout)
                job.stderr_file.write_bytes(stderr)

                raise RuntimeError(stderr.decode())
            await asyncio.sleep(0.1)
        # don't await for the results, as this will run the real command
        return proc.pid

    async def kill_job(self, job: Job):
        """Kill a job asynchronously

        Args:
            job: The job
        """
        try:
            os.killpg(int(job.jid), 9)
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
            jid = int(job.jid_file.read_text().strip())
        except (ValueError, TypeError, FileNotFoundError):
            return False

        if jid <= 0:
            return False

        return _pid_exists(jid)
