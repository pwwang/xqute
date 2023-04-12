"""The scheduler to run jobs locally"""
import asyncio
import os
from typing import Type

from ..defaults import JobStatus
from ..job import Job
from ..scheduler import Scheduler
from ..utils import a_read_text


class LocalJob(Job):
    """Local job"""

    def wrap_cmd(self, scheduler: Scheduler) -> str:
        """Wrap the command for the scheduler to submit and run

        Args:
            scheduler: The scheduler

        Returns:
            The wrapped script
        """
        return self.CMD_WRAPPER_TEMPLATE.format(
            shebang=f"#!{self.CMD_WRAPPER_SHELL}",
            prescript=scheduler.config.prescript,
            postscript=scheduler.config.postscript,
            job=self,
            status=JobStatus,
        )


class LocalScheduler(Scheduler):
    """The local scheduler

    Attributes:
        name: The name of the scheduler
        job_class: The job class
    """

    name = "local"
    job_class: Type[Job] = LocalJob

    async def submit_job(self, job: Job) -> int:
        """Submit a job locally

        Args:
            job: The job

        Returns:
            The process id
        """
        proc = await asyncio.create_subprocess_exec(
            job.CMD_WRAPPER_SHELL,
            str(await job.wrapped_script(self)),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
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
            jid = int(await a_read_text(job.jid_file))
        except (ValueError, TypeError, FileNotFoundError):
            return False

        if jid <= 0:
            return False

        try:
            os.kill(jid, 0)
        except Exception:  # pragma: no cover
            return False

        return True
