"""The scheduler to schedule jobs"""

from __future__ import annotations

import os
import signal
from abc import ABC, abstractmethod
from typing import List, Type

from cloudpathlib import AnyPath
from diot import Diot  # type: ignore

from .defaults import (
    JobStatus,
    JobErrorStrategy,
    DEFAULT_ERROR_STRATEGY,
    DEFAULT_NUM_RETRIES,
)
from .utils import logger, CommandType
from .job import Job
from .plugin import plugin


class Scheduler(ABC):
    """The abstract class for scheduler

    Attributes:
        job_class: The job class

    Args:
        workdir: The working directory
        forks: Max number of job forks
        error_strategy: The strategy when there is error happened
        num_retries: Max number of retries when error_strategy is retry
        prescript: The prescript to run before the job command
            It is a piece of script that inserted into the wrapper script, running
            on the scheduler system.
        postscript: The postscript to run when job finished
            It is a piece of script that inserted into the wrapper script, running
            on the scheduler system.
        jobname_prefix: The prefix for the job name
        **kwargs: Other arguments for the scheduler
    """

    __slots__ = (
        "config",
        "forks",
        "workdir",
        "error_strategy",
        "num_retries",
        "prescript",
        "postscript",
        "jobname_prefix",
    )

    name: str
    job_class: Type[Job]

    def __init__(
        self,
        workdir: str,
        forks: int = 1,
        error_strategy: str = DEFAULT_ERROR_STRATEGY,
        num_retries: int = DEFAULT_NUM_RETRIES,
        prescript: str = "",
        postscript: str = "",
        jobname_prefix: str | None = None,
        **kwargs,
    ):
        self.forks = forks
        self.workdir = AnyPath(workdir)
        self.error_strategy = error_strategy
        self.num_retries = num_retries
        self.prescript = prescript
        self.postscript = postscript
        self.jobname_prefix = jobname_prefix or self.name

        self.config = Diot(**kwargs)

    def create_job(self, index: int, cmd: CommandType) -> Job:
        """Create a job

        Args:
            index: The index of the job
            cmd: The command of the job

        Returns:
            The job
        """
        return self.job_class(
            index=index,
            cmd=cmd,
            workdir=self.workdir,
            error_retry=self.error_strategy == JobErrorStrategy.RETRY,
            num_retries=self.num_retries,
        )

    async def submit_job_and_update_status(self, job: Job):
        """Submit and update the status

        1. Check if the job is already submitted or running
        2. If not, run the hook
        3. If the hook is not cancelled, clean the job
        4. Submit the job, raising an exception if it fails
        5. If the job is submitted successfully, update the status
        6. If the job fails to submit, update the status and write stderr to
            the job file

        Args:
            job: The job
        """
        if await self.job_is_submitted_or_running(job):
            logger.warning(
                "/Scheduler-%s Skip submitting, "
                "job %r is already submitted or running.",
                self.name,
                job,
            )
            return

        exception: Exception | None = None
        try:
            if await plugin.hooks.on_job_submitting(self, job) is False:
                return
            job.clean()

            try:
                # raise the exception immediately
                # it somehow cannot be catched immediately
                job.jid = await self.submit_job(job)
            except Exception as exc:
                exception = RuntimeError(f"Failed to submit job: {exc}")
                exception.__traceback__ = exc.__traceback__
            else:
                logger.info(
                    "/Scheduler-%s Job %s submitted (jid: %s, wrapped: %s)",
                    self.name,
                    job.index,
                    job.jid,
                    job.wrapped_script(self),
                )

                job.status = JobStatus.SUBMITTED
                await plugin.hooks.on_job_submitted(self, job)
        except Exception as exc:  # pragma: no cover
            exception = exc

        if exception is not None:
            from traceback import format_exception

            job.stderr_file.write_text(
                "".join(
                    format_exception(
                        type(exception),
                        exception,
                        exception.__traceback__,
                    )
                ),
            )
            job.rc_file.write_text("-2")
            job.status = JobStatus.FAILED
            await plugin.hooks.on_job_failed(self, job)

    async def retry_job(self, job: Job):
        """Retry a job

        Args:
            job: The job
        """
        job.jid = ""
        job.clean(retry=True)
        job.trial_count += 1
        logger.warning(
            "/Scheduler-%s Retrying (#%s) job: %r",
            self.name,
            job.trial_count,
            job,
        )
        await self.submit_job_and_update_status(job)

    async def kill_job_and_update_status(self, job: Job):
        """Kill a job and update its status

        Args:
            job: The job
        """
        job.status = JobStatus.KILLING
        ret = await plugin.hooks.on_job_killing(self, job)
        if ret is False:  # pragma: no cover
            return

        await self.kill_job(job)
        try:
            # in case the jid file is removed by the wrapped script
            job.jid_file.unlink(missing_ok=True)
        except FileNotFoundError:  # pragma: no cover
            pass

        job.status = JobStatus.FINISHED
        await plugin.hooks.on_job_killed(self, job)

    async def polling_jobs(self, jobs: List[Job], on: str) -> bool:
        """Check if all jobs are done or new jobs can submit

        Args:
            jobs: The list of jobs
            on: query on status: `submittable` or `all_done`

        Returns:
            True if yes otherwise False.
        """
        n_running = 0
        ret = True
        for job in jobs:
            status = job.status
            if on == "submittable" and status in (
                JobStatus.QUEUED,
                JobStatus.SUBMITTED,
                JobStatus.RUNNING,
                JobStatus.KILLING,
            ):
                n_running += 1

            if job.prev_status != status:
                if status in (JobStatus.FAILED, JobStatus.RETRYING):
                    if job.prev_status != JobStatus.RUNNING:
                        await plugin.hooks.on_job_started(self, job)
                    await plugin.hooks.on_job_failed(self, job)
                elif status == JobStatus.FINISHED:
                    if job.prev_status != JobStatus.RUNNING:
                        await plugin.hooks.on_job_started(self, job)
                    await plugin.hooks.on_job_succeeded(self, job)
                elif status == JobStatus.RUNNING:
                    await plugin.hooks.on_job_started(self, job)
            elif status == JobStatus.RUNNING:
                await plugin.hooks.on_job_polling(self, job)

            if (
                self.error_strategy == JobErrorStrategy.HALT
                and status == JobStatus.FAILED
            ):
                logger.error(
                    "/Scheduler-%s Pipeline will halt " "since job failed: %r",
                    self.name,
                    job,
                )
                os.kill(os.getpid(), signal.SIGTERM)
                # job.status = JobStatus.FINISHED
                break

            if status not in (JobStatus.FINISHED, JobStatus.FAILED):
                # Try to resubmit the job for retrying
                if status == JobStatus.RETRYING:
                    await self.retry_job(job)
                ret = False
                # not returning here
                # might wait for callbacks or halt on other jobs
        return n_running < self.forks if on == "submittable" else ret

    async def kill_running_jobs(self, jobs: List[Job]):
        """Try to kill all running jobs

        Args:
            jobs: The list of jobs
        """
        logger.warning("/Scheduler-%s Killing running jobs ...", self.name)
        for job in jobs:
            status = job.status
            if status in (JobStatus.SUBMITTED, JobStatus.RUNNING):
                await self.kill_job_and_update_status(job)

    async def job_is_submitted_or_running(self, job: Job) -> bool:
        """Check if a job is already submitted or running

        Args:
            job: The job

        Returns:
            True if yes otherwise False.
        """
        if job.jid_file.is_file():
            if await self.job_is_running(job):
                job.status = JobStatus.SUBMITTED
                return True
        return False

    @abstractmethod
    async def submit_job(self, job: Job) -> int | str:
        """Submit a job

        Args:
            job: The job

        Returns:
            The unique id in the scheduler system
        """

    @abstractmethod
    async def kill_job(self, job: Job):
        """Kill a job

        Args:
            job: The job
        """

    @abstractmethod
    async def job_is_running(self, job: Job) -> bool:
        """Check if a job is really running

        Args:
            job: The job

        Returns:
            True if yes otherwise False.
        """
