"""The scheduler to schedule jobs"""
import os
import signal
from abc import ABC, abstractmethod
from typing import ClassVar, List, Type, Union
from diot import Diot
from .defaults import JobStatus
from .utils import logger, asyncify, a_write_text
from .job import Job
from .plugin import plugin


class Scheduler(ABC):
    """The abstract class for scheduler

    Attributes:
        job_class: The job class

    Args:
        forks: Max number of job forks
        prescript: The script to run before the command
        postscript: The script to run after the command
        **kwargs: Other arguments for the scheduler
    """
    __slots__ = ('config', )

    name: ClassVar[str]
    job_class: ClassVar[Type[Job]]

    def __init__(self,
                 forks: int,
                 prescript: str = '',
                 postscript: str = '',
                 **kwargs):
        """Construct"""
        self.config = Diot(forks=forks,
                           prescript=prescript,
                           postscript=postscript,
                           **kwargs)

    async def submit_job_and_update_status(self, job: Job):
        """Submit and update the status

        Args:
            job: The job
        """
        if await self.job_is_submitted_or_running(job):
            logger.warning('/Scheduler-%s Skip submitting, '
                           'job %r is already submitted or running.',
                           self.name, job)
            return
        try:
            if await plugin.hooks.on_job_submitting(self, job) is False:
                return
            await job.clean()

            try:
                # raise the exception immediately
                # it somehow cannot be catched immediately
                job.uid = await self.submit_job(job)
            except Exception as exc:
                raise RuntimeError(f"Failed to submit job: {exc}")

            logger.info('/Scheduler-%s Job %s submitted (uid: %s, wrapped: %s)',
                        self.name,
                        job.index,
                        job.uid,
                        await job.wrapped_script(self))

            job.status = JobStatus.SUBMITTED
            await plugin.hooks.on_job_submitted(self, job)
        except Exception as exc:  # pragma: no cover
            await a_write_text(job.stderr_file, str(exc))
            await a_write_text(job.rc_file, '-2')
            job.status = JobStatus.FAILED
            await plugin.hooks.on_job_failed(self, job)
            raise

    async def retry_job(self, job: Job):
        """Retry a job

        Args:
            job: The job
        """
        job.uid = ''
        await job.clean(retry=True)
        job.trial_count += 1
        logger.warning('/Scheduler-%s Retrying (#%s) job: %r',
                       self.name, job.trial_count, job)
        await self.submit_job_and_update_status(job)

    async def kill_job_and_update_status(self, job: Job):
        """Kill a job and update its status

        Args:
            job: The job
        """
        job.status = JobStatus.KILLING
        ret = await plugin.hooks.on_job_killing(self, job)
        if ret is False:
            return

        await self.kill_job(job)
        try:
            # in case the lock file is removed by the wrapped script
            await asyncify(os.unlink)(job.lock_file)
        except FileNotFoundError:  # pragma: no cover
            pass
        job.status = JobStatus.FINISHED
        await plugin.hooks.on_job_killed(self, job)

    async def polling_jobs(self,
                           jobs: List[Job],
                           on: str,
                           halt_on_error: bool) -> bool:
        """Check if all jobs are done or new jobs can submit

        Args:
            jobs: The list of jobs
            on: query on status: `can_submit` or `all_done`
            halt_on_error: Whether we should halt the whole pipeline on error

        Returns:
            True if yes otherwise False.
        """
        n_running = 0
        ret = True
        for job in jobs:
            status = job.status
            if on == 'can_submit' and status in (
                    JobStatus.QUEUED, JobStatus.SUBMITTED,
                    JobStatus.RUNNING, JobStatus.KILLING
            ):
                n_running += 1

            if job.prev_status != status:
                if status in (JobStatus.FAILED, JobStatus.RETRYING):
                    if job.prev_status != JobStatus.RUNNING:
                        await plugin.hooks.on_job_running(self, job)
                    await plugin.hooks.on_job_failed(self, job)
                elif status == JobStatus.FINISHED:
                    if job.prev_status != JobStatus.RUNNING:
                        await plugin.hooks.on_job_running(self, job)
                    await plugin.hooks.on_job_succeeded(self, job)
                elif status == JobStatus.RUNNING:
                    await plugin.hooks.on_job_running(self, job)

            if halt_on_error and status == JobStatus.FAILED:
                logger.error('/Scheduler-%s Pipeline will halt '
                             'since job failed: %r', self.name, job)
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
        return n_running < self.config.forks if on == 'can_submit' else ret

    async def kill_running_jobs(self, jobs: List[Job]):
        """Try to kill all running jobs

        Args:
            jobs: The list of jobs
        """
        logger.warning('/Scheduler-%s Killing running jobs ...', self.name)
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
        if job.lock_file.is_file():
            if await self.job_is_running(job):
                job.status = JobStatus.SUBMITTED
                return True

        return False

    @abstractmethod
    async def submit_job(self, job: Job) -> Union[int, str]:
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
