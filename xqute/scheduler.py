"""The scheduler to schedule jobs"""
import os
import signal
from abc import ABC, abstractmethod
from typing import List, Type, Union
from diot import Diot
from .utils import logger, JobStatus, asyncify, a_write_text
from .job import Job
from .plugin import plugin

class Scheduler(ABC):
    """The abstract class for scheduler

    Attributes:
        job_class: The job class

    Args:
        jobs: The reference to the list of jobs
        forks: Max number of job forks
        **kwargs: Other arguments for the scheduler
    """
    __slots__ = ('config', )
    name: str
    job_class: Type[Job]

    def __init__(self, forks: int, **kwargs):
        """Construct"""
        self.config = Diot(forks=forks, **kwargs)

    def can_submit(self, jobs: List[Job]) -> bool:
        """Whether we can submit a job.

        Depending on whether number of running jobs < self.config.forks

        Args:
            jobs: The list of all jobs

        Returns:
            True if yes otherwise False
        """
        n_running = 0
        for job in jobs:
            status = job.status
            if status in (JobStatus.QUEUED,
                          JobStatus.SUBMITTED,
                          JobStatus.RUNNING,
                          JobStatus.KILLING):
                n_running += 1
        return n_running < self.config.forks

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
            job.uid = await self.submit_job(job)
            logger.info('/Scheduler-%s Job %s submitted (uid: %s, wrapped: %s)',
                        self.name,
                        job.index,
                        job.uid,
                        await job.wrapped_script(self))

            job.status = JobStatus.SUBMITTED
            await plugin.hooks.on_job_submitted(self, job)
        except Exception as exc: # pragma: no cover
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
        except FileNotFoundError: # pragma: no cover
            pass
        job.status = JobStatus.FINISHED
        await plugin.hooks.on_job_killed(self, job)

    async def polling_jobs(self, jobs: List[Job], halt_on_error: bool) -> bool:
        """Check if all jobs are done

        Args:
            jobs: The list of jobs
            halt_on_error: Whether we should halt the whole pipeline on error

        Returns:
            True if yes otherwise False.
        """
        ret = True
        for job in jobs:
            status = job.status
            if not job.status_changed:
                if status in (JobStatus.FAILED, JobStatus.RETRYING):
                    await plugin.hooks.on_job_failed(self, job)
                elif status == JobStatus.FINISHED:
                    await plugin.hooks.on_job_succeeded(self, job)

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
                if not halt_on_error:
                    return False
        return ret

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
