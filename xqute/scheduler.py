"""The scheduler to schedule jobs"""
import os
import signal
from abc import ABC, abstractmethod
from typing import List, Type, Union
from diot import Diot
from .utils import logger, JobStatus, asyncify, a_write_text
from .job import Job
from .plugin import simplug

class Scheduler(ABC):
    """The abstract class for scheduler

    Attributes:
        name: The name of the scheduler, used to Xqute init to
            specify a scheduler by name
        job_class: The job class

    Args:
        jobs: The reference to the list of jobs
        forks: Max number of job forks
        **kwargs: Other arguments for the scheduler
    """
    name: str
    job_class: Type[Job]

    def __init__(self, jobs: List[Job], forks: int, **kwargs):
        """Construct"""
        self.config = Diot(forks=forks, **kwargs)
        self.jobs = jobs

    async def can_submit(self) -> bool:
        """Whether we can submit a job.

        Depending on whether number of running jobs < self.config.forks

        Returns:
            True if yes otherwise False
        """
        n_running = 0
        for job in self.jobs:
            status = await job.status
            if status in (JobStatus.SUBMITTED,
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
            await a_write_text(job.lock_file, '')
            await job.clean()
            await job.wrap_cmd(self)
            logger.info('/Scheduler-%s Submitting job: %r', self.name, job)
            job_uid = await self.submit_job(job)
            job.uid = job_uid
            # we don't have to, but in case the uid is used later
            await a_write_text(job.lock_file, str(job_uid))
            logger.info('/Scheduler-%s - uid: %s', self.name, job.uid)
            logger.info('/Scheduler-%s - wrapped_cmd: %s',
                        self.name, job.wrapped_cmd)
            job.status = JobStatus.SUBMITTED
            await simplug.hooks.on_job_submitted(self, job)
        except Exception as exc: # pragma: no cover
            await a_write_text(job.stderr_file, str(exc))
            await a_write_text(job.rc_file, '-2')
            job.status = JobStatus.FAILED
            raise

    async def kill_job_and_update_status(self, job: Job):
        """Kill a job and update its status

        Args:
            job: The job
        """
        job.status = JobStatus.KILLING
        rets = await simplug.hooks.on_job_killing(self, job)
        if not any(ret is False for ret in rets):
            await self.kill_job(job)
        await asyncify(os.unlink)(job.lock_file)
        job.status = JobStatus.FINISHED
        await simplug.hooks.on_job_killed(self, job)

    async def all_job_done(self, halt_on_error: bool) -> bool:
        """Check if all jobs are done

        Args:
            halt_on_error: Whether we should halt the whole pipeline on error

        Returns:
            True if yes otherwise False.
        """
        for job in self.jobs:
            status = await job.status
            if not job.hook_done:
                if status in (JobStatus.FAILED, JobStatus.RETRYING):
                    await simplug.hooks.on_job_failed(self, job)
                elif status == JobStatus.FINISHED:
                    await simplug.hooks.on_job_succeeded(self, job)

            if halt_on_error and status == JobStatus.FAILED:
                logger.error('/Scheduler-%s Pipeline will halt '
                             'since job failed: %r', self.name, job)
                await asyncify(os.kill)(os.getpid(), signal.SIGTERM)
                job.status = JobStatus.FINISHED

            # Try to resubmit the job for retrying
            if status == JobStatus.RETRYING:
                await self.retry_job(job)
                return False

            if status not in (JobStatus.FINISHED, JobStatus.FAILED):
                return False
        return True

    async def kill_running_jobs(self):
        """Try to kill all running jobs"""
        logger.warning('/Scheduler-%s Killing running jobs ...', self.name)
        for job in self.jobs:
            status = await job.status
            if status in (JobStatus.SUBMITTED, JobStatus.RUNNING):
                await self.kill_job_and_update_status(job)

    async def retry_job(self, job: Job):
        """Retry a job

        Args:
            job: The job
        """
        await job.clean(retry=True)
        job.trial_count += 1
        logger.warning('/Scheduler-%s Retrying (#%s) job: %r',
                       self.name, job.trial_count, job)
        job_uid = await self.submit_job(job)
        job.lock_file.write_text(str(job_uid))
        job.uid = job_uid
        job.status = JobStatus.SUBMITTED

    async def job_is_submitted_or_running(self, job: Job) -> True:
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
