"""The scheduler to schedule jobs"""

from __future__ import annotations

import os
import shlex
import signal
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, List, Type

from panpath import CloudPath
from diot import Diot  # type: ignore

from .defaults import (
    JobStatus,
    JobErrorStrategy,
    JOBCMD_WRAPPER_LANG,
    JOBCMD_WRAPPER_TEMPLATE,
    DEFAULT_ERROR_STRATEGY,
    DEFAULT_NUM_RETRIES,
    DEFAULT_SUBMISSION_BATCH,
    get_jobcmd_wrapper_init,
)
from .utils import logger, CommandType
from .path import SpecPath
from .job import Job
from .plugin import plugin


class Scheduler(ABC):
    """The abstract class for scheduler

    Attributes:
        name: The name of the scheduler
        jobcmd_wrapper_init: The init script for the job command wrapper

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
        submission_batch: The number of consumers to submit jobs. This allows
            multiple jobs to be submitted in parallel. This is useful when
            there are many jobs to be submitted and the scheduler has a high
            latency for each submission. Set this to a smaller number if the
            scheduler cannot handle too many simultaneous submissions.
        recheck_interval: The number of polling iterations between rechecks of
            whether a job is still running on the scheduler. Helps detect jobs
            that fail before the wrapped script updates status (e.g., resource
            allocation failures). Each iteration takes ~0.1s, so default 600 means
            rechecking every ~60 seconds.
        cwd: The working directory for the job command wrapper
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
        "recheck_interval",
        "subm_batch",
        "cwd",
    )

    # The name of the scheduler
    name: str
    # The number of consumers to submit jobs in parallel
    submission_batch: int = DEFAULT_SUBMISSION_BATCH
    job_class: Type[Job] = Job

    def __init__(
        self,
        workdir: str | Path,
        forks: int = 1,
        error_strategy: str = DEFAULT_ERROR_STRATEGY,
        num_retries: int = DEFAULT_NUM_RETRIES,
        prescript: str = "",
        postscript: str = "",
        jobname_prefix: str | None = None,
        submission_batch: int | None = None,
        recheck_interval: int = 600,
        cwd: str | Path = None,
        **kwargs,
    ):
        self.forks = forks
        mounted_workdir = kwargs.pop("mounted_workdir", None)
        self.workdir = SpecPath(workdir, mounted=mounted_workdir)

        self.error_strategy = error_strategy
        self.num_retries = num_retries
        self.prescript = prescript
        self.postscript = postscript
        self.jobname_prefix = jobname_prefix or self.name
        self.subm_batch = submission_batch or self.__class__.submission_batch
        self.recheck_interval = recheck_interval
        self.cwd = None if cwd is None else str(cwd)

        self.config = Diot(**kwargs)

    async def create_job(
        self,
        index: int,
        cmd: CommandType,
        envs: dict[str, Any] | None = None,
    ) -> Job:
        """Create a job

        Args:
            index: The index of the job
            cmd: The command of the job

        Returns:
            The job
        """
        job = self.job_class(
            index=index,
            cmd=cmd,
            workdir=self.workdir,
            error_retry=self.error_strategy == JobErrorStrategy.RETRY,
            num_retries=self.num_retries,
            envs=envs,
        )
        logger.debug("/Job-%s Creating metadir: %s", job.index, job.metadir)
        await job.metadir.a_mkdir(parents=True, exist_ok=True)
        return job

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
                "/Job-%s Skip submitting, already submitted or running.",
                job.index,
            )
            return

        exception: Exception | None = None
        try:
            logger.debug("/Job-%s Calling on_job_submitting hook ...", job.index)
            if await plugin.hooks.on_job_submitting(self, job) is False:
                logger.info("/Job-%s submission cancelled by hook.", job.index)
                return

            logger.debug("/Job-%s Cleaning up before submission", job.index)
            await job.clean()

            try:
                # raise the exception immediately
                # it somehow cannot be catched immediately
                logger.debug(
                    "/Job-%s Submitting to scheduler '%s' ...",
                    job.index,
                    self.name,
                )
                await job.set_jid(await self.submit_job(job))
            except Exception as exc:
                exception = RuntimeError(f"Failed to submit job: {exc}")
                exception.__traceback__ = exc.__traceback__
            else:
                await self.transition_job_status(job, JobStatus.SUBMITTED)

        except Exception as exc:  # pragma: no cover
            exception = exc

        if exception is not None:
            from traceback import format_exception

            error_msg = "".join(
                format_exception(
                    type(exception),
                    exception,
                    exception.__traceback__,
                )
            )
            await job.stderr_file.a_write_text(error_msg)
            await self.transition_job_status(job, JobStatus.FAILED, rc="-2")

    async def transition_job_status(
        self,
        job: Job,
        new_status: int,
        old_status: int | None = None,
        flush: bool = True,
        rc: str | None = None,
        error_msg: str | None = None,
        is_killed: bool = False,
    ):
        """Centralized status transition handler

        Handles all aspects of job status transitions:
        - Status change logging
        - Hook lifecycle management (ensuring on_job_started is called)
        - Appropriate hook calls based on new status
        - RC file updates
        - Error message appending to stderr
        - JID file cleanup for terminal states
        - Pipeline halt on errors if configured

        Note that this method will not flush status changes to disk (job.status_file).
        You need to call job.set_status() separately if needed.

        Args:
            job: The job to transition
            new_status: The new status to transition to
            old_status: The previous status (if known).
                If None, will use job._status
            flush: Whether to flush the status to disk
            rc: Optional return code to write to rc_file
            error_msg: Optional error message to append to stderr_file
            is_killed: Whether this is a killed job (uses on_job_killed hook)
        """
        # Save the previous status before updating
        # (setter will update prev_status, so we need to save it first)
        old_status = job._status if old_status is None else old_status
        await job.set_status(new_status, flush=flush)

        # Handle killed jobs specially
        if is_killed:
            logger.debug("/Job-%s Calling on_job_killed hook ...", job.index)
            await plugin.hooks.on_job_killed(self, job)
            return

        # Handle status-specific logic
        if new_status == JobStatus.FAILED:
            # Ensure lifecycle hook was called
            # Job may go from SUBMITTED to FAILED directly (finished too soon)
            if old_status != JobStatus.RUNNING:
                await self.transition_job_status(
                    job,
                    JobStatus.RUNNING,
                    old_status=old_status,
                    flush=False,
                )
                await self.transition_job_status(
                    job,
                    new_status,
                    old_status=JobStatus.RUNNING,
                    flush=False,
                )
                return

            # Write rc file if provided
            if rc is not None:  # pragma: no cover
                await job.set_rc(rc)

            # Append error message if provided
            if error_msg is not None:  # pragma: no cover
                async with job.stderr_file.a_open("a") as f:
                    f.write(f"\n{error_msg}\n")

            # Call failure hook
            logger.debug("/Job-%s Calling on_job_failed hook ...", job.index)
            await plugin.hooks.on_job_failed(self, job)

            # Clean up jid file
            await job.jid_file.a_unlink(missing_ok=True)

            # Handle halt strategy
            if self.error_strategy == JobErrorStrategy.HALT:
                logger.error(
                    "/Sched-%s Pipeline will halt since job failed: %r",
                    self.name,
                    job,
                )
                os.kill(os.getpid(), signal.SIGTERM)

        elif new_status == JobStatus.FINISHED:
            # Ensure lifecycle hook was called
            if old_status != JobStatus.RUNNING:  # pragma: no cover
                await self.transition_job_status(
                    job,
                    JobStatus.RUNNING,
                    old_status=old_status,
                    flush=False,
                )
                await self.transition_job_status(
                    job,
                    new_status,
                    old_status=JobStatus.RUNNING,
                    flush=False,
                )
                return

            # Call success hook
            logger.debug("/Job-%s Calling on_job_succeeded hook ...", job.index)
            await plugin.hooks.on_job_succeeded(self, job)

            # Clean up jid file
            await job.jid_file.a_unlink(missing_ok=True)

        elif new_status == JobStatus.RUNNING:
            # Call started hook
            logger.debug("/Job-%s Calling on_job_started hook ...", job.index)
            await plugin.hooks.on_job_started(self, job)

        elif new_status == JobStatus.SUBMITTED:
            # Call submitted hook
            logger.info(
                "/Sched-%s Job %s submitted (jid: %s, wrapped: %s)",
                self.name,
                job.index,
                await job.get_jid(),
                await self.wrapped_job_script(job),
            )
            logger.debug("/Job-%s Calling on_job_submitted hook ...", job.index)
            await plugin.hooks.on_job_submitted(self, job)

    async def kill_job_and_update_status(self, job: Job):
        """Kill a job and update its status

        Args:
            job: The job
        """
        await job.set_status(JobStatus.KILLING)
        logger.debug("/Job-%s Calling on_job_killing hook ...", job.index)
        ret = await plugin.hooks.on_job_killing(self, job)
        if ret is False:
            logger.info(
                "/Sched-%s Job %s killing cancelled by hook.",
                self.name,
                job.index,
            )
            return

        logger.warning("/Sched-%s Killing job %s ...", self.name, job.index)
        await self.kill_job(job)

        await self.transition_job_status(job, JobStatus.FINISHED, is_killed=True)

    async def retry_job(self, job: Job):
        """Retry a job

        Args:
            job: The job
        """
        await job.set_jid("")
        await job.clean(retry=True)
        job.trial_count += 1
        logger.warning(
            "/Sched-%s Retrying (#%s) job: %r",
            self.name,
            job.trial_count,
            job,
        )
        await self.transition_job_status(job, JobStatus.RETRYING, flush=False)
        await self.submit_job_and_update_status(job)

    async def count_running_jobs(self, jobs: List[Job]) -> int:
        """Count currently running/active jobs (lightweight check)

        This is optimized for the producer to check if new jobs can be submitted.
        It only counts jobs without refreshing status or calling hooks.

        Args:
            jobs: The list of jobs

        Returns:
            Number of jobs currently in active states
        """
        # check_job_done() has updated _status already
        # statuses = await asyncio.gather(*(job.get_status() for job in jobs))
        return sum(
            1
            for job in jobs
            if job._status
            in (
                JobStatus.QUEUED,
                JobStatus.SUBMITTED,
                JobStatus.RUNNING,
                JobStatus.KILLING,
            )
        )

    async def _check_job_done(self, job: Job, polling_counter: int) -> bool | str:
        """Check if a single job is done (lightweight check)

        This is optimized for the producer to check if new jobs can be submitted.
        It only checks the status without refreshing or calling hooks.

        Args:
            job: The job
            polling_counter: The polling counter for hook calls

        Returns:
            True if the job is done, False if not.
            If the job failed, return the "failed".
        """
        prev_status = job._status
        status = await job.get_status(refresh=True)
        # Keep the previous status, which will be updated later
        # So that later parts can know the previous status
        job._status = prev_status

        # Status changed
        if status != prev_status:
            await self.transition_job_status(
                job,
                status,
                old_status=prev_status,
                flush=False,
            )

        elif status == JobStatus.SUBMITTED:
            # Status keep being SUBMITTED
            # Check if the job fails before running
            if await self.job_fails_before_running(job):  # pragma: no cover
                logger.warning(
                    "/Sched-%s Job %s seems to fail before running, "
                    "check your scheduler logs if necessary.",
                    self.name,
                    job.index,
                )
                await self.transition_job_status(
                    job,
                    JobStatus.FAILED,
                    old_status=prev_status,
                    flush=False,
                    rc="-3",
                    error_msg=(
                        "Error: job seems to fail before running.\n"
                        "Check your scheduler logs if necessary."
                    ),
                )
                # transition_job_status handles the HALT, but we still need to break
                if self.error_strategy == JobErrorStrategy.HALT:
                    return "failed"

        elif status == JobStatus.RUNNING:
            # Status keep being RUNNING
            logger.debug(
                "/Sched-%s Job %s is running, calling polling hook ...",
                self.name,
                job.index,
            )
            # Call the polling hook
            logger.debug("/Job-%s Calling on_job_polling hook ...", job.index)
            await plugin.hooks.on_job_polling(self, job, polling_counter)
            # Let's make sure the job is really running
            if (
                not await job.rc_file.a_is_file()
                and (polling_counter + 1) % self.recheck_interval == 0
                and not await self.job_is_running(job)
            ):  # pragma: no cover
                logger.warning(
                    "/Sched-%s Job %s is not running in the scheduler, "
                    "but its status is still RUNNING, setting it to FAILED",
                    self.name,
                    job.index,
                )
                await self.transition_job_status(
                    job,
                    JobStatus.FAILED,
                    old_status=prev_status,
                    flush=False,
                    rc="-3",
                    error_msg=(
                        "Error: job is not running in the scheduler, "
                        "but its status is still RUNNING.\n"
                        "It is likely that the resource is preempted."
                    ),
                )
                # transition_job_status handles the HALT, but we still need to break
                if self.error_strategy == JobErrorStrategy.HALT:
                    return "failed"

        # Check if we need to halt - this catches any FAILED status
        # transition_job_status already sent SIGTERM, we just need to break the loop
        if self.error_strategy == JobErrorStrategy.HALT and status == JobStatus.FAILED:
            return "failed"

        if status not in (JobStatus.FINISHED, JobStatus.FAILED):
            logger.debug(
                "/Sched-%s Not all jobs are done yet, job %s is %s",
                self.name,
                job.index,
                JobStatus.get_name(status),
            )
            return False

        # Try to resubmit the job for retrying
        if (
            status == JobStatus.FAILED
            and job._error_retry
            and job.trial_count < job._num_retries
        ):
            logger.debug(
                "/Sched-%s Job %s is retrying ...",
                self.name,
                job.index,
            )
            await self.retry_job(job)
            return False

        return True

    async def check_all_done(
        self,
        jobs: List[Job],
        polling_counter: int,
    ) -> bool:
        """Check if all jobs are done (full polling with hooks)

        This does complete status refresh and calls all lifecycle hooks.
        Used by the main polling loop to track job completion.

        Args:
            jobs: The list of jobs
            polling_counter: The polling counter for hook calls

        Returns:
            True if all jobs are done, False otherwise
        """
        ret = True
        logger.debug(
            "/Sched-%s Checking if all jobs are done (#%s) ...",
            self.name,
            polling_counter,
        )

        for job in jobs:
            # We don't use gather here, since we may break the loop early
            job_done = await self._check_job_done(job, polling_counter)
            if job_done == "failed":
                ret = False
                break

            if not job_done:
                ret = False

        return ret

    async def kill_running_jobs(self, jobs: List[Job]):
        """Try to kill all running jobs

        Args:
            jobs: The list of jobs
        """
        logger.warning("/Sched-%s Killing running jobs ...", self.name)
        for job in jobs:
            status = await job.get_status()
            if status in (JobStatus.SUBMITTED, JobStatus.RUNNING):
                await self.kill_job_and_update_status(job)

    async def job_is_submitted_or_running(self, job: Job) -> bool:
        """Check if a job is already submitted or running

        Args:
            job: The job

        Returns:
            True if yes otherwise False.
        """
        if await job.jid_file.a_is_file():
            if await self.job_is_running(job):
                await job.set_status(JobStatus.SUBMITTED)
                return True
        return False

    async def job_fails_before_running(self, job: Job) -> bool:
        """Check if a job fails before running.

        For some schedulers, the job might fail before running (after submission).
        For example, the job might fail to allocate resources. In such a case,
        the wrapped script might not be executed, and the job status will not be
        updated (stays in SUBMITTED). We need to check such jobs and mark them as
        FAILED.

        For the instant scheduler, for example, the local scheduler, the failure will
        be immediately reported when submitting the job, so we don't need to check
        such jobs.

        Args:
            job: The job to check

        Returns:
            True if the job fails before running, otherwise False.
        """
        return False

    @property
    def jobcmd_wrapper_init(self) -> str:
        """The init script for the job command wrapper"""
        wrapper_init = get_jobcmd_wrapper_init(
            not isinstance(self.workdir.mounted, CloudPath)
        )
        if self.cwd:
            # Some schedulers (e.g. Google Cloud Batch) doesn't support changing the
            # working directory via configuration, so we need to change it in the
            # wrapper script.
            # See: https://issuetracker.google.com/issues/336164416
            wrapper_init = f"cd {shlex.quote(self.cwd)}\n\n{wrapper_init}"

        return wrapper_init

    def jobcmd_shebang(self, job: Job) -> str:
        """The shebang of the wrapper script"""
        wrapper_lang = (
            JOBCMD_WRAPPER_LANG
            if isinstance(JOBCMD_WRAPPER_LANG, (tuple, list))
            else [JOBCMD_WRAPPER_LANG]
        )
        return shlex.join(wrapper_lang)

    def jobcmd_init(self, job) -> str:
        """The job command init"""
        init_code = []
        if job.envs:
            init_code.append("# Environment variables")
            init_code.extend(
                [
                    f"export {key}={shlex.quote(str(value))}"
                    for key, value in job.envs.items()
                ]
            )

        codes = plugin.hooks.on_jobcmd_init(self, job)
        init_code.extend([code for code in codes if code])
        return "\n".join(init_code)

    def jobcmd_prep(self, job) -> str:
        """The job command preparation"""
        codes = plugin.hooks.on_jobcmd_prep(self, job)
        codes = [code for code in codes if code]
        return "\n".join(codes)

    def jobcmd_end(self, job) -> str:
        """The job command end"""
        codes = plugin.hooks.on_jobcmd_end(self, job)
        codes = [code for code in codes if code]
        return "\n".join(codes)

    def wrap_job_script(self, job: Job) -> str:
        """Wrap the job script

        Args:
            job: The job

        Returns:
            The wrapped script
        """
        return JOBCMD_WRAPPER_TEMPLATE.format(
            scheduler=self,
            shebang=self.jobcmd_shebang(job),
            status=JobStatus,
            job=job,
            cmd=shlex.join(job.cmd),
            jobcmd_init=self.jobcmd_init(job),
            jobcmd_prep=self.jobcmd_prep(job),
            jobcmd_end=self.jobcmd_end(job),
        )

    async def wrapped_job_script(self, job: Job) -> SpecPath:
        """Get the wrapped job script

        Args:
            job: The job

        Returns:
            The path of the wrapped job script
        """
        base = f"job.wrapped.{self.name}"
        wrapt_script = job.metadir / base
        await wrapt_script.a_write_text(self.wrap_job_script(job))

        return wrapt_script

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
