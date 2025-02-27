"""The xqute module"""

from __future__ import annotations

import asyncio
import functools
import signal
from collections import deque
from typing import TYPE_CHECKING, Any, List, Mapping, Type

from .defaults import (
    DEFAULT_WORKDIR,
    DEFAULT_ERROR_STRATEGY,
    DEFAULT_NUM_RETRIES,
    DEFAULT_SCHEDULER_FORKS,
    DEFAULT_SUBMISSION_BATCH,
    JobStatus,
)
from .utils import logger, CommandType
from .plugin import plugin
from .schedulers import get_scheduler

if TYPE_CHECKING:  # pragma: no cover
    from .path import PathType
    from .scheduler import Scheduler
    from .job import Job


class Xqute:
    """The main class of the package

    Attributes:
        name: The name, used in logger
        EMPTY_BUFFER_SLEEP_TIME: The time to sleep while waiting when
            the buffer is empty to wait for the jobs to be pushed

        jobs: The jobs registry
        plugins: The plugins to be enabled or disabled
            to disable a plugin, using `-plugin_name`
            either all plugin names should be prefixed with '+'/'-' or none
            of them should

        _cancelling: A mark to mark whether a shutting down event
            is triggered (True for natural cancelling, the signal for
            cancelling with a signal, SIGINT for example)

        buffer_queue: A buffer queue to save the pushed jobs
        queue: The job queue
        scheduler: The scheduler
        task: The task of producer and consumers

    Args:
        scheduler: The scheduler class or name
        plugins: The plugins to be enabled or disabled
            to disable a plugin, using `-plugin_name`
            either all plugin names should be prefixed with '+'/'-' or none
            of them should
        workdir: The job meta directory
        submission_batch: The number of consumers to submit jobs. So that the
            submission process won't exhaust the local resources
        error_strategy: The strategy when there is error happened
        num_retries: Max number of retries when error_strategy is retry
        forks: Max number of job forks for scheduler
        scheduler_opts: Additional keyword arguments for scheduler
    """

    name: str = "Xqute"
    EMPTY_BUFFER_SLEEP_TIME: int = 1

    def __init__(
        self,
        scheduler: str | Type[Scheduler] = "local",
        *,
        plugins: List[Any] | None = None,
        workdir: str | PathType = DEFAULT_WORKDIR,
        submission_batch: int = DEFAULT_SUBMISSION_BATCH,
        error_strategy: str = DEFAULT_ERROR_STRATEGY,
        num_retries: int = DEFAULT_NUM_RETRIES,
        forks: int = DEFAULT_SCHEDULER_FORKS,
        scheduler_opts: Mapping[str, Any] | None = None,
        jobname_prefix: str | None = None,
    ) -> None:
        self.jobs: List[Job] = []

        self.plugin_context = plugin.plugins_context(plugins)

        self.plugin_context.__enter__()

        logger.info(
            "/%s Enabled plugins: %s",
            self.name,
            plugin.get_enabled_plugin_names(),
        )

        self._cancelling: bool | signal.Signals = False

        self.buffer_queue: deque = deque()
        self.queue: asyncio.Queue = asyncio.Queue()

        scheduler_opts = scheduler_opts or {}
        self.scheduler = get_scheduler(scheduler)(
            workdir=workdir,
            forks=forks,
            error_strategy=error_strategy,
            num_retries=num_retries,
            jobname_prefix=jobname_prefix,
            **scheduler_opts,
        )

        # requires to be defined in a loop
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, functools.partial(self.cancel, sig))

        self.task = asyncio.gather(
            self._producer(),
            *(self._consumer(i) for i in range(submission_batch)),
        )
        plugin.hooks.on_init(self)

    def cancel(self, sig: signal.Signals | None = None) -> None:
        """Cancel the producer-consumer task

        `self._cancelling` will be set to `signaled` if sig is provided,
        otherwise it will be set to `True`

        Args:
            sig: Whether this cancelling is caused by a signal
        """
        self._cancelling = True
        if sig:
            self._cancelling = sig
            logger.warning(
                "/%s Got signal %r, trying a graceful " "shutdown ...",
                self.name,
                sig.name,
            )

        if plugin.hooks.on_shutdown(self, sig) is not False:
            self.task.cancel()

    async def _producer(self) -> None:
        """The producer"""

        while True:
            if not self.buffer_queue:
                logger.debug("/%s Buffer queue is empty, waiting ...", self.name)
                await asyncio.sleep(self.EMPTY_BUFFER_SLEEP_TIME)
                continue

            job = self.buffer_queue.popleft()
            if not await self.scheduler.polling_jobs(self.jobs, "submittable"):
                logger.debug("/%s Hit max forks of scheduler ...", self.name)
                await asyncio.sleep(0.1)
                self.buffer_queue.appendleft(job)
                continue

            job.status = JobStatus.QUEUED
            await self.queue.put(job)

    async def _consumer(self, index: int) -> None:
        """The consumer

        Args:
            index: The index of the consumer
        """
        while True:
            job = await self.queue.get()
            logger.debug("/%s 'Consumer-%s' submitting %s", self.name, index, job)
            await self.scheduler.submit_job_and_update_status(job)
            self.queue.task_done()

    async def put(self, cmd: CommandType | Job) -> None:
        """Put a command into the buffer

        Args:
            cmd: The command
        """
        from .job import Job

        if isinstance(cmd, Job):
            job = cmd
        else:
            job = self.scheduler.create_job(len(self.jobs), cmd)

        await plugin.hooks.on_job_init(self.scheduler, job)
        self.jobs.append(job)
        logger.info("/%s Pushing job: %r", self.name, job)

        self.buffer_queue.append(job)
        await plugin.hooks.on_job_queued(self.scheduler, job)

    async def _polling_jobs(self) -> None:
        """Polling the jobs to see if they are all done.

        If yes, cancel the producer-consumer task naturally.
        """
        while self._cancelling is False and not await self.scheduler.polling_jobs(
            self.jobs,
            "all_done",
        ):
            await asyncio.sleep(1.0)

        if self._cancelling is False:
            self.cancel()

    async def _await_task(self) -> None:
        """Await the producer-consumer task and catch the CancelledError"""
        try:
            await self.task
        except asyncio.CancelledError:
            logger.debug("/%s Stoping producer and consumer ...", self.name)
            if self._cancelling not in (True, False):  # signaled
                await self.scheduler.kill_running_jobs(self.jobs)

        logger.info("/%s Done!", self.name)

    async def run_until_complete(self) -> None:
        """Wait until all jobs complete"""
        logger.debug(
            "/%s Done feeding jobs, waiting for jobs to be done ...", self.name
        )
        try:
            await asyncio.gather(self._polling_jobs(), self._await_task())
        finally:
            if self.plugin_context:
                self.plugin_context.__exit__()
