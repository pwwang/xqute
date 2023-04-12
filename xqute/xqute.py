"""The xqute module"""
from __future__ import annotations

import asyncio
import functools
import signal
from collections import deque
from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Type

from .defaults import (
    DEFAULT_JOB_METADIR,
    DEFAULT_JOB_ERROR_STRATEGY,
    DEFAULT_JOB_NUM_RETRIES,
    DEFAULT_SCHEDULER_FORKS,
    DEFAULT_JOB_SUBMISSION_BATCH,
    JobErrorStrategy,
    JobStatus,
)
from .utils import logger
from .plugin import plugin
from .schedulers import get_scheduler

if TYPE_CHECKING:  # pragma: no cover
    from .scheduler import Scheduler
    from .job import Job


class Xqute:
    """The main class of the package

    Attributes:
        name: The name, used in logger
        EMPTY_BUFFER_SLEEP_TIME: The time to sleep while waiting when
            the buffer is empty

        jobs: The jobs registry
        plugins: The plugins to be enabled or disabled
            to disable a plugin, using `no:plugin_name`
            either all plugin names should be prefixed with 'no:' or none
            of them should
        job_submission_batch: The number of consumers to submit jobs

        _job_metadir: The job meta directory
        _job_error_strategy: The strategy when there is error happened
        _job_num_retries: The number of retries when strategy is retry

        _cancelling: A mark to mark whether a shutting down event
            is triggered (True for natual cancelling, the signale for
            cancelling with a signal, SIGINT for example)

        buffer_queue: A buffer queue to save the pushed jobs
        queue: The job queue
        scheduler: The scheduler
        task: The task of producer and consumers

    Args:
        scheduler: The scheduler class or name
        plugins: The plugins to be enabled or disabled
            to disable a plugin, using `no:plugin_name`
            either all plugin names should be prefixed with 'no:' or none
            of them should
            To enabled plugins, objects are
        job_metadir: The job meta directory
        job_submission_batch: The number of consumers to submit jobs
        job_error_strategy: The strategy when there is error happened
        job_num_retries: Max number of retries when job_error_strategy is retry
        scheduler_forks: Max number of job forks
        **scheduler_opts: Additional keyword arguments for scheduler
    """

    name: str = "Xqute"
    EMPTY_BUFFER_SLEEP_TIME: int = 1

    def __init__(
        self,
        scheduler: str | Type[Scheduler] = "local",
        plugins: List[Any] | None = None,
        *,
        job_metadir: PathLike = DEFAULT_JOB_METADIR,
        job_submission_batch: int = DEFAULT_JOB_SUBMISSION_BATCH,
        job_error_strategy: str = DEFAULT_JOB_ERROR_STRATEGY,
        job_num_retries: int = DEFAULT_JOB_NUM_RETRIES,
        scheduler_forks: int = DEFAULT_SCHEDULER_FORKS,
        scheduler_prescript: str = "",
        scheduler_postscript: str = "",
        **scheduler_opts,
    ) -> None:
        """Construct"""
        self.jobs: List[Job] = []

        if plugins is not None:
            no_plugins = [
                isinstance(plug, str) and plug.startswith("no:")
                for plug in plugins
            ]
            if any(no_plugins) and not all(no_plugins):
                raise ValueError(
                    'Either all plugin names start with "no:" or '
                    "none of them does."
                )
            if all(no_plugins):
                self.plugin_context = plugin.plugins_but_context(
                    plug[3:] for plug in plugins
                )
            else:
                self.plugin_context = plugin.plugins_only_context(plugins)
        else:
            self.plugin_context = plugin.plugins_only_context(plugins)

        if self.plugin_context:
            self.plugin_context.__enter__()

        logger.info(
            "/%s Enabled plugins: %s",
            self.name,
            plugin.get_enabled_plugin_names(),
        )

        self.job_submission_batch = job_submission_batch
        self._job_metadir = job_metadir
        Path(self._job_metadir).mkdir(exist_ok=True)
        self._job_error_strategy = job_error_strategy
        self._job_num_retries = job_num_retries

        self._cancelling: bool | signal.Signals = False

        self.buffer_queue: deque = deque()
        self.queue: asyncio.Queue = asyncio.Queue()
        self.scheduler = get_scheduler(scheduler)(
            scheduler_forks,
            scheduler_prescript,
            scheduler_postscript,
            **scheduler_opts,
        )

        # requires to be defined in a loop
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, functools.partial(self.cancel, sig))

        self.task = asyncio.gather(
            self._producer(),
            *(self._consumer(i) for i in range(self.job_submission_batch)),
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

    async def _producer(self):
        """The producer"""

        while True:
            if not self.buffer_queue:
                logger.debug(
                    "/%s Buffer queue is empty, waiting ...", self.name
                )
                await asyncio.sleep(self.EMPTY_BUFFER_SLEEP_TIME)
                continue

            job = self.buffer_queue.popleft()
            if not await self.scheduler.polling_jobs(
                self.jobs,
                "can_submit",
                self._job_error_strategy == JobErrorStrategy.HALT,
            ):
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
            logger.debug(
                "/%s 'Consumer-%s' submitting %s", self.name, index, job
            )
            await self.scheduler.submit_job_and_update_status(job)
            self.queue.task_done()

    async def put(self, cmd: Job | str | List[str]) -> None:
        """Put a command into the buffer

        Args:
            cmd: The command
        """
        if isinstance(cmd, self.scheduler.job_class):
            job = cmd
            if job._error_retry is None:
                job._error_retry = (
                    self._job_error_strategy == JobErrorStrategy.RETRY
                )
            if job._num_retries is None:
                job._num_retries = self._job_num_retries
        else:
            job = self.scheduler.job_class(
                len(self.jobs),
                cmd,  # type: ignore
                self._job_metadir,
                self._job_error_strategy == JobErrorStrategy.RETRY,
                self._job_num_retries,
            )
        await plugin.hooks.on_job_init(self.scheduler, job)
        self.jobs.append(job)
        logger.info("/%s Pushing job: %r", self.name, job)

        self.buffer_queue.append(job)
        await plugin.hooks.on_job_queued(self.scheduler, job)

    async def _polling_jobs(self) -> None:
        """Polling the jobs to see if they are all done.

        If yes, cancel the producer-consumer task natually.
        """
        while (
            self._cancelling is False
            and not await self.scheduler.polling_jobs(
                self.jobs,
                "all_done",
                self._job_error_strategy == JobErrorStrategy.HALT,
            )
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
