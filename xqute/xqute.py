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
    # DEFAULT_SUBMISSION_BATCH,
    JobStatus,
    SLEEP_INTERVAL_PRODUCER_MAX_FORKS,
    SLEEP_INTERVAL_POLLING_JOBS,
    SLEEP_INTERVAL_KEEP_FEEDING,
)
from .utils import logger, CommandType
from .plugin import plugin
from .schedulers import get_scheduler

if TYPE_CHECKING:
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
        submission_batch: The number of consumers to submit jobs. This allows
            multiple jobs to be submitted in parallel. This is useful when
            there are many jobs to be submitted and the scheduler has a high
            latency for each submission. Set this to a smaller number if the
            scheduler cannot handle too many simultaneous submissions.
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
        submission_batch: int | None = None,
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
        self._keep_feeding: bool = False
        self._completion_task: asyncio.Task | None = None

        self.buffer_queue: deque = deque()
        self._buffer_event: asyncio.Event = asyncio.Event()
        self.queue: asyncio.Queue = asyncio.Queue()

        scheduler_opts = scheduler_opts or {}
        self.scheduler = get_scheduler(scheduler)(
            workdir=workdir,
            forks=forks,
            error_strategy=error_strategy,
            num_retries=num_retries,
            jobname_prefix=jobname_prefix,
            submission_batch=submission_batch,
            **scheduler_opts,
        )

        # requires to be defined in a loop
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, functools.partial(self.cancel, sig))

        self._tasks: asyncio.Task | None = None

        logger.debug("/%s Calling on_init hook ...", self.name)
        plugin.hooks.on_init(self)

    def __del__(self) -> None:
        """Destructor to warn if stop_feeding was not called"""
        if hasattr(self, "is_feeding") and self.is_feeding():
            logger.warning(
                "/%s Instance destroyed while still in keep_feeding mode. "
                "Did you forget to call 'await xqute.stop_feeding()'?",
                self.name,
            )

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

        logger.debug("/%s Calling on_shutdown hook ...", self.name)

        # Always cancel tasks if not already cancelled, regardless of hook result
        # This prevents plugins from accidentally leaving tasks running
        if plugin.hooks.on_shutdown(self, sig) is not False:
            if self._tasks:
                self._tasks.cancel()

    async def _producer(self) -> None:
        """The producer"""
        polling_counter = 0

        try:
            while True:
                if not self.buffer_queue:
                    # If not in keep_feeding mode and buffer is empty, exit
                    if not self._keep_feeding:
                        logger.debug(
                            "/Producer Buffer empty and not in keep_feeding mode, "
                            "exiting ..."
                        )
                        break
                    logger.debug("/Producer Buffer queue is empty, waiting ...")
                    # Wait for buffer event instead of sleep polling
                    await self._buffer_event.wait()
                    self._buffer_event.clear()
                    continue

                job = self.buffer_queue.popleft()
                # Lightweight check: just count running jobs, no hooks
                n_running = await self.scheduler.count_running_jobs(self.jobs)
                if n_running >= self.scheduler.forks:
                    logger.debug("/Producer Hit max forks of scheduler ...")
                    self.buffer_queue.appendleft(job)
                    # Wait longer when hitting max forks to reduce polling overhead
                    await asyncio.sleep(SLEEP_INTERVAL_PRODUCER_MAX_FORKS)
                    polling_counter += 1
                    continue

                await job.set_status(JobStatus.QUEUED)
                await self.queue.put(job)
                polling_counter = 0  # Reset counter after successful queuing

            # Send sentinel values to stop consumers
            logger.debug("/Producer Finished, sending sentinels to consumers ...")
            for _ in range(self.scheduler.subm_batch):
                await self.queue.put(None)
        except asyncio.CancelledError:
            logger.debug("/Producer Cancelled ...")

    async def _consumer(self, index: int) -> None:
        """The consumer

        Args:
            index: The index of the consumer
        """
        try:
            while True:
                job = await self.queue.get()
                # Check for sentinel value to exit gracefully
                if job is None:
                    logger.debug("/Consumer-%s Received sentinel, exiting ...", index)
                    self.queue.task_done()
                    break

                logger.debug("/Consumer-%s submitting %s", index, job)
                await self.scheduler.submit_job_and_update_status(job)
                self.queue.task_done()
        except asyncio.CancelledError:
            logger.warning("/Consumer-%s Cancelled while submitting ...", index)

    async def feed(self, cmd: CommandType | Job, envs: dict[str, Any] = None) -> None:
        """Put a command into the buffer

        Args:
            cmd: The command
            envs: The environment variables for the job
        """
        from .job import Job

        envs = envs or {}

        if isinstance(cmd, Job):
            job = cmd
            job.envs.update(envs)
        else:
            job = await self.scheduler.create_job(len(self.jobs), cmd, envs)

        logger.debug("/Job-%s Calling on_job_init hook ...", job.index)
        await plugin.hooks.on_job_init(self.scheduler, job)
        self.jobs.append(job)
        logger.info("/%s Pushing job: %r", self.name, job)

        self.buffer_queue.append(job)
        # Signal producer that buffer has new jobs
        self._buffer_event.set()
        logger.debug("/Job-%s Calling on_job_queued hook ...", job.index)
        await plugin.hooks.on_job_queued(self.scheduler, job)

    def is_feeding(self) -> bool:
        """Check if the system is in keep_feeding mode.

        Returns:
            True if in keep_feeding mode and waiting for stop_feeding() to be called.
        """
        return (
            hasattr(self, "_keep_feeding")
            and self._keep_feeding
            and hasattr(self, "_completion_task")
            and self._completion_task
        )

    async def stop_feeding(self) -> None:
        """Stop feeding mode and wait for all jobs to complete.

        After calling this method, the producer will exit once the buffer
        queue is empty, and this method will wait for all jobs to complete.
        This should be called after all jobs have been submitted when using
        run_until_complete(keep_feeding=True).

        Raises:
            RuntimeError: If called without first calling
                run_until_complete(keep_feeding=True)
        """
        if not self.is_feeding():
            logger.error(
                "/%s stop_feeding() called but keep_feeding mode was not started. "
                "Ignoring ...",
                self.name,
            )
            return

        logger.debug("/%s Stopping feeding mode", self.name)
        self._keep_feeding = False

        # Wait for completion if we started in keep_feeding mode
        try:
            await self._completion_task
        except asyncio.CancelledError:  # pragma: no cover
            if self._tasks:
                self._tasks.cancel()

        self._completion_task = None

    async def _polling_jobs(self) -> None:
        """Polling the jobs to see if they are all done.

        If yes, cancel the producer-consumer task naturally.
        """
        try:
            # Wait for feeding to stop if in keep_feeding mode
            while self._keep_feeding:
                await asyncio.sleep(SLEEP_INTERVAL_KEEP_FEEDING)

            polling_counter = 0
            while self._cancelling is False and not await self.scheduler.check_all_done(
                self.jobs, polling_counter
            ):
                await asyncio.sleep(SLEEP_INTERVAL_POLLING_JOBS)
                polling_counter += 1

            if self._cancelling is False:
                self.cancel()
        except asyncio.CancelledError:
            logger.debug("/%s Polling cancelled ...", self.name)
            if self._cancelling not in (True, False):  # signaled
                await self.scheduler.kill_running_jobs(self.jobs)

    async def run_until_complete(self, keep_feeding: bool = False) -> None:
        """Wait until all jobs complete

        Args:
            keep_feeding: If True, starts running in background and returns immediately,
                allowing jobs to be added after calling this method.
                You must call stop_feeding() when done adding jobs, which will
                wait for all jobs to complete.
                If False (default), waits for all current jobs to complete immediately.

        Examples:
            Traditional usage:
            ```python
            xqute = Xqute()
            await xqute.feed(['echo', '1'])
            await xqute.feed(['echo', '2'])
            await xqute.run_until_complete()
            ```

            Keep feeding mode:
            ```python
            xqute = Xqute()
            await xqute.feed(['echo', '1'])
            await xqute.run_until_complete(keep_feeding=True)  # Returns immediately
            await xqute.feed(['echo', '2'])  # Can add more jobs
            await xqute.stop_feeding()  # Waits for completion
            ```
        """
        self._keep_feeding = keep_feeding

        if keep_feeding:
            # Start completion tasks in background
            logger.debug("/%s Starting in keep_feeding mode ...", self.name)
            self._completion_task = asyncio.create_task(self._run_completion_tasks())
            # Return immediately to allow more jobs to be added
            return

        # Traditional mode - wait for completion
        logger.debug(
            "/%s Done feeding jobs, waiting for jobs to be done ...", self.name
        )
        await self._run_completion_tasks()

    async def _run_completion_tasks(self) -> None:
        """Run the completion tasks (polling and await)"""
        self._tasks = asyncio.gather(
            self._producer(),
            *(self._consumer(i) for i in range(self.scheduler.subm_batch)),
            self._polling_jobs(),
        )
        try:
            await self._tasks
        except asyncio.CancelledError:
            logger.debug("/%s Completion tasks cancelled ...", self.name)
        finally:
            logger.info("/%s Done!", self.name)
            if self.plugin_context:
                self.plugin_context.__exit__()
