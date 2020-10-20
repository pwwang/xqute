"""The xqute module"""
from typing import Any, List, Type, Union
from os import PathLike
from pathlib import Path
import signal
import functools
import asyncio
from .defaults import (
    DEFAULT_JOB_METADIR,
    DEFAULT_JOB_ERROR_STRATEGY,
    DEFAULT_JOB_NUM_RETRIES,
    DEFAULT_SCHEDULER_FORKS
)
from .utils import JobErrorStrategy, logger, JobStatus
from .consumer import Consumer
from .scheduler import Scheduler
from .schedulers import get_scheduler
from .plugin import simplug

class Xqute:
    """The main class of the package

    Attributes:
        name: The name, used in logger

        _job_error_strategy: The strategy when there is error happened
        _job_num_retries: The number of retries when strategy is retry
        _shutting_down: A mark to mark whether a shutting down event
            is triggered
        _job_metadir: The job meta directory

        jobs: The jobs registry
        scheduler: The scheduler
        queue: The job queue
        consumer: The consumer

    Args:
        scheduler: The scheduler class or name
        job_metadir: The job meta directory
        job_error_strategy: The strategy when there is error happened
        job_num_retries: Max number of retries when job_error_strategy is retry
        scheduler_forks: Max number of job forks
        **kwargs: Additional keyword arguments for scheduler
    """
    name: str = 'Xqute'

    def __init__(
            self,
            scheduler: Union[str, Type[Scheduler]] = 'local',
            *,
            job_metadir: PathLike = DEFAULT_JOB_METADIR,
            job_error_strategy: str = DEFAULT_JOB_ERROR_STRATEGY,
            job_num_retries: int = DEFAULT_JOB_NUM_RETRIES,
            scheduler_forks: int = DEFAULT_SCHEDULER_FORKS,
            **kwargs
    ):
        """Construct"""
        if not isinstance(scheduler, type):
            scheduler = get_scheduler(scheduler)

        self._job_error_strategy = job_error_strategy
        self._job_num_retries = job_num_retries
        self._shutting_down = False
        self._job_metadir = Path(job_metadir)
        self._job_metadir.mkdir(exist_ok=True, parents=True)

        logger.info('/%s Registered plugins: %s',
                    self.name, simplug.get_all_plugin_names())

        self.jobs = []
        self.scheduler = scheduler(self.jobs,
                                   scheduler_forks,
                                   **kwargs)
        self.queue = asyncio.Queue()
        self.consumer = Consumer(
            self.queue,
            self.scheduler
        )

        self._register_signal_handler()
        self.consumer.create_task(self._task_done)

    def _task_done(self, fut): # pylint: disable=unused-argument
        """A callback for the consumer task"""
        self._shutting_down = True

    def _register_signal_handler(self):
        """Signal handler to gracefully shutdown the pipeline"""
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError as exc:
            raise RuntimeError(
                f'{self.__class__.__name__} should be inistantized '
                'in an event loop.'
            ).with_traceback(exc.__traceback__) from None
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(
                sig,
                functools.partial(self._shutdown, sig)
            )
        loop.set_exception_handler(self._handler_exception)

    def _handler_exception(self,
                           loop, # pylint: disable=unused-argument
                           context): # pragma: no cover
        """Asyncio exception handler"""
        exc = context.get('exception', context['message'])
        self._shutdown(exc)

    def _shutdown(self, sig: Any):
        """Start async shutdown task

        Args:
            sig: Any signal received
        """
        self._shutting_down = True
        logger.warning('/%s Got signal %r, trying a graceful shutdown ...',
                       self.name, sig.name)

        asyncio.create_task(self._async_shutdown())

    async def _async_shutdown(self):
        """The async shutdown task"""
        rets = await simplug.hooks.on_shutdown(self.scheduler, self.consumer)
        if any(ret is False for ret in rets):
            return
        # Don't consume anymore
        self.consumer.task.cancel()
        await self.scheduler.kill_running_jobs()

    async def push(self, cmd: List[str]):
        """Push a command to the queue

        Args:
            cmd: The command to push
        """
        job = self.scheduler.job_class(
            len(self.jobs),
            cmd,
            self._job_metadir,
            self._job_error_strategy == JobErrorStrategy.RETRY,
            self._job_num_retries
        )
        await simplug.hooks.on_job_init(self.scheduler, job)
        self.jobs.append(job)
        logger.info('/%s Pushing job: %r', self.name, job)
        await self.queue.put(job)
        job.status = JobStatus.QUEUED
        await simplug.hooks.on_job_queued(self.scheduler, job)

    async def run_until_complete(self):
        """Run until all jobs are complete, and then shutdown everything"""
        # don't do anything if the consumer task is canelled
        # leave it to the signal handler
        if not self.consumer.task.cancelled():
            halt_on_error = self._job_error_strategy == JobErrorStrategy.HALT
            while not self.consumer.task.done():
                if self._shutting_down:
                    break
                all_job_done = await self.scheduler.all_job_done(
                    halt_on_error=halt_on_error
                )
                if all_job_done:
                    break
                await asyncio.sleep(1)

        await self.consumer.complete()
        await simplug.hooks.on_complete(self.scheduler)

        logger.info('/%s Completed', self.name)
