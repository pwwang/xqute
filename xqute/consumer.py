"""The consumer"""
import asyncio
from typing import Callable
from .utils import logger
from .plugin import simplug

class Consumer:
    """The consumer class

    Attributes:
        name: The name, used in logger
        queue: The job queue
        scheduler: The scheduler
        task: The async task

    Args:
        queue: The asyncio queue
        scheduler: The scheduler
    """

    name: str = 'Consumer'

    def __init__(self, queue: asyncio.Queue, scheduler: "Scheduler"):
        """Construct"""
        self.queue = queue
        self.scheduler = scheduler
        self.task = None

    def create_task(self, callback: Callable):
        """Create the task

        Args:
            callback: The callback when the task is done
        """
        self.task = asyncio.create_task(self.task_run())
        self.task.add_done_callback(callback)

    async def task_run(self):
        """Consumes the queue and submit the jobs"""
        logger.info('/%s Starting ...', self.name)
        await simplug.hooks.on_init(self.scheduler)
        while True:
            # if self.task.cancelled():
            #     break
            can_submit = await self.scheduler.can_submit()
            logger.debug('/%s Hit max forks of scheduler? %s',
                         self.name, not can_submit)
            if not can_submit:
                await asyncio.sleep(.1)
                continue
            job = await self.queue.get()
            if job is None:
                logger.debug('/%s Hit end of queue', self.name)
                self.queue.task_done()
                break
            await self.scheduler.submit_job_and_update_status(job)
            self.queue.task_done()

    async def complete(self):
        """Complete the task, put None to queue to notify"""
        await self.queue.put(None)
        try:
            await self.task
        except asyncio.CancelledError:
            logger.warning('/%s Cancelling consumer task ...', self.name)
