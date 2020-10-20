"""The scheduler to run jobs locally"""
from subprocess import Popen
from typing import List, Type
import psutil
from ..job import Job
from ..scheduler import Scheduler
from ..utils import JobStatus, a_write_text, asyncify, a_read_text

@asyncify
def a_proc_children(proc: psutil.Process,
                    recursive: bool = False) -> List[psutil.Process]:
    """Get the children of a process asyncly

    Args:
        proc: The process
        recursive: Whether get the children recursively

    Returns:
        The children of the process
    """
    return proc.children(recursive=recursive)

@asyncify
def a_proc_kill(proc: psutil.Process):
    """Kill a process asynchronously

    Args:
        proc: The process

    Returns:
        The result from proc.kill()
    """
    return proc.kill()

class LocalJob(Job):
    """Local job"""

    async def wrap_cmd(self, scheduler: "Scheduler") -> None:
        """Wrap the command for the scheduler to submit and run

        Args:
            scheduler: The scheduler
        """
        wrapt_script = self.wrapped_script(scheduler)
        await a_write_text(wrapt_script, self.CMD_WRAPPER_TEMPLATE.format(
            shebang=f'#!{self.CMD_WRAPPER_SHELL}', job=self, status=JobStatus
        ))
        self._wrapped_cmd = [self.CMD_WRAPPER_SHELL, wrapt_script]

class LocalScheduler(Scheduler):
    """The local scheduler

    Attributes:
        name: The name of the scheduler
        job_class: The job class
    """
    name: str = 'local'
    job_class: Type[Job] = LocalJob

    async def submit_job(self, job: Job) -> int:
        """Submit a job locally

        Args:
            job: The job

        Returns:
            The process id
        """
        # run at background
        # not using asyncio.create_subprocess_exec, becaused the result
        # needs to be awaited
        # otherwise
        # RuntimeWarning: A loop is being detached from a child watcher
        #   with pending handlers
        proc = await asyncify(Popen)(job.wrapped_cmd)
        return proc.pid

    async def kill_job(self, job: Job):
        """Kill a job asynchronously

        Args:
            job: The job
        """
        try:
            proc = psutil.Process(int(job.uid))
            children = await a_proc_children(proc, recursive=True)
            for child in children:
                await a_proc_kill(child)
            await a_proc_kill(proc)
        except psutil.NoSuchProcess: # pragma: no cover
            # job has finished
            pass

    async def job_is_running(self, job: Job) -> bool:
        """Tell if a job is really running, not only the job.lock_file

        In case where the lockfile is not cleaned when job is done.

        Args:
            job: The job

        Returns:
            True if it is, otherwise False
        """
        try:
            uid = int(await a_read_text(job.lock_file))
        except (ValueError, TypeError, FileNotFoundError): # pragma: no cover
            return False

        return await asyncify(psutil.pid_exists)(uid)
