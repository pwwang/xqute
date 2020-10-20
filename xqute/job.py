"""Job to execute"""
from os import PathLike, unlink
import shlex
import shutil
from typing import Any, List
from pathlib import Path
from abc import ABC, abstractmethod
from .defaults import (
    DEFAULT_JOB_METADIR,
    DEFAULT_JOB_NUM_RETRIES,
    DEFAULT_JOB_CMD_WRAPPER_TEMPLATE,
    DEFAULT_JOB_CMD_WRAPPER_SHELL,
)
from .utils import (
    a_mkdir, logger,
    JobStatus,
    a_read_text,
    asyncify
)

# pylint: disable=invalid-name
a_shutil_move = asyncify(shutil.move)
a_shutil_rmtree = asyncify(shutil.rmtree)
a_os_unlink = asyncify(unlink)
# pylint: enable=invalid-name

class Job(ABC):
    """The abstract class for job

    Attributes:
        CMD_WRAPPER_TEMPLATE: The template for job wrapping
        CMD_WRAPPER_SHELL: The shell to run the wrapped script

        cmd: The command
        index: The index of the job
        metadir: The metadir of the job
        uid: The uid of the job in scheduler system
        trial_count: The count for re-tries
        hook_done: Mark whether hooks have already been. Since we don't have
            a trigger for job finished/failed, so we do a polling on it. This
            is to avoid calling the hooks repeatedly
        _status: The status of the job
        _rc: The return code of the job
        _error_retry: Whether we should retry if error happened
        _num_retries: Total number of retries
        _wrapped_cmd: The wrapped cmd, used for job submission

    Args:
        index: The index of the job
        cmd: The command of the job
        metadir: The meta directory of the Job
        error_retry: Whether we should retry if error happened
        num_retries: Total number of retries
    """
    CMD_WRAPPER_TEMPLATE: str = DEFAULT_JOB_CMD_WRAPPER_TEMPLATE
    CMD_WRAPPER_SHELL: str = DEFAULT_JOB_CMD_WRAPPER_SHELL

    def __init__(self,
                 index: int,
                 cmd: List[str],
                 metadir: PathLike = DEFAULT_JOB_METADIR,
                 error_retry: bool = False,
                 num_retries: int = DEFAULT_JOB_NUM_RETRIES):
        """Construct"""
        self.cmd = cmd
        self.index = index
        self.metadir = Path(metadir) / str(self.index)
        self.metadir.mkdir(exist_ok=True, parents=True)

        # The name of the job, should be the unique id from the scheduler
        self.uid = None
        self.trial_count = 0
        self.hook_done = False

        self._status = JobStatus.INIT
        self._rc = -1
        self._error_retry = error_retry
        self._num_retries = num_retries
        self._wrapped_cmd = None

    def __repr__(self) -> str:
        """repr of the job"""
        if not self.uid:
            return f'<{self.__class__.__name__}-{self.index}: ({self.cmd})>'
        return (f'<{self.__class__.__name__}-{self.index}({self.uid}): '
                f'({self.cmd})>')


    @property
    def stdout_file(self) -> Path:
        """The stdout file of the job"""
        return self.metadir / 'job.stdout'

    @property
    def stderr_file(self) -> Path:
        """The stderr file of the job"""
        return self.metadir / 'job.stderr'

    @property
    def status_file(self) -> Path:
        """The status file of the job"""
        return self.metadir / 'job.status'

    @property
    def rc_file(self) -> Path:
        """The rc file of the job"""
        return self.metadir / 'job.rc'

    @property
    def lock_file(self) -> Path:
        """The lock file of the job"""
        return self.metadir / 'job.lock'

    @property
    def retry_dir(self) -> Path:
        """The retry directory of the job"""
        return self.metadir / 'job.retry'

    @property
    async def status(self) -> int:
        """Query the status of the job

        If the job is submitted, try to query it from the status file
        Make sure the status is updated by trap in wrapped script
        """
        prev_status = self._status
        if self.status_file.is_file() and self._status in (
                JobStatus.SUBMITTED,
                JobStatus.RUNNING,
                JobStatus.KILLING
        ):
            try:
                self._status = int(await a_read_text(self.status_file))
            except (FileNotFoundError,
                    ValueError,
                    TypeError): # pragma: no cover
                pass

        if (self._status == JobStatus.FAILED and
                self._error_retry and
                self.trial_count < self._num_retries):
            self._status = JobStatus.RETRYING

        if self._status != prev_status and (
                self._status == JobStatus.RETRYING or
                self._status >= JobStatus.KILLING
        ):
            logger.info('/Job-%s Status changed: %r -> %r',
                        self.index,
                        *JobStatus.get_name(prev_status, self._status))
        return self._status

    @status.setter
    def status(self, stat: int):
        """Set the status manually

        Args:
            stat: The status to set
        """
        logger.debug('/Job-%s Status changed: %r -> %r',
                     self.index,
                     *JobStatus.get_name(self._status, stat))
        self._status = stat

    @property
    async def rc(self) -> int:
        """The return code of the job"""
        if not self.rc_file.is_file():
            return self._rc # pragma: no cover
        return int(await a_read_text(self.rc_file))

    @property
    def strcmd(self) -> str:
        """Get the string representation of the command"""
        return ' '.join(shlex.quote(str(cmditem)) for cmditem in self.cmd)

    @property
    def wrapped_cmd(self) -> Any:
        """Get the wrapped command"""
        return self._wrapped_cmd

    async def clean(self, retry=False):
        """Clean up the meta files

        Args:
            retry: Whether clean it for retrying
        """
        self.hook_done = False
        if retry:
            retry_dir = self.retry_dir / str(self.trial_count)
            if retry_dir.exists():
                await a_shutil_rmtree(retry_dir)
            await a_mkdir(retry_dir, parents=True)

            if self.stdout_file.is_file():
                shutil.move(str(self.stdout_file), str(retry_dir))
            if self.stderr_file.is_file():
                shutil.move(str(self.stderr_file), str(retry_dir))
            if self.status_file.is_file():
                shutil.move(str(self.status_file), str(retry_dir))
            if self.rc_file.is_file():
                shutil.move(str(self.rc_file), str(retry_dir))
        else:
            if self.stdout_file.is_file():
                unlink(self.stdout_file)
            if self.stderr_file.is_file():
                unlink(self.stderr_file)
            if self.status_file.is_file():
                unlink(self.status_file)
            if self.rc_file.is_file():
                unlink(self.rc_file)

    def wrapped_script(self, scheduler: "Scheduler") -> PathLike:
        """Get the wrapped script

        Args:
            scheduler: The scheduler

        Returns:
            The path of the wrapped script
        """
        return self.metadir / f'job.wrapped.{scheduler.name}'

    def name(self, scheduler: "Scheduler") -> str:
        """Get the name of job according to the scheduler

        Args:
            scheduler: The scheduler

        Returns:
            The name of the job
        """
        return f"{scheduler.name}.job.{self.index}"

    @abstractmethod
    async def wrap_cmd(self, scheduler: "Scheduler") -> None:
        """Wrap the command for the scheduler to submit and run

        Args:
            scheduler: The scheduler
        """
