"""Job to execute"""
from __future__ import annotations

import shlex
import shutil
from abc import ABC, abstractmethod
from os import PathLike, unlink
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, List, Optional

from aiopath import AsyncPath  # type: ignore

from .defaults import (
    DEFAULT_JOB_METADIR,
    DEFAULT_JOB_CMD_WRAPPER_TEMPLATE,
    DEFAULT_JOB_CMD_WRAPPER_SHELL,
    JobStatus,
)
from .utils import logger, a_mkdir, a_read_text, a_write_text, asyncify

if TYPE_CHECKING:  # pragma: no cover
    from .scheduler import Scheduler


a_shutil_move = asyncify(shutil.move)
a_os_unlink = asyncify(unlink)


class Job(ABC):
    """The abstract class for job

    Attributes:
        CMD_WRAPPER_TEMPLATE: The template for job wrapping
        CMD_WRAPPER_SHELL: The shell to run the wrapped script

        cmd: The command
        index: The index of the job
        metadir: The metadir of the job
        jid: The jid of the job in scheduler system
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

    __slots__ = (
        "cmd",
        "index",
        "metadir",
        "trial_count",
        "_jid",
        "_status",
        "_rc",
        "_error_retry",
        "_num_retries",
        "prev_status",
    )

    CMD_WRAPPER_TEMPLATE: ClassVar[str] = DEFAULT_JOB_CMD_WRAPPER_TEMPLATE
    CMD_WRAPPER_SHELL: ClassVar[str] = DEFAULT_JOB_CMD_WRAPPER_SHELL

    def __init__(
        self,
        index: int,
        cmd: str | List[str],
        metadir: PathLike = DEFAULT_JOB_METADIR,
        error_retry: Optional[bool] = None,
        num_retries: Optional[int] = None,
    ):
        """Construct"""
        self.cmd = cmd
        self.index = index
        self.metadir = Path(metadir) / str(self.index)
        self.metadir.mkdir(exist_ok=True, parents=True)

        # The name of the job, should be the unique id from the scheduler
        self.trial_count = 0
        self.prev_status = JobStatus.INIT

        self._jid: int | str | None = None
        self._status = JobStatus.INIT
        self._rc = -1
        self._error_retry = error_retry
        self._num_retries = num_retries

    def __repr__(self) -> str:
        """repr of the job"""
        if not self.jid:
            return f"<{self.__class__.__name__}-{self.index}: ({self.cmd})>"
        return (
            f"<{self.__class__.__name__}-{self.index}({self.jid}): "
            f"({self.cmd})>"
        )

    @property
    def jid(self) -> int | str | None:
        """Get the jid of the job in scheduler system"""
        if self._jid is None and not self.jid_file.is_file():
            return None
        if self._jid is not None:
            return self._jid
        self._jid = self.jid_file.read_text()
        return self._jid

    @jid.setter
    def jid(self, uniqid: int | str):
        self._jid = uniqid
        self.jid_file.write_text(str(uniqid))

    @property
    def stdout_file(self) -> Path:
        """The stdout file of the job"""
        return self.metadir / "job.stdout"

    @property
    def stderr_file(self) -> Path:
        """The stderr file of the job"""
        return self.metadir / "job.stderr"

    @property
    def status_file(self) -> Path:
        """The status file of the job"""
        return self.metadir / "job.status"

    @property
    def rc_file(self) -> Path:
        """The rc file of the job"""
        return self.metadir / "job.rc"

    @property
    def jid_file(self) -> Path:
        """The jid file of the job"""
        return self.metadir / "job.jid"

    @property
    def retry_dir(self) -> Path:
        """The retry directory of the job"""
        return self.metadir / "job.retry"

    @property
    def status(self) -> int:
        """Query the status of the job

        If the job is submitted, try to query it from the status file
        Make sure the status is updated by trap in wrapped script
        """
        self.prev_status = self._status
        if self.status_file.is_file() and self._status in (
            JobStatus.SUBMITTED,
            JobStatus.RUNNING,
            JobStatus.KILLING,
        ):
            try:
                self._status = int(self.status_file.read_text())
            except (
                FileNotFoundError,
                ValueError,
                TypeError,
            ):  # pragma: no cover
                pass

        if (
            self._status == JobStatus.FAILED
            and self._error_retry
            and self.trial_count < self._num_retries  # type: ignore
        ):
            self._status = JobStatus.RETRYING

        if self.prev_status != self._status and (
            self._status == JobStatus.RETRYING
            or self._status >= JobStatus.KILLING
        ):
            logger.info(
                "/Job-%s Status changed: %r -> %r",
                self.index,
                *JobStatus.get_name(self.prev_status, self._status),
            )

        return self._status

    @status.setter
    def status(self, stat: int):
        """Set the status manually

        Args:
            stat: The status to set
        """
        logger.debug(
            "/Job-%s Status changed: %r -> %r",
            self.index,
            *JobStatus.get_name(self._status, stat),
        )
        self.prev_status = self._status
        self._status = stat

    @property
    async def rc(self) -> int:
        """The return code of the job"""
        if not await AsyncPath(self.rc_file).is_file():
            return self._rc  # pragma: no cover
        return int(await a_read_text(self.rc_file))

    @property
    def strcmd(self) -> str:
        """Get the string representation of the command"""
        if isinstance(self.cmd, list):
            return " ".join(shlex.quote(str(cmditem)) for cmditem in self.cmd)
        return self.cmd

    async def clean(self, retry=False):
        """Clean up the meta files

        Args:
            retry: Whether clean it for retrying
        """
        if retry:
            retry_dir = self.retry_dir / str(self.trial_count)
            if await AsyncPath(retry_dir).exists():
                shutil.rmtree(retry_dir)
            await a_mkdir(retry_dir, parents=True)

            if await AsyncPath(self.stdout_file).is_file():
                shutil.move(str(self.stdout_file), str(retry_dir))
            if await AsyncPath(self.stderr_file).is_file():
                shutil.move(str(self.stderr_file), str(retry_dir))
            if await AsyncPath(self.status_file).is_file():
                shutil.move(str(self.status_file), str(retry_dir))
            if await AsyncPath(self.rc_file).is_file():
                shutil.move(str(self.rc_file), str(retry_dir))
        else:
            if await AsyncPath(self.stdout_file).is_file():
                unlink(self.stdout_file)
            if await AsyncPath(self.stderr_file).is_file():
                unlink(self.stderr_file)
            if await AsyncPath(self.status_file).is_file():
                unlink(self.status_file)
            if await AsyncPath(self.rc_file).is_file():
                unlink(self.rc_file)

    async def wrapped_script(self, scheduler: Scheduler) -> PathLike:
        """Get the wrapped script

        Args:
            scheduler: The scheduler

        Returns:
            The path of the wrapped script
        """
        wrapt_script = self.metadir / f"job.wrapped.{scheduler.name}"
        wrapt_cmd = self.wrap_cmd(scheduler)
        if (
            not await AsyncPath(wrapt_script).is_file()
            or await a_read_text(wrapt_script) != wrapt_cmd
        ):
            await a_write_text(wrapt_script, self.wrap_cmd(scheduler))
        return wrapt_script

    @abstractmethod
    def wrap_cmd(self, scheduler: Scheduler) -> str:
        """Wrap the command for the scheduler to submit and run

        Args:
            scheduler: The scheduler
        """
