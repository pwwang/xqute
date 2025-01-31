"""Job to execute"""

from __future__ import annotations

import shlex
from abc import ABC
from typing import TYPE_CHECKING, Tuple

from cloudpathlib import AnyPath

from .defaults import (
    JobStatus,
    JOBCMD_WRAPPER_LANG,
    JOBCMD_WRAPPER_TEMPLATE,
)
from .plugin import plugin
from .utils import logger, rmtree, PathType, CommandType

if TYPE_CHECKING:  # pragma: no cover
    from .scheduler import Scheduler


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
        "remote_metadir",
    )

    def __init__(
        self,
        index: int,
        cmd: CommandType,
        workdir: PathType,
        error_retry: bool | None = None,
        num_retries: int | None = None,
        remote_workdir: PathType | None = None,
    ):
        """Construct a new Job

        Args:
            index: The index of the job
            cmd: The command of the job
            metadir: The meta directory of the Job
            error_retry: Whether we should retry if error happened
            num_retries: Total number of retries
        """
        self.cmd: Tuple[str] = tuple(
            map(
                str,
                (cmd if isinstance(cmd, (tuple, list)) else shlex.split(cmd)),
            )
        )
        self.index = index
        self.metadir = AnyPath(workdir) / str(self.index)
        self.metadir.mkdir(exist_ok=True, parents=True)
        # In case the job is running on a remote system (e.g. cloud)
        remote_workdir = remote_workdir or workdir
        self.remote_metadir = AnyPath(remote_workdir) / str(self.index)

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
        prefix = f"{self.__class__.__name__}-{self.index}"
        if not self.jid:
            return f"<{prefix}: ({self.cmd})>"
        return f"<{prefix}({self.jid}): ({self.cmd})>"

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
    def stdout_file(self) -> PathType:
        """The stdout file of the job"""
        return self.metadir / "job.stdout"

    @property
    def remote_stdout_file(self) -> PathType:
        """The remote stdout file of the job"""
        return self.remote_metadir / "job.stdout"

    @property
    def stderr_file(self) -> PathType:
        """The stderr file of the job"""
        return self.metadir / "job.stderr"

    @property
    def remote_stderr_file(self) -> PathType:
        """The stderr file of the job"""
        return self.remote_metadir / "job.stderr"

    @property
    def status_file(self) -> PathType:
        """The status file of the job"""
        return self.metadir / "job.status"

    @property
    def remote_status_file(self) -> PathType:
        """The remote status file of the job"""
        return self.remote_metadir / "job.status"

    @property
    def rc_file(self) -> PathType:
        """The rc file of the job"""
        return self.metadir / "job.rc"

    @property
    def remote_rc_file(self) -> PathType:
        """The remote rc file of the job"""
        return self.remote_metadir / "job.rc"

    @property
    def jid_file(self) -> PathType:
        """The jid file of the job"""
        return self.metadir / "job.jid"

    @property
    def remote_jid_file(self) -> PathType:
        """The remote jid file of the job"""
        return self.remote_metadir / "job.jid"

    @property
    def retry_dir(self) -> PathType:
        """The retry directory of the job"""
        return self.metadir / "job.retry"

    @property
    def remote_retry_dir(self) -> PathType:
        """The remote retry directory of the job"""
        return self.remote_metadir / "job.retry"

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
            self._status == JobStatus.RETRYING or self._status >= JobStatus.KILLING
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
    def rc(self) -> int:
        """The return code of the job"""
        if not self.rc_file.is_file():
            return self._rc  # pragma: no cover
        return int(self.rc_file.read_text())

    def clean(self, retry=False):
        """Clean up the meta files

        Args:
            retry: Whether clean it for retrying
        """
        files_to_clean = [
            self.stdout_file,
            self.stderr_file,
            self.status_file,
            self.rc_file,
        ]

        if retry:
            retry_dir = self.retry_dir / str(self.trial_count)
            if retry_dir.exists():
                rmtree(retry_dir)
            retry_dir.mkdir(parents=True)

            for file in files_to_clean:
                if file.is_file():
                    file.rename(retry_dir / file.name)
        else:
            for file in files_to_clean:
                if file.is_file():
                    file.unlink()

    def wrap_script(self, scheduler: Scheduler) -> str:
        """Wrap the script with the template

        Args:
            scheduler: The scheduler

        Returns:
            The wrapped script
        """
        jobcmd_init = plugin.hooks.on_jobcmd_init(scheduler, self)
        jobcmd_prep = plugin.hooks.on_jobcmd_prep(scheduler, self)
        jobcmd_end = plugin.hooks.on_jobcmd_end(scheduler, self)
        return JOBCMD_WRAPPER_TEMPLATE.format(
            shebang=JOBCMD_WRAPPER_LANG,
            status=JobStatus,
            job=self,
            jobcmd_init="\n".join(jobcmd_init),
            jobcmd_prep="\n".join(jobcmd_prep),
            jobcmd_end="\n".join(jobcmd_end),
            cmd=shlex.join(self.cmd),
            prescript=scheduler.prescript,
            postscript=scheduler.postscript,
            keep_jid_file=False,
        )

    def wrapped_script(self, scheduler: Scheduler, remote: bool = False) -> PathType:
        """Get the wrapped script

        Args:
            scheduler: The scheduler

        Returns:
            The path of the wrapped script
        """
        base = f"job.wrapped.{scheduler.name}"
        wrapt_script = self.metadir / base
        wrapt_script.write_text(self.wrap_script(scheduler))

        return (self.remote_metadir / base) if remote else wrapt_script
