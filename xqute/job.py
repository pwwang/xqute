"""Job to execute"""

from __future__ import annotations

import shlex
from typing import Any, Tuple

from .defaults import JobStatus
from .utils import logger, CommandType
from .path import SpecPath


class Job:
    """The class for job

    Attributes:

        cmd: The command
        index: The index of the job
        metadir: The metadir of the job
        jid: The jid of the job in scheduler system
        trial_count: The count for re-tries
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
        "envs",
        "_status_cache_valid",
    )

    def __init__(
        self,
        index: int,
        cmd: CommandType,
        workdir: SpecPath,
        error_retry: bool | None = None,
        num_retries: int | None = None,
        envs: dict[str, Any] | None = None,
    ):
        """Construct a new Job

        Args:
            index: The index of the job
            cmd: The command of the job
            metadir: The meta directory of the Job
            error_retry: Whether we should retry if error happened
            num_retries: Total number of retries
        """
        self.cmd: Tuple[str, ...] = tuple(
            map(
                str,
                (cmd if isinstance(cmd, (tuple, list)) else shlex.split(cmd)),
            )
        )
        self.index = index
        self.envs = envs or {}
        self.envs["XQUTE_JOB_INDEX"] = str(self.index)
        self.envs["XQUTE_METADIR"] = str(workdir)
        self.metadir = workdir / str(self.index)  # type: ignore
        self.envs["XQUTE_JOB_METADIR"] = str(self.metadir)
        self.metadir.mkdir(exist_ok=True, parents=True)

        # The name of the job, should be the unique id from the scheduler
        self.trial_count = 0
        self.prev_status = JobStatus.INIT

        self._jid: int | str | None = None
        self._status = JobStatus.INIT
        self._rc = -1
        self._error_retry = error_retry
        self._num_retries = num_retries
        self._status_cache_valid = True  # Status is valid initially

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
    def stdout_file(self) -> SpecPath:
        """The stdout file of the job"""
        return self.metadir / "job.stdout"

    @property
    def stderr_file(self) -> SpecPath:
        """The stderr file of the job"""
        return self.metadir / "job.stderr"

    @property
    def status_file(self) -> SpecPath:
        """The status file of the job"""
        return self.metadir / "job.status"

    @property
    def rc_file(self) -> SpecPath:
        """The rc file of the job"""
        return self.metadir / "job.rc"

    @property
    def jid_file(self) -> SpecPath:
        """The jid file of the job"""
        return self.metadir / "job.jid"

    @property
    def retry_dir(self) -> SpecPath:
        """The retry directory of the job"""
        return self.metadir / "job.retry"

    @property
    def status(self) -> int:
        """Query the status of the job

        If the job is submitted, try to query it from the status file
        Make sure the status is updated by trap in wrapped script

        Uses caching to avoid excessive file I/O. Cache is invalidated
        when status is explicitly set.
        """
        # Only read from file if cache is invalid and job is in pollable state
        status_changed_from_file = False
        if (
            not self._status_cache_valid
            and self.status_file.is_file()
            and self._status in (
                JobStatus.SUBMITTED,
                JobStatus.RUNNING,
                JobStatus.KILLING,
            )
        ):
            try:
                new_status_from_file = int(self.status_file.read_text())
                # Update prev_status before changing _status if status changed
                if new_status_from_file != self._status:
                    self.prev_status = self._status
                    self._status = new_status_from_file
                    status_changed_from_file = True
                else:
                    self._status = new_status_from_file
                self._status_cache_valid = True
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

        if status_changed_from_file and (
            self._status == JobStatus.RETRYING or self._status >= JobStatus.RUNNING
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
        # Only log if status is actually changing
        if self._status != stat:
            logger.debug(
                "/Job-%s Status changed: %r -> %r",
                self.index,
                *JobStatus.get_name(self._status, stat),
            )
            self.prev_status = self._status
            self._status = stat
        else:
            # Even if status doesn't change, update prev_status to acknowledge
            # this status and prevent duplicate hook calls in the scheduler
            self.prev_status = stat
        # Always invalidate cache when status is explicitly set
        self._status_cache_valid = False

    def refresh_status(self) -> int:
        """Force refresh status from file system

        This invalidates the cache and reads the current status.
        Use this in polling loops to get the latest status.

        Returns:
            The current job status
        """
        self._status_cache_valid = False
        return self.status

    async def refresh_status_async(self) -> int:
        """Async version of refresh_status for batch operations

        This invalidates the cache and reads the current status asynchronously.
        Use this in polling loops with asyncio.gather() for parallel reads.

        Returns:
            The current job status
        """
        import asyncio

        self._status_cache_valid = False
        # Read status file asynchronously using run_in_executor
        loop = asyncio.get_running_loop()

        # Don't update prev_status here - let the polling logic handle transitions
        if (
            self.status_file.is_file()
            and self._status in (
                JobStatus.SUBMITTED,
                JobStatus.RUNNING,
                JobStatus.KILLING,
            )
        ):
            try:
                # Run blocking I/O in executor for true async operation
                status_text = await loop.run_in_executor(
                    None, self.status_file.read_text
                )
                self._status = int(status_text)
                self._status_cache_valid = True
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

        # Don't log here - let scheduler handle transition logging
        return self._status

    @property
    def rc(self) -> int:
        """The return code of the job"""
        if not self.rc_file.is_file():
            return self._rc  # pragma: no cover
        return int(self.rc_file.read_text())

    def clean(self, retry: bool = False) -> None:
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
            retry_dir = self.retry_dir / str(self.trial_count)  # type: ignore
            if retry_dir.exists():
                retry_dir.rmtree()
            retry_dir.mkdir(parents=True)

            for file in files_to_clean:
                if file.is_file():
                    file.rename(retry_dir / file.name)
        else:
            for file in files_to_clean:
                if file.is_file():
                    file.unlink()
