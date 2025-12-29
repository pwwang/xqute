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
        "envs",
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
        # For cloud paths, this requires cloud client
        # self.metadir.mkdir(exist_ok=True, parents=True)
        # Let Scheduler.create_job handle metadir creation

        # The name of the job, should be the unique id from the scheduler
        self.trial_count = 0

        self._jid: int | str | None = None
        self._status = JobStatus.INIT
        self._error_retry = error_retry
        self._num_retries = num_retries

    def __repr__(self) -> str:
        """repr of the job"""
        prefix = f"{self.__class__.__name__}-{self.index}"
        if not self._jid:
            return f"<{prefix}: ({self.cmd})>"
        return f"<{prefix}({self._jid}): ({self.cmd})>"

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

    async def get_jid(self) -> int | str | None:
        """Get the jid of the job in scheduler system"""
        if self._jid is None and not await self.jid_file.a_is_file():
            return None
        if self._jid is not None:
            return self._jid
        self._jid = await self.jid_file.a_read_text()
        return self._jid

    async def set_jid(self, uniqid: int | str):
        self._jid = uniqid
        await self.jid_file.a_write_text(str(uniqid))

    async def get_status(self, refresh: bool = False) -> int:
        """Query the status of the job

        If the job is submitted, try to query it from the status file
        Make sure the status is updated by trap in wrapped script

        Uses caching to avoid excessive file I/O. Cache is invalidated
        when status is explicitly set.

        Args:
            refresh: Whether to refresh the status from file
        """
        if not refresh:
            return self._status

        if await self.status_file.a_is_file() and self._status in (
            JobStatus.SUBMITTED,
            JobStatus.RUNNING,
            JobStatus.KILLING,
        ):
            try:
                status_text = await self.status_file.a_read_text()
                self._status = int(status_text)
            except (
                FileNotFoundError,
                ValueError,
                TypeError,
            ):  # pragma: no cover
                pass

        # if (
        #     self._status == JobStatus.FAILED
        #     and self._error_retry
        #     and self.trial_count < self._num_retries  # type: ignore
        # ):
        #     self._status = JobStatus.RETRYING

        # Don't log here - let scheduler handle transition logging
        return self._status

    async def set_status(self, stat: int, flush: bool = True) -> None:
        """Set the status manually

        Args:
            stat: The status to set
            flush: Whether to flush the status to file
        """
        # Only log if status is actually changing
        prev_status = self._status

        if stat != prev_status:
            logger.info(
                "/Job-%s Status changed: %r -> %r",
                self.index,
                *JobStatus.get_name(prev_status, stat),
            )
            self._status = stat
            if flush:
                await self.status_file.a_write_text(str(stat))

    async def get_rc(self) -> int:
        """The return code of the job"""
        if not await self.rc_file.a_is_file():
            return -9
        return int(await self.rc_file.a_read_text())

    async def set_rc(self, rc: int | str) -> None:
        """Set the return code of the job

        Args:
            rc: The return code
        """
        await self.rc_file.a_write_text(str(rc))

    async def clean(self, retry: bool = False) -> None:
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
            if await retry_dir.a_exists():
                await retry_dir.a_rmtree()
            await retry_dir.a_mkdir(parents=True)

            for file in files_to_clean:
                if await file.a_is_file():
                    await file.a_rename(retry_dir / file.name)
        else:
            for file in files_to_clean:
                if await file.a_is_file():
                    await file.a_unlink()
