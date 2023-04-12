"""The scheduler to run jobs on SSH"""
from __future__ import annotations

import asyncio
from typing import Any, List, Type
from threading import Lock

from ..scheduler import Scheduler
from ..utils import a_read_text
from ..job import TYPE_CHECKING, Job
from .local_scheduler import LocalJob

if TYPE_CHECKING:  # pragma: no cover
    from asyncio.subprocess import Process
    from typing import Coroutine


class SshJob(LocalJob):
    """SSH job"""


class SshScheduler(Scheduler):
    """The ssh scheduler

    Attributes:
        name: The name of the scheduler
        job_class: The job class

    Args:
        ssh_*: SSH options
        ... other Scheduler args
    """
    _CONN_LOCK = Lock()

    name: str = "ssh"
    job_class: Type[Job] = SshJob

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ssh = self.config.get('ssh', 'ssh')
        self.servers = []

    def _create_proc(
        self,
        server: str,
        cmd: str | List[str],
    ) -> Coroutine[Any, Any, Process]:
        opts = self.config['ssh_servers'][server]
        options = []
        for key, val in opts.items():
            if isinstance(val, list):
                for v in val:
                    options.append(f'-{key}')
                    options.append(str(v))
            else:
                options.append(f'-{key}')
                options.append(str(val))

        cmd = cmd if isinstance(cmd, list) else [cmd]
        return asyncio.create_subprocess_exec(
            self.ssh, *options, server, *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

    async def connect(self):
        with self._CONN_LOCK:
            for server in self.config.get('ssh_servers', {}):
                proc = await self._create_proc(server, 'true')
                server_alive = False
                try:
                    await asyncio.wait_for(
                        proc.wait(),
                        self.config.get('ssh_conn_timeout', 3),
                    )
                except asyncio.TimeoutError:  # pragma: no cover
                    pass
                else:
                    server_alive = proc.returncode == 0

                if server_alive:
                    self.servers.append(server)

            if not self.servers:
                raise RuntimeError('No available SSH servers')

    async def submit_job(self, job: Job) -> str:
        """Submit a job to SSH

        Args:
            job: The job

        Returns:
            The job id
        """
        if not self.servers:
            await self.connect()

        svr_id = job.index % len(self.servers)
        server = self.servers[svr_id]
        proc = await self._create_proc(
            server,
            str(await job.wrapped_script(self)),
        )
        return f"{server}-{proc.pid}"

    async def kill_job(self, job: Job):
        """Kill a job on SSH

        Args:
            job: The job
        """
        try:
            server, _, pid = str(job.jid).rpartition('-')
            proc = await self._create_proc(server, ['kill', '-9', f'-{pid}'])
            await proc.wait()
        except Exception:  # pragma: no cover
            raise

    async def job_is_running(self, job: Job) -> bool:
        """Tell if a job is really running, not only the job.jid_file

        In case where the jid file is not cleaned when job is done.

        Args:
            job: The job

        Returns:
            True if it is, otherwise False
        """
        try:
            jid = await a_read_text(job.jid_file)
        except FileNotFoundError:
            return False

        if not jid:
            return False

        server, _, pid = jid.rpartition('-')
        if server not in self.servers:
            return False

        proc = await self._create_proc(server, ['kill', '-0', pid])
        try:
            await proc.wait()
        except Exception:  # pragma: no cover
            return False
        else:
            return proc.returncode == 0
