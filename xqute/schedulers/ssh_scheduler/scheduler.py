"""The scheduler to run jobs on SSH"""
from __future__ import annotations

from typing import Mapping, Type

from ...scheduler import Scheduler
from ...utils import a_read_text
from ...job import Job
from ..local_scheduler import LocalJob

from .client import SSHClient


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
    name: str = "ssh"
    job_class: Type[Job] = SshJob

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.servers: Mapping[str, SSHClient] = {}
        ssh = self.config.get('ssh', 'ssh')
        for key, val in self.config.get('ssh_servers', {}).items():
            client = SSHClient(ssh, key, **val)
            self.servers[client.name] = client

        if not self.servers:
            raise ValueError(
                "No ssh_servers defined in config, "
                "please define at least one server",
            )

    async def submit_job(self, job: Job) -> str:
        """Submit a job to SSH

        Args:
            job: The job

        Returns:
            The job id
        """
        server = list(self.servers.values())[job.index % len(self.servers)]
        await server.connect()

        return await server.submit(
            job.CMD_WRAPPER_SHELL,
            await job.wrapped_script(self),
        )

    async def kill_job(self, job: Job):
        """Kill a job on SSH

        Args:
            job: The job
        """
        try:
            server, pid = str(job.jid).split('/')
            await self.servers[server].kill(pid)
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

        server, pid = jid.split('/')
        if server not in self.servers:
            return False

        try:
            return await self.servers[server].is_running(pid)
        except Exception:  # pragma: no cover
            return False
