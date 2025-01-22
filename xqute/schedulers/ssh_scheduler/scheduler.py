"""The scheduler to run jobs on SSH"""
from __future__ import annotations

import asyncio
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

    def __init__(self, forks: int, prescript: str = "", postscript: str = "", **kwargs):
        super().__init__(forks, prescript, postscript, **kwargs)
        self.servers: Mapping[str, SSHClient] = {}
        ssh = self.config.get('ssh', 'ssh')
        ssh_servers = self.config.get('ssh_servers', {})
        if isinstance(ssh_servers, (tuple, list)):
            ssh_servers = {server: {} for server in ssh_servers}
        for key, val in ssh_servers.items():
            client = SSHClient(ssh, key, **val)
            self.servers[client.name] = client

        if not self.servers:
            raise ValueError(
                "No ssh_servers defined in config, "
                "please define at least one server",
            )

    def __del__(self):
        for server in self.servers.values():
            if server.is_connected:
                server.disconnect()

    async def submit_job(self, job: Job) -> str:
        """Submit a job to SSH

        Args:
            job: The job

        Returns:
            The job id
        """
        server = list(self.servers.values())[job.index % len(self.servers)]
        await server.connect()

        wrapped_script = str(await job.wrapped_script(self))
        rc, stdout, stderr = await server.submit(
            job.__class__.CMD_WRAPPER_SHELL,
            wrapped_script,
        )
        if rc != 0:
            job.stdout_file.write_bytes(stdout)
            job.stderr_file.write_bytes(stderr)
            raise RuntimeError(
                f"Failed to submit job #{job.index}: {stderr.decode()}"
            )
        try:
            pid, server = stdout.decode().split('@', 1)
        except (ValueError, TypeError):  # pragma: no cover
            raise RuntimeError(
                f"Failed to submit job #{job.index}: "
                f"expecting 'pid@server', got {stdout.decode()}"
            )
        else:
            # wait for a while to make sure the process is running
            # this is to avoid the real command is not run when proc is recycled
            # too early
            # this happens for python < 3.12
            while not job.stderr_file.exists() or not job.stdout_file.exists():
                if not await self.servers[server].is_running(pid):
                    job.stdout_file.write_bytes(stdout)
                    job.stderr_file.write_bytes(stderr)

                    raise RuntimeError(
                        f"Failed to submit job #{job.index}: {stderr.decode()}"
                    )
                await asyncio.sleep(0.1)  # pragma: no cover
        return stdout.decode()

    async def kill_job(self, job: Job):
        """Kill a job on SSH

        Args:
            job: The job
        """
        try:
            pid, server = str(job.jid).split('@', 1)
            await self.servers[server].kill(pid)
        except Exception:  # pragma: no cover
            pass

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

        pid, server = jid.split('@', 1)
        if server not in self.servers:
            return False

        try:
            return await self.servers[server].is_running(pid)
        except Exception:  # pragma: no cover
            return False
