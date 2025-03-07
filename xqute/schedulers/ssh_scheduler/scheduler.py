"""The scheduler to run jobs on SSH"""

from __future__ import annotations

import asyncio
import shlex

from yunpath import CloudPath

from ...scheduler import Scheduler
from ...job import Job

from .client import SSHClient


class SshScheduler(Scheduler):
    """The ssh scheduler

    Attributes:
        name: The name of the scheduler
        job_class: The job class

    Args:
        ...: other Scheduler args
    """

    name: str = "ssh"

    __slots__ = Scheduler.__slots__ + ("ssh", "servers")

    def __init__(self, *args, **kwargs):
        self.ssh = kwargs.pop("ssh", "ssh")
        ssh_servers = kwargs.pop("servers", {})
        super().__init__(*args, **kwargs)
        self.servers = {}
        if isinstance(ssh_servers, (tuple, list)):
            ssh_servers = {server: {} for server in ssh_servers}
        for key, val in ssh_servers.items():
            client = SSHClient(self.ssh, key, **val)
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

        rc, stdout, stderr = await server.submit(
            *shlex.split(self.jobcmd_shebang(job)),
            self.wrapped_job_script(job).fspath,
        )
        if rc != 0:
            # job.stdout_file.write_bytes(stdout)
            # job.stderr_file.write_bytes(stderr)
            raise RuntimeError(f"Failed to submit job #{job.index}: {stderr.decode()}")
        try:
            pid, srvname = stdout.decode().split("@", 1)
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
            while not job.stdout_file.exists() and not job.stderr_file.exists():
                if not await self.servers[srvname].is_running(pid):  # pragma: no cover
                    # job.stdout_file.write_bytes(stdout)
                    # job.stderr_file.write_bytes(stderr)

                    raise RuntimeError(
                        f"Failed to submit job #{job.index}: {stderr.decode()}"
                    )

                if isinstance(job.stdout_file, CloudPath):  # pragma: no cover
                    await asyncio.sleep(2)
                else:  # pragma: no cover
                    await asyncio.sleep(0.1)

        return stdout.decode()

    async def kill_job(self, job: Job):
        """Kill a job on SSH

        Args:
            job: The job
        """
        try:
            pid, server = str(job.jid).split("@", 1)
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
            jid = job.jid_file.read_text().strip()
        except FileNotFoundError:
            return False

        if not jid:
            return False

        try:
            pid, server = jid.split("@", 1)
        except (ValueError, TypeError):  # pragma: no cover
            # Can be the jid file by a different scheduler from previous runs
            return False

        if server not in self.servers:
            return False

        try:
            return await self.servers[server].is_running(pid)
        except Exception:  # pragma: no cover
            return False
