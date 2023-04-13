from __future__ import annotations

import os
import sys
import asyncio
from pathlib import Path
from tempfile import gettempdir
from typing import Any


class SSHClient:
    def __init__(
        self,
        ssh: str,
        server: str,
        port: int | None = None,
        user: str | None = None,
        keyfile: str | None = None,
        ctrl_persist: int = 600,  # seconds
        ctrl_dir: str | Path = gettempdir()
    ):
        self.ssh = ssh
        self.server = server
        self.port = port
        self.user = user
        self.keyfile = keyfile
        self.ctrl_persist = ctrl_persist
        port = port or 22
        if user:
            self.name = f"{user}@{server}:{port}"
        else:
            self.name = f"{server}:{port}"
        self.ctrl_file = Path(ctrl_dir) / f'ssh-{self.name}.sock'
        self._conn_lock = asyncio.Lock()

    @property
    def is_connected(self):
        return self.ctrl_file.exists()

    async def connect(self):
        if self.is_connected:
            return

        async with self._conn_lock:
            command = [
                self.ssh,
                '-o', 'ControlMaster=yes',
                '-o', f'ControlPath={self.ctrl_file}',
                '-o', f'ControlPersist={self.ctrl_persist}',
            ]
            if self.port:
                command.extend(['-p', str(self.port)])
            if self.keyfile:
                command.extend(['-i', str(self.keyfile)])
            if self.user:
                command.extend([f'{self.user}@{self.server}', 'true'])
            else:  # pragma: no cover
                command.extend([self.server, 'true'])

            proc = await asyncio.create_subprocess_exec(*command)
            await proc.wait()

            if proc.returncode != 0 or not self.is_connected:
                raise RuntimeError(
                    f'Failed to connect to SSH server: {self.server}'
                )

    async def disconnect(self):
        if self.is_connected:
            self.ctrl_file.unlink()

    async def create_proc(self, *cmds: Any):
        cmds = map(str, cmds)
        command = [
            self.ssh,
            '-o', f'ControlPath={self.ctrl_file}',
        ]
        if self.port:  # pragma: no cover
            command.extend(['-p', str(self.port)])
        if self.keyfile:
            command.extend(['-i', str(self.keyfile)])
        if self.user:
            command.extend([f'{self.user}@{self.server}', *cmds])
        else:  # pragma: no cover
            command.extend([self.server, *cmds])

        return await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

    async def run(self, *cmds: Any) -> int:
        proc = await self.create_proc(*cmds)
        await proc.wait()
        return proc.returncode

    async def submit(self, *cmds: str) -> str:
        """Submit a job to SSH, get the pid of the job on the remote server"""
        submitter = Path(__file__).parent.resolve() / 'submitter.py'

        proc = await self.create_proc(
            sys.executable,
            submitter,
            self.name,
            os.getcwd(),
            *cmds,
        )
        await proc.wait()
        return (await proc.stdout.read()).decode().strip()

    async def kill(self, pid: str):
        """Kill a job on SSH"""
        await self.run('kill', '-9', f"-{pid}")

    async def is_running(self, pid: str) -> bool:
        return await self.run('kill', '-0', pid) == 0
