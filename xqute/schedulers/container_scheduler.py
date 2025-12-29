"""The scheduler to run jobs via containers"""

from __future__ import annotations

import asyncio
import os
import shlex
import shutil
from pathlib import Path
from typing import List, Sequence

from ..job import Job
from ..path import SpecPath
from ..defaults import JOBCMD_WRAPPER_LANG
from .local_scheduler import LocalScheduler
from .gbatch_scheduler import NAMED_MOUNT_RE, DEFAULT_MOUNTED_ROOT

CONTAINER_TYPES = {
    "docker": "docker",
    "podman": "podman",
    "apptainer": "apptainer",
    "singularity": "apptainer",
}


class ContainerScheduler(LocalScheduler):
    """Scheduler to run jobs via containers (Docker/Podman/Apptainer)

    This scheduler can execute jobs inside containers using Docker, Podman,
    or Apptainer.

    Args:
        image: Container image to use for running jobs
        entrypoint: Entrypoint command for the container
        bin: Path to container runtime binary (e.g. /path/to/docker)
        volumes: host:container volume mapping string or strings
            or named volume mapping like `MOUNTED=/path/on/host`
            then it will be mounted to `/mnt/disks/MOUNTED` in the container.
            You can use environment variable `MOUNTED` in your job scripts to
            refer to the mounted path.
        user: User to run the container as (only for Docker/Podman)
            By default, it runs as the current user (os.getuid() and os.getgid())
        remove: Whether to remove the container after execution.
            Only applies to Docker/Podman.
        bin_args: Additional arguments to pass to the container runtime
        **kwargs: Additional arguments passed to parent Scheduler
    """

    name = "container"

    __slots__ = (
        "image",
        "entrypoint",
        "bin",
        "volumes",
        # "envs",
        "remove",
        "user",
        "bin_args",
        "_container_type",
        "_path_envs",
    )

    def __init__(
        self,
        image: str,
        entrypoint: str | List[str] = JOBCMD_WRAPPER_LANG,
        bin: str = "docker",
        volumes: str | Sequence[str] | None = None,
        # envs: Dict[str, str] | None = None,
        remove: bool = True,
        user: str | None = None,
        bin_args: List[str] | None = None,
        **kwargs,
    ):
        if "mount" in kwargs:
            raise ValueError(
                "You used 'mount' argument for container scheduler, "
                "did you mean 'volumes'?"
            )

        kwargs.setdefault("mounted_workdir", f"{DEFAULT_MOUNTED_ROOT}/xqute_workdir")
        super().__init__(**kwargs)

        self.bin = shutil.which(bin)
        if not self.bin:
            raise ValueError(f"Container runtime binary '{bin}' not found in PATH")

        self.image = image
        self.entrypoint = (
            list(entrypoint) if isinstance(entrypoint, (list, tuple)) else [entrypoint]
        )
        self._path_envs = {}
        self.volumes = volumes or []
        self.volumes = (
            [self.volumes] if isinstance(self.volumes, str) else list(self.volumes)
        )
        for i, vol in enumerate(self.volumes):
            if NAMED_MOUNT_RE.match(vol):
                name, host_path = vol.split("=", 1)
                host_path_obj = Path(host_path).expanduser().resolve()
                if not host_path_obj.exists():
                    raise FileNotFoundError(
                        f"Volume host path '{host_path}' does not exist"
                    )
                if host_path_obj.is_file():
                    host_path = str(host_path_obj.parent)
                    mount_path = (
                        f"{DEFAULT_MOUNTED_ROOT}/{name}/{host_path_obj.parent.name}"
                    )
                    self._path_envs[name] = f"{mount_path}/{host_path_obj.name}"
                    self.volumes[i] = f"{host_path}:{mount_path}"
                else:
                    host_path = str(host_path_obj)
                    mount_path = f"{DEFAULT_MOUNTED_ROOT}/{name}"
                    self._path_envs[name] = mount_path
                    self.volumes[i] = f"{host_path}:{mount_path}"

        # self.envs = envs or {}
        self.remove = remove
        self.user = user or f"{os.getuid()}:{os.getgid()}"
        self.bin_args = bin_args or []
        self.volumes.append(f"{self.workdir}:{self.workdir.mounted}")

        self._container_type = CONTAINER_TYPES.get(
            Path(self.bin).name.lower(),
            "docker",
        )
        if self._container_type in ("docker", "podman") and self.image.startswith(
            "docker://"
        ):
            # Convert docker://image to image name
            self.image = self.image[9:]

    async def wrapped_job_script(self, job: Job) -> SpecPath:
        """Get the wrapped job script

        Args:
            job: The job

        Returns:
            The path of the wrapped job script
        """
        base = f"job.wrapped.{self.name}-{self._container_type}"
        wrapt_script = job.metadir / base
        await wrapt_script.a_write_text(self.wrap_job_script(job))

        return wrapt_script

    def jobcmd_shebang(self, job: Job) -> str:
        """The shebang of the wrapper script"""
        cmd = [self.bin, "run"]
        if self._container_type == "apptainer":
            if self.cwd:  # pragma: no cover
                cmd.extend(["--pwd", self.cwd])
            else:
                cmd.extend(["--pwd", str(self.workdir.mounted)])
            for key, value in job.envs.items():
                cmd.extend(["--env", f"{key}={value}"])
            for vol in self.volumes:
                cmd.extend(["--bind", f"{vol}"])
        else:
            if self.remove:
                cmd.append("--rm")
            cmd.extend(["--user", self.user])
            for key, value in job.envs.items():
                cmd.extend(["-e", f"{key}={value}"])
            for vol in self.volumes:
                cmd.extend(["-v", vol])

            if self.cwd:
                cmd.extend(["--workdir", self.cwd])
            else:
                cmd.extend(["--workdir", str(self.workdir.mounted)])

        cmd.extend(self.bin_args)
        cmd.append(self.image)
        cmd.extend(self.entrypoint)

        return shlex.join(cmd)

    async def submit_job(self, job: Job) -> int:  # type: ignore[override]
        """Submit a job locally

        Args:
            job: The job

        Returns:
            The process id
        """
        wrapt_script_path = (await self.wrapped_job_script(job)).mounted
        # In case the process exits very quickly
        if not await job.jid_file.a_exists():
            await job.jid_file.a_write_text("0")

        proc = await asyncio.create_subprocess_exec(
            *shlex.split(self.jobcmd_shebang(job)),
            wrapt_script_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            start_new_session=True,
            # Changing the working directory here may cause wrapped_job_script to fail
            # to be found, so we don't set cwd here.
            # The cwd is changed in the wrapper script instead.
            # cwd=self.cwd
        )

        # wait for a while to make sure the process is running
        # this is to avoid the real command is not run when proc is recycled too early
        # this happens for python < 3.12
        await asyncio.sleep(0.1)

        if await job.stdout_file.a_exists():  # pragma: no cover
            # job submitted successfully and already started very soon
            return proc.pid

        if proc.returncode is not None and proc.returncode != 0:
            # The process has already finished and no stdout/stderr files are
            # generated
            # Something went wrong with the wrapper script?
            stderr = await proc.stdout.read()
            raise RuntimeError(
                f"Failed to submit job #{job.index} (rc={proc.returncode}): "
                f"{stderr.decode()}\n"
                f"Command: {self.jobcmd_shebang(job)} "
                f"{wrapt_script_path}\n"
            )

        # don't await for the results, as this will run the real command
        return proc.pid

    def jobcmd_init(self, job) -> str:
        init_cmd = super().jobcmd_init(job)
        path_envs_exports = [
            f"export {key}={shlex.quote(value)}"
            for key, value in self._path_envs.items()
        ]
        if path_envs_exports:
            path_envs_exports.insert(0, "# Mounted paths")
            init_cmd = "\n".join(path_envs_exports) + "\n" + init_cmd

        return init_cmd
