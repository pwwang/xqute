"""The scheduler to run jobs via containers"""
from __future__ import annotations

import os
import shlex
import shutil
from pathlib import Path
from typing import Dict, List, Sequence

from ..job import Job
from ..defaults import JOBCMD_WRAPPER_LANG
from ..path import SpecPath
from .local_scheduler import LocalScheduler


DEFAULT_MOUNTED_WORKDIR = "/mnt/disks/xqute_workdir"
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
        envs: Environment variables to set in container
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
        "envs",
        "remove",
        "user",
        "bin_args",
        "_container_type",
    )

    def __init__(
        self,
        image: str,
        entrypoint: str | List[str] = JOBCMD_WRAPPER_LANG,
        bin: str = "docker",
        volumes: str | Sequence[str] | None = None,
        envs: Dict[str, str] | None = None,
        remove: bool = True,
        user: str | None = None,
        bin_args: List[str] | None = None,
        **kwargs
    ):
        kwargs.setdefault("mounted_workdir", DEFAULT_MOUNTED_WORKDIR)
        super().__init__(**kwargs)

        self.bin = shutil.which(bin)
        if not self.bin:
            raise ValueError(
                f"Container runtime binary '{bin}' not found in PATH"
            )

        self.image = image
        self.entrypoint = (
            list(entrypoint)
            if isinstance(entrypoint, (list, tuple))
            else [entrypoint]
        )
        self.volumes = volumes or []
        self.volumes = (
            [self.volumes] if isinstance(self.volumes, str) else list(self.volumes)
        )
        self.envs = envs or {}
        self.remove = remove
        self.user = user or f"{os.getuid()}:{os.getgid()}"
        self.bin_args = bin_args or []
        self.volumes.append(f"{self.workdir}:{self.workdir.mounted}")

        self._container_type = CONTAINER_TYPES.get(
            Path(self.bin).name.lower(),
            "docker",
        )
        if (
            self._container_type in ("docker", "podman")
            and self.image.startswith("docker://")
        ):
            # Convert docker://image to image name
            self.image = self.image[9:]

    def wrapped_job_script(self, job: Job) -> SpecPath:
        """Get the wrapped job script

        Args:
            job: The job

        Returns:
            The path of the wrapped job script
        """
        base = f"job.wrapped.{self._container_type}"
        wrapt_script = job.metadir / base
        wrapt_script.write_text(self.wrap_job_script(job))

        return wrapt_script

    def jobcmd_shebang(self, job: Job) -> str:
        """The shebang of the wrapper script"""
        cmd = [self.bin, "run"]
        if self._container_type == "apptainer":
            if self.cwd:  # pragma: no cover
                cmd.extend(["--pwd", self.cwd])
            else:
                cmd.extend(["--pwd", str(self.workdir.mounted)])
            for key, value in self.envs.items():
                cmd.extend(["--env", f"{key}={value}"])
            for vol in self.volumes:
                cmd.extend(["--bind", f"{vol}"])
        else:
            if self.remove:
                cmd.append("--rm")
            cmd.extend(["--user", self.user])
            for key, value in self.envs.items():
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

    async def submit_job(self, job: Job) -> int:
        """Submit a job locally

        Args:
            job: The job

        Returns:
            The process id
        """
        return await super().submit_job(job, _mounted=True)
