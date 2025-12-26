"""The scheduler to run jobs via containers"""

from __future__ import annotations

import os
import shlex
import shutil
from pathlib import Path
from typing import List, Sequence

from ..job import Job
from ..defaults import JOBCMD_WRAPPER_LANG
from ..path import SpecPath
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

    async def wrapped_job_script(self, job: Job, _mounted: bool = False) -> SpecPath:
        """Get the wrapped job script

        Args:
            job: The job

        Returns:
            The path of the wrapped job script
        """
        base = f"job.wrapped.{self._container_type}"
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
        return await super().submit_job(job, _mounted=True)

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
