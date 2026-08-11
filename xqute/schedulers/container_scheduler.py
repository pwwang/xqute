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
from ..defaults import DEFAULT_WORKDIR_NAME, JOBCMD_WRAPPER_LANG
from .local_scheduler import LocalScheduler
from .gbatch_scheduler import GbatchScheduler, sanitize_mounts

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
        mount: Alias for `volumes`
        volume_as_cwd: If set, the container will be run with this volume as the
            working directory. This is useful for running jobs in a specific
            directory inside the container. The volume will be mounted to
            `<DEFAULT_MOUNTED_ROOT>/.cwd` in the container.
        mount_as_cwd: Alias for `volume_as_cwd`
        user: User to run the container as (only for Docker/Podman)
            By default, it runs as the current user (os.getuid() and os.getgid())
        remove: Whether to remove the container after execution.
            Only applies to Docker/Podman.
        bin_args: Additional arguments to pass to the container runtime
        **kwargs: Additional arguments passed to parent Scheduler
    """

    name = "container"
    DEFAULT_MOUNTED_ROOT = GbatchScheduler.DEFAULT_MOUNTED_ROOT
    SUBMIT_JOB_SLEEP = 1

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
        "_kwargs",
    )

    def __init__(
        self,
        *args,
        image: str,
        entrypoint: str | List[str] = JOBCMD_WRAPPER_LANG,
        bin: str = "docker",
        volumes: str | Sequence[str] | None = None,
        volume_as_cwd: str | None = None,
        mount: str | Sequence[str] | None = None,
        mount_as_cwd: str | None = None,
        # envs: Dict[str, str] | None = None,
        remove: bool = True,
        user: str | None = None,
        bin_args: List[str] | None = None,
        **kwargs,
    ):
        if mount and volumes:
            raise ValueError(
                "You can't specify both 'mount' and 'volumes' arguments. "
                "Use only one of them."
            )

        if mount_as_cwd and volume_as_cwd:
            raise ValueError(
                "You can't specify both 'mount_as_cwd' and 'volume_as_cwd' arguments. "
                "Use only one of them."
            )

        volume_as_cwd = volume_as_cwd or mount_as_cwd

        cwd = kwargs.get("cwd")
        if volume_as_cwd and cwd:
            raise ValueError(
                "You can't specify both 'volume_as_cwd' and 'cwd' arguments. "
                "Use only one of them."
            )

        self.bin = shutil.which(bin)
        if not self.bin:
            raise ValueError(f"Container runtime binary '{bin}' not found in PATH")

        self.image = image
        self.entrypoint = (
            list(entrypoint) if isinstance(entrypoint, (list, tuple)) else [entrypoint]
        )
        self.remove = remove
        self.user = user or f"{os.getuid()}:{os.getgid()}"
        self.bin_args = bin_args or []
        self.volumes = []
        self._container_type = CONTAINER_TYPES.get(
            Path(self.bin).name.lower(),
            "docker",
        )
        if self._container_type in ("docker", "podman") and self.image.startswith(
            "docker://"
        ):
            # Convert docker://image to image name
            self.image = self.image[9:]

        if not args:
            kwargs.setdefault("workdir", DEFAULT_WORKDIR_NAME)
        super().__init__(*args, **kwargs)

        self._kwargs = {
            "volumes": volumes or mount,
            "volume_as_cwd": volume_as_cwd or mount_as_cwd,
            "workdir": kwargs.get("workdir"),
            "mounted_workdir": kwargs.get("mounted_workdir"),
        }

    async def post_init(self):
        """Post initialization to handle mounts and workdir"""
        volumes: list[str] = self._kwargs["volumes"] or []
        if not isinstance(volumes, Sequence) or isinstance(volumes, str):
            volumes = [volumes]
        else:
            volumes = list(volumes)

        volume_as_cwd = self._kwargs["volume_as_cwd"]
        if volume_as_cwd:
            volumes.insert(0, f"{volume_as_cwd}:{self.DEFAULT_MOUNTED_ROOT}/.cwd")

        mounts, self._path_envs = await sanitize_mounts(
            volumes,
            self.DEFAULT_MOUNTED_ROOT,
        )

        workdir_path = Path(self._kwargs["workdir"] or DEFAULT_WORKDIR_NAME)

        if volume_as_cwd:
            self.cwd = f"{self.DEFAULT_MOUNTED_ROOT}/.cwd"

            workdir_mount_needed = workdir_path.is_absolute()
            if not workdir_mount_needed:
                self._kwargs["workdir"] = f"{volume_as_cwd}/{workdir_path}"
                self._kwargs["mounted_workdir"] = (
                    self._kwargs["mounted_workdir"]
                    or f"{self.cwd}/{workdir_path}"
                )

                # If mounted_workdir is set, and it is not under cwd,
                # we need to mount the workdir as well
                if not any(
                    Path(self._kwargs["mounted_workdir"]).is_relative_to(mounted)
                    for _, mounted in mounts
                ):
                    workdir_mount_needed = True
        elif self.cwd:
            cwd = Path(self.cwd)
            workdir_mount_needed = workdir_path.is_absolute()
            if not workdir_mount_needed:
                cwd_mount = None
                for host, mounted in mounts:
                    if cwd.is_relative_to(mounted):
                        cwd_mount = (
                            host / cwd.relative_to(mounted),
                            mounted / cwd.relative_to(mounted),
                        )
                        break

                if cwd_mount is None:
                    raise ValueError(
                        "Can't determine workdir with a relative path to "
                        "the mounted cwd. Use an absolute path for workdir or ensure "
                        "`cwd` is under one of the mounted paths."
                    )

                self._kwargs["workdir"] = f"{cwd_mount[0]}/{workdir_path}"
                self._kwargs["mounted_workdir"] = (
                    self._kwargs["mounted_workdir"]
                    or f"{cwd_mount[1]}/{workdir_path}"
                )

                if not any(
                    Path(self._kwargs["mounted_workdir"]).is_relative_to(mounted)
                    for _, mounted in mounts
                ):
                    workdir_mount_needed = True
        else:
            self._kwargs["workdir"] = str(workdir_path.resolve())
            workdir_mount_needed = True

        if workdir_mount_needed:
            self._kwargs["mounted_workdir"] = (
                self._kwargs["mounted_workdir"]
                or f"{self.DEFAULT_MOUNTED_ROOT}/{DEFAULT_WORKDIR_NAME}"
            )

        self.workdir = SpecPath(
            self._kwargs["workdir"],
            mounted=self._kwargs["mounted_workdir"],
        )

        for host, mounted in mounts:
            self.volumes.append(f"{host}:{mounted}")

        if workdir_mount_needed:
            self.volumes.append(f"{self.workdir}:{self.workdir.mounted}")

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
        wrapt_script_path = await self.wrapped_job_script(job)
        # In case the process exits very quickly
        if not await job.jid_file.a_exists():
            await job.jid_file.a_write_text("0")

        command_file = wrapt_script_path.with_name(
            f"{wrapt_script_path.name}.submission"
        )
        command = [
            *shlex.split(self.jobcmd_shebang(job)),
            str(wrapt_script_path.mounted),
        ]
        await command_file.a_write_text(" \\\n  ".join(command))

        proc = await asyncio.create_subprocess_exec(
            *command,
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
        await asyncio.sleep(self.SUBMIT_JOB_SLEEP)

        if await job.stdout_file.a_exists():  # pragma: no cover
            # job submitted successfully and already started very soon
            return proc.pid

        if proc.returncode is not None and proc.returncode != 0:
            # The process has already finished and no stdout/stderr files are
            # generated
            # Something went wrong with the wrapper script?
            stderr = await proc.stdout.read()  # type: ignore
            raise RuntimeError(
                f"Failed to submit job #{job.index} (rc={proc.returncode}): "
                f"{stderr.decode()}\n"
                f"Command: {shlex.join(command)}\n"
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
