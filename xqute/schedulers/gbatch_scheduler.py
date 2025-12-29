from __future__ import annotations

import asyncio
import json
import re
import shlex
import getpass
from typing import Sequence
from copy import deepcopy
from hashlib import sha256
from panpath import GSPath, PanPath

from ..job import Job
from ..scheduler import Scheduler
from ..defaults import (
    JOBCMD_WRAPPER_LANG,
    # get_jobcmd_wrapper_init,
    # JOBCMD_WRAPPER_TEMPLATE,
    # JobStatus,
    SLEEP_INTERVAL_GBATCH_STATUS_CHECK,
)
from ..utils import logger
from ..path import SpecPath


JOBNAME_PREFIX_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9-]{0,47}$")
NAMED_MOUNT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*=.+$")
DEFAULT_MOUNTED_ROOT = "/mnt/disks"


class GbatchScheduler(Scheduler):
    """Scheduler for Google Cloud Batch

    You can pass extra configuration parameters to the constructor
    that will be used in the job configuration file.
    For example, you can pass `taskGroups` to specify the task groups
    and their specifications.

    For using containers, it is a little bit tricky to specify the commands.
    When no `entrypoint` is specified, the `commands` should be a list
    with the first element being the interpreter (e.g. `/bin/bash`)
    and the second element being the path to the wrapped job script.
    If the `entrypoint` is specified, we can use the `{lang}` and `{script}`
    placeholders in the `commands` list, where `{lang}` will be replaced
    with the interpreter (e.g. `/bin/bash`) and `{script}` will be replaced
    with the path to the wrapped job script.
    With `entrypoint` specified and no `{script}` placeholder, the joined command
    will be the interpreter followed by the path to the wrapped job script will be
    appended to the `commands` list.

    Args:
        project: GCP project ID
        location: GCP location (e.g. us-central1)
        mount: GCS path to mount (e.g. gs://my-bucket:/mnt/my-bucket)
            You can pass a list of mounts.
            You can also use named mount like `NAME=gs://bucket/dir`
            then it will be mounted to `/mnt/disks/NAME` in the container.
            You can use environment variable `NAME` in your job scripts to
            refer to the mounted path.
        service_account: GCP service account email (e.g. test-account@example.com)
        network: GCP network (e.g. default-network)
        subnetwork: GCP subnetwork (e.g. regions/us-central1/subnetworks/default)
        no_external_ip_address: Whether to disable external IP address
        machine_type: GCP machine type (e.g. e2-standard-4)
        provisioning_model: GCP provisioning model (e.g. SPOT)
        image_uri: Container image URI (e.g. ubuntu-2004-lts)
        entrypoint: Container entrypoint (e.g. /bin/bash)
        commands: The command list to run in the container.
            There are three ways to specify the commands:
            1. If no entrypoint is specified, the final command will be
            [commands, wrapped_script], where the entrypoint is the wrapper script
            interpreter that is determined by `JOBCMD_WRAPPER_LANG` (e.g. /bin/bash),
            commands is the list you provided, and wrapped_script is the path to the
            wrapped job script.
            2. You can specify something like "-c", then the final command
            will be ["-c", "wrapper_script_interpreter, wrapper_script"]
            3. You can use the placeholders `{lang}` and `{script}` in the commands
            list, where `{lang}` will be replaced with the interpreter (e.g. /bin/bash)
            and `{script}` will be replaced with the path to the wrapped job script.
            For example, you can specify ["{lang} {script}"] and the final command
            will be ["wrapper_interpreter, wrapper_script"]
        runnables: Additional runnables to run before or after the main job.
            Each runnable should be a dictionary that follows the
            [GCP Batch API specification](https://cloud.google.com/batch/docs/reference/rest/v1/projects.locations.jobs#runnable).
            You can also specify an "order" key in the dictionary to control the
            execution order of the runnables. Runnables with negative order
            will be executed before the main job, and those with non-negative
            order will be executed after the main job. The main job runnable
            will always be executed in the order it is defined in the list.
        *args, **kwargs: Other arguments passed to base Scheduler class
    """  # noqa: E501

    name = "gbatch"

    __slots__ = Scheduler.__slots__ + (
        "gcloud",
        "project",
        "location",
        "runnable_index",
        "_path_envs",
    )

    def __init__(
        self,
        *args,
        project: str,
        location: str,
        mount: str | Sequence[str] | None = None,
        service_account: str | None = None,
        network: str | None = None,
        subnetwork: str | None = None,
        no_external_ip_address: bool | None = None,
        machine_type: str | None = None,
        provisioning_model: str | None = None,
        image_uri: str | None = None,
        entrypoint: str = None,
        commands: str | Sequence[str] | None = None,
        runnables: Sequence[dict] | None = None,
        **kwargs,
    ):
        """Construct the gbatch scheduler"""
        self.gcloud = kwargs.pop("gcloud", "gcloud")
        self.project = project
        self.location = location
        kwargs.setdefault("mounted_workdir", f"{DEFAULT_MOUNTED_ROOT}/xqute_workdir")
        super().__init__(*args, **kwargs)

        if not isinstance(self.workdir, GSPath):
            raise ValueError(
                "'gbatch' scheduler requires google cloud storage 'workdir'."
            )

        if not JOBNAME_PREFIX_RE.match(self.jobname_prefix):
            raise ValueError(
                "'jobname_prefix' for gbatch scheduler doesn't follow pattern "
                "^[a-zA-Z][a-zA-Z0-9-]{0,47}$."
            )

        self._path_envs = {}
        task_groups = self.config.setdefault("taskGroups", [])
        if not task_groups:
            task_groups.append({})
        if not task_groups[0]:
            task_groups[0] = {}

        task_spec = task_groups[0].setdefault("taskSpec", {})
        task_runnables = task_spec.setdefault("runnables", [])

        # Process additional runnables with ordering
        additional_runnables = []
        if runnables:
            for runnable_dict in runnables:
                runnable_copy = deepcopy(runnable_dict)
                order = runnable_copy.pop("order", 0)
                additional_runnables.append((order, runnable_copy))

        # Sort by order
        additional_runnables.sort(key=lambda x: x[0])

        # Create main job runnable
        if not task_runnables:
            task_runnables.append({})
        if not task_runnables[0]:
            task_runnables[0] = {}

        job_runnable = task_runnables[0]
        if "container" in job_runnable or image_uri:
            job_runnable.setdefault("container", {})
            if not isinstance(job_runnable["container"], dict):  # pragma: no cover
                raise ValueError(
                    "'taskGroups[0].taskSpec.runnables[0].container' should be a "
                    "dictionary for gbatch configuration."
                )
            if image_uri:
                job_runnable["container"].setdefault("image_uri", image_uri)
            if entrypoint:
                job_runnable["container"].setdefault("entrypoint", entrypoint)

            job_runnable["container"].setdefault("commands", commands or [])
        else:
            job_runnable["script"] = {
                "text": None,  # placeholder for job command
                "_commands": commands,  # Store commands for later use
            }

        # Clear existing runnables and rebuild with proper ordering
        task_runnables.clear()

        # Add runnables with negative order (before job)
        for order, runnable_dict in additional_runnables:
            if order < 0:
                task_runnables.append(runnable_dict)

        # Add the main job runnable
        task_runnables.append(job_runnable)
        self.runnable_index = len(task_runnables) - 1

        # Add runnables with positive order (after job)
        for order, runnable_dict in additional_runnables:
            if order >= 0:
                task_runnables.append(runnable_dict)

        # Only logs the stdout/stderr of submission (when wrapped script doesn't run)
        # The logs of the wrapped script are logged to stdout/stderr files
        # in the workdir.
        logs_policy = self.config.setdefault("logsPolicy", {})
        logs_policy.setdefault("destination", "CLOUD_LOGGING")

        volumes = task_spec.setdefault("volumes", [])
        if not isinstance(volumes, list):
            raise ValueError(
                "'taskGroups[0].taskSpec.volumes' should be a list for "
                "gbatch configuration."
            )

        volumes.insert(
            0,
            {
                "gcs": {"remotePath": str(self.workdir).split("://", 1)[1]},
                "mountPath": str(self.workdir.mounted),
            },
        )

        if mount and not isinstance(mount, (tuple, list)):
            mount = [mount]  # type: ignore
        if mount:
            for m in mount:
                # Let's check if mount is provided as "OUTDIR=gs://bucket/dir"
                # If so, we mounted it to $DEFAULT_MOUNTED_ROOT/OUTDIR
                # and set OUTDIR env variable to the mounted path in self._path_envs
                if NAMED_MOUNT_RE.match(m):
                    name, gcs = m.split("=", 1)
                    if not gcs.startswith("gs://"):
                        raise ValueError(
                            "When using named mount, it should be in the format "
                            "'NAME=gs://bucket/dir', where NAME matches "
                            "^[A-Za-z][A-Za-z0-9_]*$"
                        )
                    gcs_path = PanPath(gcs)
                    # Check if it is a file path
                    if gcs_path.is_file():
                        # Mount the parent directory
                        gcs = str(gcs_path.parent._no_prefix)
                        mount_path = (
                            f"{DEFAULT_MOUNTED_ROOT}/{name}/{gcs_path.parent.name}"
                        )
                        self._path_envs[name] = f"{mount_path}/{gcs_path.name}"
                    else:
                        gcs = gcs[5:]
                        mount_path = f"{DEFAULT_MOUNTED_ROOT}/{name}"
                        self._path_envs[name] = mount_path

                    volumes.append(
                        {
                            "gcs": {"remotePath": gcs},
                            "mountPath": mount_path,
                        }
                    )
                else:
                    # Or, we expect a literal mount "gs://bucket/dir:/mount/path"
                    gcs, mount_path = m.rsplit(":", 1)
                    if gcs.startswith("gs://"):
                        gcs = gcs[5:]
                    volumes.append(
                        {
                            "gcs": {"remotePath": gcs},
                            "mountPath": mount_path,
                        }
                    )

        # Add some labels for filtering by `gcloud batch jobs list`
        labels = self.config.setdefault("labels", {})

        labels.setdefault("xqute", "true")
        labels.setdefault("user", getpass.getuser())

        allocation_policy = self.config.setdefault("allocationPolicy", {})

        if service_account:
            allocation_policy.setdefault("serviceAccount", {}).setdefault(
                "email", service_account
            )

        if network or subnetwork or no_external_ip_address is not None:
            network_interface = allocation_policy.setdefault("network", {}).setdefault(
                "networkInterfaces", []
            )
            if not network_interface:
                network_interface.append({})
            network_interface = network_interface[0]
            if network:
                network_interface.setdefault("network", network)
            if subnetwork:
                network_interface.setdefault("subnetwork", subnetwork)
            if no_external_ip_address is not None:
                network_interface.setdefault(
                    "noExternalIpAddress", no_external_ip_address
                )

        if machine_type or provisioning_model:
            instances = allocation_policy.setdefault("instances", [])
            if not instances:
                instances.append({})
            policy = instances[0].setdefault("policy", {})
            if machine_type:
                policy.setdefault("machineType", machine_type)
            if provisioning_model:
                policy.setdefault("provisioningModel", provisioning_model)

        email = allocation_policy.get("serviceAccount", {}).get("email")
        if email:
            # 63 character limit, '@' is not allowed in labels
            # labels.setdefault("email", email[:63])
            labels.setdefault("sacct", email.split("@", 1)[0][:63])

    async def job_config_file(self, job: Job) -> SpecPath:
        base = f"job.wrapped.{self.name}.json"
        conf_file = job.metadir / base

        wrapt_script = await self.wrapped_job_script(job)
        config = deepcopy(self.config)
        runnable = config["taskGroups"][0]["taskSpec"]["runnables"][self.runnable_index]
        if "container" in runnable:
            container = runnable["container"]
            if "entrypoint" not in container:
                # supports only /bin/bash, but not /bin/bash -u
                container["entrypoint"] = JOBCMD_WRAPPER_LANG
                container["commands"].append(str(wrapt_script.mounted))
            elif any("{script}" in cmd for cmd in container["commands"]):
                # If the entrypoint is already set, we assume it is a script
                # that will be executed with the job command.
                container["commands"] = [
                    cmd.replace("{lang}", str(JOBCMD_WRAPPER_LANG)).replace(
                        "{script}", str(wrapt_script.mounted)
                    )
                    for cmd in container["commands"]
                ]
            else:
                container["commands"].append(
                    shlex.join(
                        shlex.split(JOBCMD_WRAPPER_LANG) + [str(wrapt_script.mounted)]
                    )
                )
        else:
            # Apply commands for script runnables as well
            stored_commands = runnable["script"].pop("_commands", None)
            if stored_commands:
                if any("{script}" in str(cmd) for cmd in stored_commands):
                    # Use commands with script placeholder replacement
                    command_parts = [
                        shlex.quote(cmd)
                        .replace("{lang}", str(JOBCMD_WRAPPER_LANG))
                        .replace("{script}", str(wrapt_script.mounted))
                        for cmd in stored_commands
                    ]
                else:
                    # Append script to commands
                    command_parts = [
                        *(shlex.quote(str(cmd)) for cmd in stored_commands),
                        shlex.quote(
                            shlex.join(
                                (
                                    *shlex.split(JOBCMD_WRAPPER_LANG),
                                    str(wrapt_script.mounted),
                                )
                            )
                        ),
                    ]
            else:
                command_parts = [
                    *shlex.split(JOBCMD_WRAPPER_LANG),
                    str(wrapt_script.mounted),
                ]

            runnable["script"]["text"] = " ".join(command_parts)

        async with conf_file.a_open("w") as f:
            jsons = json.dumps(config, indent=2)
            await f.write(jsons)

        return conf_file

    async def _delete_job(self, job: Job) -> None:
        """Try to delete the job from google cloud's registry

        As google doesn't allow jobs to have the same id.

        Args:
            job: The job to delete
        """
        logger.debug(
            "/Sched-%s Try deleting job %r on GCP.",
            self.name,
            job,
        )
        status = await self._get_job_status(job)
        while status.endswith("_IN_PROGRESS"):  # pragma: no cover
            await asyncio.sleep(SLEEP_INTERVAL_GBATCH_STATUS_CHECK)
            status = await self._get_job_status(job)

        command = [
            self.gcloud,
            "batch",
            "jobs",
            "delete",
            await job.get_jid(),
            "--project",
            self.project,
            "--location",
            self.location,
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception:
            pass
        else:
            await proc.wait()

        status = await self._get_job_status(job)
        while status == "DELETION_IN_PROGRESS":  # pragma: no cover
            await asyncio.sleep(SLEEP_INTERVAL_GBATCH_STATUS_CHECK)
            status = await self._get_job_status(job)

        if status != "UNKNOWN":
            logger.warning(
                "/Sched-%s Failed to delete job %r on GCP, submision may fail.",
                self.name,
                job,
            )

    async def submit_job(self, job: Job) -> str:

        sha = sha256(str(self.workdir).encode()).hexdigest()[:8]
        jid = f"{self.jobname_prefix}-{sha}-{job.index}".lower()
        await job.set_jid(jid)
        await self._delete_job(job)

        conf_file = await self.job_config_file(job)
        proc = await asyncio.create_subprocess_exec(
            self.gcloud,
            "batch",
            "jobs",
            "submit",
            jid,
            "--config",
            conf_file.mounted,
            "--project",
            self.project,
            "--location",
            self.location,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )

        stdout, _ = await proc.communicate()
        if proc.returncode != 0:  # pragma: no cover
            raise RuntimeError(
                "Can't submit job to Google Cloud Batch: \n"
                f"{stdout.decode()}\n"
                "Check the configuration file:\n"
                f"{conf_file}"
            )

        return jid

    async def kill_job(self, job: Job):
        command = [
            self.gcloud,
            "alpha",
            "batch",
            "jobs",
            "cancel",
            await job.get_jid(),
            "--project",
            self.project,
            "--location",
            self.location,
            "--quiet",
        ]
        proc = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.wait()

    async def _get_job_status(self, job: Job) -> str:
        if not await job.jid_file.a_is_file():
            return "UNKNOWN"

        # Do not rely on _jid, as it can be a obolete job.
        jid = (await job.jid_file.a_read_text()).strip()

        command = [
            self.gcloud,
            "batch",
            "jobs",
            "describe",
            jid,
            "--project",
            self.project,
            "--location",
            self.location,
        ]

        try:
            proc = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except Exception:
            return "UNKNOWN"

        if await proc.wait() != 0:
            return "UNKNOWN"

        stdout = (await proc.stdout.read()).decode()
        return re.search(r"state: (.+)", stdout).group(1)

    async def job_fails_before_running(self, job: Job) -> bool:  # pragma: no cover
        status = await self._get_job_status(job)
        return status in ("FAILED", "DELETION_IN_PROGRESS", "CANCELLED")

    async def job_is_running(self, job: Job) -> bool:
        status = await self._get_job_status(job)
        return status in ("RUNNING", "QUEUED", "SCHEDULED")

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
