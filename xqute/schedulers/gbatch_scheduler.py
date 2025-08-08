import asyncio
import json
import re
import shlex
from copy import deepcopy
from hashlib import sha256
from yunpath import GSPath
from diot import Diot

from ..job import Job
from ..scheduler import Scheduler
from ..defaults import JOBCMD_WRAPPER_LANG
from ..utils import logger
from ..path import SpecPath


JOBNAME_PREFIX_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9-]{0,47}$")
DEFAULT_MOUNTED_WORKDIR = "/mnt/disks/xqute_workdir"


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
    """

    name = "gbatch"

    __slots__ = Scheduler.__slots__ + (
        "gcloud",
        "project",
        "location",
    )

    def __init__(self, *args, project: str, location: str, **kwargs):
        """Construct the gbatch scheduler"""
        self.gcloud = kwargs.pop("gcloud", "gcloud")
        self.project = project
        self.location = location
        kwargs.setdefault("mounted_workdir", DEFAULT_MOUNTED_WORKDIR)
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

        task_groups = self.config.setdefault("taskGroups", [])
        if not task_groups:
            task_groups.append(Diot())
        if not task_groups[0]:
            task_groups[0] = Diot()

        task_spec = task_groups[0].setdefault("taskSpec", Diot())
        runnables = task_spec.setdefault("runnables", [])
        if not runnables:
            runnables.append(Diot())
        if not runnables[0]:
            runnables[0] = Diot()

        if "container" in runnables[0]:
            if not isinstance(runnables[0].container, dict):  # pragma: no cover
                raise ValueError(
                    "'taskGroups[0].taskSpec.runnables[0].container' should be a "
                    "dictionary for gbatch configuration."
                )
            runnables[0].container.setdefault("commands", [])
        else:
            runnables[0].script = Diot(text=None)  # placeholder for job command

        # Only logs the stdout/stderr of submission (when wrapped script doesn't run)
        # The logs of the wrapped script are logged to stdout/stderr files
        # in the workdir.
        logs_policy = self.config.setdefault("logsPolicy", Diot())
        logs_policy.setdefault("destination", "CLOUD_LOGGING")

        volumes = task_spec.setdefault("volumes", [])
        if not isinstance(volumes, list):
            raise ValueError(
                "'taskGroups[0].taskSpec.volumes' should be a list for "
                "gbatch configuration."
            )

        meta_volume = Diot()
        meta_volume.gcs = Diot(remotePath=self.workdir._no_prefix)
        meta_volume.mountPath = str(self.workdir.mounted)

        volumes.insert(0, meta_volume)

        # Add some labels for filtering by `gcloud batch jobs list`
        labels = self.config.setdefault("labels", Diot())
        labels.setdefault("xqute", "true")
        email = (
            self.config.get("allocationPolicy", {})
            .get("serviceAccount", {})
            .get("email")
        )
        if email:
            # 63 character limit, '@' is not allowed in labels
            # labels.setdefault("email", email[:63])
            labels.setdefault("user", email.split("@", 1)[0][:63])

    def job_config_file(self, job: Job) -> SpecPath:
        base = f"job.wrapped.{self.name}.json"
        conf_file = job.metadir / base

        wrapt_script = self.wrapped_job_script(job)
        config = deepcopy(self.config)
        runnable = config.taskGroups[0].taskSpec.runnables[0]
        if "container" in runnable:
            container = runnable.container
            if "entrypoint" not in container:
                # supports only /bin/bash, but not /bin/bash -u
                container.entrypoint = JOBCMD_WRAPPER_LANG
                container.commands.append(str(wrapt_script.mounted))
            elif any("{script}" in cmd for cmd in container.commands):
                # If the entrypoint is already set, we assume it is a script
                # that will be executed with the job command.
                container.commands = [
                    cmd.replace("{lang}", str(JOBCMD_WRAPPER_LANG)).replace(
                        "{script}", str(wrapt_script.mounted)
                    )
                    for cmd in container.commands
                ]
            else:
                container.commands.append(
                    shlex.join(
                        shlex.split(JOBCMD_WRAPPER_LANG) + [str(wrapt_script.mounted)]
                    )
                )
        else:
            runnable.script.text = shlex.join(
                shlex.split(JOBCMD_WRAPPER_LANG) + [str(wrapt_script.mounted)]
            )

        with conf_file.open("w") as f:
            json.dump(config, f, indent=2)

        return conf_file

    async def _delete_job(self, job: Job) -> None:
        """Try to delete the job from google cloud's registry

        As google doesn't allow jobs to have the same id.

        Args:
            job: The job to delete
        """
        logger.debug(
            "/Scheduler-%s Try deleting job %r on GCP.",
            self.name,
            job,
        )
        status = await self._get_job_status(job)
        while status.endswith("_IN_PROGRESS"):  # pragma: no cover
            await asyncio.sleep(1)
            status = await self._get_job_status(job)

        command = [
            self.gcloud,
            "batch",
            "jobs",
            "delete",
            job.jid,
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
        else:  # pragma: no cover
            await proc.wait()

        status = await self._get_job_status(job)
        while status == "DELETION_IN_PROGRESS":  # pragma: no cover
            await asyncio.sleep(1)
            status = await self._get_job_status(job)

        if status != "UNKNOWN":  # pragma: no cover
            logger.warning(
                "/Scheduler-%s Failed to delete job %r on GCP, submision may fail.",
                self.name,
                job,
            )

    async def submit_job(self, job: Job) -> str:

        sha = sha256(str(self.workdir).encode()).hexdigest()[:8]
        job.jid = f"{self.jobname_prefix}-{sha}-{job.index}".lower()
        await self._delete_job(job)

        conf_file = self.job_config_file(job)
        proc = await asyncio.create_subprocess_exec(
            self.gcloud,
            "batch",
            "jobs",
            "submit",
            job.jid,
            "--config",
            conf_file.fspath,
            "--project",
            self.project,
            "--location",
            self.location,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        _, stderr = await proc.communicate()
        if proc.returncode != 0:  # pragma: no cover
            raise RuntimeError(
                "Can't submit job to Google Cloud Batch: \n"
                f"{stderr.decode()}\n"
                "Check the configuration file:\n"
                f"{conf_file}"
            )

        return job.jid

    async def kill_job(self, job: Job):
        command = [
            self.gcloud,
            "alpha",
            "batch",
            "jobs",
            "cancel",
            job.jid,
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
        if not job.jid_file.is_file():
            return "UNKNOWN"

        # Do not rely on _jid, as it can be a obolete job.
        jid = job.jid_file.read_text().strip()

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
        except Exception:  # pragma: no cover
            return "UNKNOWN"

        if await proc.wait() != 0:
            return "UNKNOWN"

        stdout = (await proc.stdout.read()).decode()
        return re.search(r"state: (.+)", stdout).group(1)

    async def job_is_running(self, job: Job) -> bool:
        status = await self._get_job_status(job)
        return status in ("RUNNING", "QUEUED", "SCHEDULED")
