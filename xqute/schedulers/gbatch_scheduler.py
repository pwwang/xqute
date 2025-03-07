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
from ..defaults import JOBCMD_WRAPPER_LANG, get_jobcmd_wrapper_init
from ..utils import logger
from ..path import SpecPath


JOBNAME_PREFIX_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9-]{0,47}$")
DEFAULT_MOUNTED_WORKDIR = "/mnt/xqute_workdir"


class GbatchScheduler(Scheduler):
    """Scheduler for Google Cloud Batch"""

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

        self.config.setdefault("taskGroups", [])
        if not self.config.taskGroups:
            self.config.taskGroups.append(Diot())
        if not self.config.taskGroups[0]:
            self.config.taskGroups[0] = Diot()

        self.config.taskGroups[0].setdefault("taskSpec", Diot())
        self.config.taskGroups[0].taskSpec.setdefault("runnables", [])
        if not self.config.taskGroups[0].taskSpec.runnables:
            self.config.taskGroups[0].taskSpec.runnables.append(Diot())
        if not self.config.taskGroups[0].taskSpec.runnables[0]:
            self.config.taskGroups[0].taskSpec.runnables[0] = Diot()
        self.config.taskGroups[0].taskSpec.runnables[0].script = Diot(
            text=None  # placeholder for job command
        )
        # Only logs the stdout/stderr of submission (when wrapped script doesn't run)
        # The logs of the wrapped script are logged to stdout/stderr files
        # in the workdir.
        self.config.setdefault("logsPolicy", Diot())
        self.config.logsPolicy.setdefault("destination", "CLOUD_LOGGING")

        self.config.taskGroups[0].taskSpec.setdefault("volumes", [])
        if not isinstance(self.config.taskGroups[0].taskSpec.volumes, list):
            raise ValueError(
                "'taskGroups[0].taskSpec.volumes' should be a list for "
                "gbatch configuration."
            )

        meta_volume = Diot()
        meta_volume.gcs = Diot(remotePath=self.workdir._no_prefix)
        meta_volume.mountPath = str(self.workdir.mounted)

        self.config.taskGroups[0].taskSpec.volumes.insert(0, meta_volume)

    @property
    def jobcmd_wrapper_init(self) -> str:
        return get_jobcmd_wrapper_init(True, self.remove_jid_after_done)

    def job_config_file(self, job: Job) -> SpecPath:
        base = f"job.wrapped.{self.name}.json"
        conf_file = job.metadir / base

        wrapt_script = self.wrapped_job_script(job)
        config = deepcopy(self.config)
        config.taskGroups[0].taskSpec.runnables[0].script.text = shlex.join(
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
