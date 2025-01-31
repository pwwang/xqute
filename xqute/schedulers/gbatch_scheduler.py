import asyncio
import re
import shlex
from copy import deepcopy
from hashlib import sha256
from cloudpathlib import GSPath
from diot import Diot

from ..job import Job
from ..scheduler import Scheduler
from ..defaults import (
    JobStatus,
    JobErrorStrategy,
    JOBCMD_WRAPPER_LANG,
    JOBCMD_WRAPPER_TEMPLATE,
)
from ..utils import PathType
from ..plugin import plugin


JOBNAME_PREFIX_RE = re.compile(r"^[a-zA-Z][a-zA-Z0-9-]{0,47}$")
DEFAULT_REMOTE_WORKDIR = "/mnt/.xqute_workdir"


class GbatchJob(Job):
    """Job for Google Cloud Batch"""

    def wrap_script(self, scheduler: Scheduler) -> str:
        jobcmd_init = plugin.hooks.on_jobcmd_init(scheduler, self)
        jobcmd_prep = plugin.hooks.on_jobcmd_prep(scheduler, self)
        jobcmd_end = plugin.hooks.on_jobcmd_end(scheduler, self)

        return JOBCMD_WRAPPER_TEMPLATE.format(
            shebang=JOBCMD_WRAPPER_LANG,
            status=JobStatus,
            job=self,
            jobcmd_init="\n".join(jobcmd_init),
            jobcmd_prep="\n".join(jobcmd_prep),
            jobcmd_end="\n".join(jobcmd_end),
            cmd=shlex.join(self.cmd),
            prescript=scheduler.prescript,
            postscript=scheduler.postscript,
        )

    def config_file(self, scheduler: Scheduler, remote: bool = False) -> PathType:
        base = f"job.wrapped.{scheduler.name}.json"
        conf_file = self.metadir / base

        wrapt_script = self.wrapped_script(scheduler, remote=True)
        config = deepcopy(scheduler.config)
        config.taskGroups[0].taskSpec.runnables[0].script.text = shlex.join(
            shlex.split(JOBCMD_WRAPPER_LANG) + [str(wrapt_script)]
        )
        config.to_json(conf_file, indent=2)
        return (self.remote_metadir / base) if remote else conf_file


class GbatchScheduler(Scheduler):
    """Scheduler for Google Cloud Batch"""

    name = "gbatch"
    job_class = GbatchJob

    __slots__ = Scheduler.__slots__ + (
        "gcloud",
        "project",
        "location",
        "remote_workdir",
    )

    def __init__(self, project: str, location: str, *args, **kwargs):
        """Construct the gbatch scheduler"""
        self.gcloud = kwargs.pop("gcloud", "gcloud")
        self.project = project
        self.location = location
        self.remote_workdir = kwargs.pop("remote_workdir", DEFAULT_REMOTE_WORKDIR)
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

        self.config.taskGroups[0].taskSpec.setdefault("volumes", [])
        if not isinstance(self.config.taskGroups[0].taskSpec.volumes, list):
            raise ValueError(
                "'taskGroups[0].taskSpec.volumes' should be a list for "
                "gbatch configuration."
            )

        meta_volume = Diot()
        meta_volume.gcs = Diot(remotePath=str(self.workdir))
        meta_volume.mountPath = self.remote_workdir

        self.config.taskGroups[0].taskSpec.volumes.append(meta_volume)

    def create_job(self, index, cmd) -> Job:
        """Create a job

        Args:
            index: The index of the job
            cmd: The command for the job

        Returns:
            The new Job instance
        """
        return self.job_class(
            index=index,
            cmd=cmd,
            workdir=self.workdir,
            error_retry=self.error_strategy == JobErrorStrategy.RETRY,
            num_retries=self.num_retries,
            remote_workdir=self.remote_workdir,
        )

    async def _delete_job(self, job: Job) -> None:
        """Try to delete the job from google cloud's registry

        As google doesn't allow jobs to have the same id.

        Args:
            job: The job to delete
        """
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
        else:
            await proc.wait()

    async def submit_job(self, job: Job) -> str:
        await self._delete_job(job)

        sha = sha256(str(self.workdir).encode()).hexdigest()[:8]
        jobname = f"{self.jobname_prefix}-{sha}-{job.index}".lower()

        proc = await asyncio.create_subprocess_exec(
            self.gcloud,
            "batch",
            "jobs",
            "submit",
            jobname,
            "--config",
            job.config_file(self, remote=True),
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
                f"Can't submit job to Google Cloud Batch: {stderr.decode()}"
            )

        return jobname

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

    async def job_is_running(self, job: Job) -> bool:
        # https://github.com/drivendataorg/cloudpathlib/issues/388
        if not job.jid_file.is_file():
            return False

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
        except Exception:
            return False

        if await proc.wait() != 0:
            return False

        stdout = await proc.stdout.read()
        return (
            b"state: RUNNING" in stdout
            or b"state: QUEUED" in stdout
            or b"state: SCHEDULED" in stdout
        )
