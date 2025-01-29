import asyncio
import shlex
from diot import Diot

from ..job import Job
from ..scheduler import Scheduler
from ..plugin import plugin


class GBatchJob(Job):
    """Job for Google Cloud Batch"""

    def launch(self, scheduler: Scheduler) -> str:
        """The command to launch the job.

        This will be inserted into the wrapped script.

        Args:
            scheduler: The scheduler

        Returns:
            The command to launch the job
        """
        # By default, suppose the interpreter is the same as the daemon
        # It requires this package to be installed with the python in the scheduler
        # system
        from .. import __version__

        plugins = plugin.get_enabled_plugins()
        launch_cmd = [
            scheduler.python,
            "-m",
            "xqute",
            "++metadir",
            self.metadir,
            "++scheduler",
            scheduler.name,
            "++version",
            __version__,
            "++cmd",
        ]
        launch_cmd.extend(self.cmd)
        for name, plug in plugins.items():  # pragma: no branch
            launch_cmd.extend(["++plugin", f"{name}:{plug.version}"])

        return " ".join(shlex.quote(str(cmditem)) for cmditem in launch_cmd)

    def wrap_script(self, scheduler: Scheduler) -> str:
        jobname_prefix = scheduler.config.get("jobname_prefix", scheduler.name)
        job_config = scheduler.config
        job_config.name = f"{jobname_prefix}.{self.index}"
        job_config.setdefault("taskGroups", [])
        if not job_config.taskGroups:
            job_config.taskGroups.append(Diot())
        if not job_config.taskGroups[0]:
            job_config.taskGroups[0] = Diot()
        job_config.taskGroups[0].setdefault("taskSpec", Diot())
        job_config.taskGroups[0].taskSpec.setdefault("runnables", [])
        if not job_config.taskGroups[0].taskSpec.runnables:
            job_config.taskGroups[0].taskSpec.runnables.append(Diot())
        if not job_config.taskGroups[0].taskSpec.runnables[0]:
            job_config.taskGroups[0].taskSpec.runnables[0] = Diot()
        job_config.taskGroups[0].taskSpec.runnables[0].script = Diot(
            text=self.launch(scheduler)
        )

        return job_config.to_json()


class GBatchScheduler(Scheduler):
    """Scheduler for Google Cloud Batch"""

    name = "gbatch"
    job_class = GBatchJob

    __slots__ = Scheduler.__slots__ + ("gcloud", "project_id", "location")

    def __init__(self, project_id: str, location: str, **kwargs):
        self.gcloud = kwargs.pop("gcloud", "gcloud")
        self.project_id = project_id
        self.location = location
        super().__init__(**kwargs)

    async def submit_job(self, job: Job) -> str:
        jobname_prefix = self.config.get("jobname_prefix", self.name)
        jobname = f"{jobname_prefix}.{job.index}"
        proc = await asyncio.create_subprocess_exec(
            self.gcloud,
            "batch",
            "jobs",
            "submit",
            jobname,
            "--config",
            job.wrapped_script(self),
            "--project",
            self.project_id,
            "--location",
            self.location,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:  # pragma: no cover
            raise RuntimeError(
                f"Can't submit job to Google Cloud Batch: {stderr.decode()}"
            )

        # Job jobprefix.1-7a1654ca-211c-40e8-b0fb-8a00 was successfully submitted.
        try:
            job_id = stdout.decode().splitlines()[0].split()[1].strip()
        except Exception:  # pragma: no cover
            raise RuntimeError("Can't get job id from gcloud output.", stdout, stderr)

        return job_id

    async def kill_job(self, job: Job):
        command = [
            self.gcloud,
            "batch",
            "jobs",
            "delete",
            job.jid,
            "--project",
            self.project_id,
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
        try:
            jid = job.jid_file.read_text().strip()
        except FileNotFoundError:
            return False

        command = [
            self.gcloud,
            "batch",
            "jobs",
            "describe",
            jid,
            "--project",
            self.project_id,
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
        return b"state: RUNNING" in stdout
