"""The scheduler to run jobs on Slurm"""
import asyncio
from pathlib import Path
from typing import Type

from ..job import Job
from ..scheduler import Scheduler
from ..utils import a_read_text


class SlurmJob(Job):
    """Slurm job"""

    def shebang(self, scheduler: Scheduler) -> str:
        """Make the shebang with options

        Args:
            scheduler: The scheduler

        Returns:
            The shebang with options
        """
        options = {
            key[6:]: val
            for key, val in scheduler.config.items()
            if key.startswith("slurm_")
        }
        sbatch_options = {
            key[7:]: val
            for key, val in scheduler.config.items()
            if key.startswith("sbatch_")
        }
        options.update(sbatch_options)

        jobname_prefix = scheduler.config.get(
            "scheduler_jobprefix",
            scheduler.name,
        )
        options["job-name"] = f"{jobname_prefix}.{self.index}"
        options["chdir"] = str(Path.cwd().resolve())
        options["output"] = self.stdout_file
        options["error"] = self.stderr_file

        options_list = []
        for key, val in options.items():
            key = key.replace("_", "-")
            if len(key) == 1:
                fmt = "#SBATCH -{key} {val}"
            else:
                fmt = "#SBATCH --{key}={val}"
            options_list.append(fmt.format(key=key, val=val))

        options_str = "\n".join(options_list)

        return f"{super().shebang(scheduler)}\n{options_str}\n"


class SlurmScheduler(Scheduler):
    """The Slurm scheduler

    Attributes:
        name: The name of the scheduler
        job_class: The job class

    Args:
        sbatch: path to sbatch command
        squeue: path to squeue command
        scancel: path to scancel command
        slurm_*: Slurm options for sbatch.
        ... other Scheduler args
    """

    name: str = "slurm"
    job_class: Type[Job] = SlurmJob

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sbatch = self.config.get("sbatch", "sbatch")
        # self.srun = self.config.get('srun', 'srun')
        self.scancel = self.config.get("scancel", "scancel")
        self.squeue = self.config.get("squeue", "squeue")

    async def submit_job(self, job: Job) -> str:
        """Submit a job to Slurm

        Args:
            job: The job

        Returns:
            The job id
        """
        proc = await asyncio.create_subprocess_exec(
            self.sbatch,
            # str(await job.wrapped_script(self. self.srun)),
            str(await job.wrapped_script(self)),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        rc = await proc.wait()
        if rc != 0:  # pragma: no cover
            stderr = await proc.stderr.read()
            raise Exception(stderr.decode().strip())

        stdout = await proc.stdout.read()
        # salloc: Granted job allocation 65537
        # sbatch: Submitted batch job 65537
        return stdout.decode().strip().split()[-1]

    async def kill_job(self, job: Job):
        """Kill a job on Slurm

        Args:
            job: The job
        """
        proc = await asyncio.create_subprocess_exec(
            self.scancel,
            str(job.jid),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.wait()

    async def job_is_running(self, job: Job) -> bool:
        """Tell if a job is really running, not only the job.jid_file

        In case where the jid file is not cleaned when job is done.

        Args:
            job: The job

        Returns:
            True if it is, otherwise False
        """
        try:
            jid = await a_read_text(job.jid_file)
        except FileNotFoundError:
            return False

        if not jid:
            return False

        proc = await asyncio.create_subprocess_exec(
            self.squeue,
            "-j",
            jid,
            "--noheader",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.wait()
        if proc.returncode != 0:
            return False

        # ['8792', 'queue', 'merge', 'user', 'R', '7:34:34', '1', 'server']
        st = (
            await proc.stdout.read()  # type: ignore
        ).decode().strip().split()[4]
        # If job is still take resources, it is running
        return st in (
            "R",
            "RUNNING",
            "PD",
            "PENDING",
            "CG",
            "COMPLETING",
            "S",
            "SUSPENDED",
            "CF",
            "CONFIGURING",
            # Job is being held after requested reservation was deleted.
            "RD",
            "RESV_DEL_HOLD",
            # Job is being requeued by a federation.
            "RF",
            "REQUEUE_FED",
            # Held job is being requeued.
            "RH",
            "REQUEUE_HOLD",
            # Completing job is being requeued.
            "RQ",
            "REQUEUED",
            # Job is about to change size.
            "RS",
            "RESIZING",
            # Sibling was removed from cluster due to other cluster
            # starting the job.
            "RV",
            "REVOKED",
            # The job was requeued in a special state. This state can be set by
            # users, typically in EpilogSlurmctld, if the job has terminated
            # with a particular exit value.
            "SE",
            "SPECIAL_EXIT",
            # Job is staging out files.
            "SO",
            "STAGE_OUT",
            # Job has an allocation, but execution has been stopped with
            # SIGSTOP signal. CPUS have been retained by this job.
            "ST",
            "STOPPED",
        )
