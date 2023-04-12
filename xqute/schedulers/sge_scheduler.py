"""The scheduler to run jobs on SGE"""
import asyncio
from typing import Type
from ..defaults import JobStatus
from ..job import Job
from ..scheduler import Scheduler
from ..utils import a_read_text


class SgeJob(Job):
    """SGE job"""

    def wrap_cmd(self, scheduler: Scheduler) -> str:
        """Wrap the command to enable status, returncode, cleaning when
        job exits

        Args:
            scheduler: The scheduler

        Returns:
            The wrapped script
        """
        options = {
            key[4:]: val
            for key, val in scheduler.config.items()
            if key.startswith("sge_")
        }
        qsub_options = {
            key[5:]: val
            for key, val in scheduler.config.items()
            if key.startswith("qsub_")
        }
        options.update(qsub_options)
        jobname_prefix = scheduler.config.get(
            "scheduler_jobprefix", scheduler.name
        )
        options["N"] = f"{jobname_prefix}.{self.index}"
        options["cwd"] = True
        options["o"] = self.stdout_file
        options["e"] = self.stderr_file

        options_list = []
        for key, val in options.items():
            if val is True:
                options_list.append(f"#$ -{key}")
            elif isinstance(val, (tuple, list)):
                for optval in val:
                    options_list.append(f"#$ -{key} {optval}")
            else:
                options_list.append(f"#$ -{key} {val}")
        options_str = "\n".join(options_list)

        return self.CMD_WRAPPER_TEMPLATE.format(
            shebang=f"#!{self.CMD_WRAPPER_SHELL}\n{options_str}\n",
            prescript=scheduler.config.prescript,
            postscript=scheduler.config.postscript,
            job=self,
            status=JobStatus,
        )


class SgeScheduler(Scheduler):
    """The sge scheduler

    Attributes:
        name: The name of the scheduler
        job_class: The job class

    Args:
        qsub: path to qsub command
        qstat: path to qstat command
        qdel: path to qdel command
        sge_*: SGE options for qsub. List or tuple options will be expanded.
            For example: `sge_l=['hvmem=2G', 'gpu=1']` will be expaned into
            `-l h_vmem=2G -l gpu=1`
        ... other Scheduler args
    """

    name: str = "sge"
    job_class: Type[Job] = SgeJob

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.qsub = self.config.get("qsub", "qsub")
        self.qdel = self.config.get("qdel", "qdel")
        self.qstat = self.config.get("qstat", "qstat")

    async def submit_job(self, job: Job) -> str:
        """Submit a job to SGE

        Args:
            job: The job

        Returns:
            The job id
        """
        proc = await asyncio.create_subprocess_exec(
            self.qsub,
            str(await job.wrapped_script(self)),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        # Your job 613815 (...) has been submitted
        return stdout.decode().split()[2]

    async def kill_job(self, job: Job):
        """Kill a job on SGE

        Args:
            job: The job
        """
        proc = await asyncio.create_subprocess_exec(
            self.qdel,
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
            self.qstat,
            "-j",
            jid,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        await proc.wait()
        return proc.returncode == 0
