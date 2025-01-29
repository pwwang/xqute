"""The scheduler to run jobs on SGE"""
import asyncio
import shlex
from typing import Type

from ..job import Job
from ..scheduler import Scheduler
from ..utils import runnable


class SgeJob(Job):
    """SGE job"""

    def wrap_script(self, scheduler: Scheduler) -> str:
        """Make the shebang with options

        Args:
            scheduler: The scheduler

        Returns:
            The shebang with options
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
        jobname_prefix = scheduler.config.get("jobname_prefix", scheduler.name)
        options["N"] = f"{jobname_prefix}.{self.index}"
        options["cwd"] = True
        # options["o"] = self.stdout_file
        # options["e"] = self.stderr_file

        options_list = []
        for key, val in options.items():
            if val is True:
                options_list.append(f"#$ -{key}")
            elif isinstance(val, (tuple, list)):
                for optval in val:
                    options_list.append(f"#$ -{key} {optval}")
            else:
                options_list.append(f"#$ -{key} {val}")

        script = [
            "#!" + " ".join(map(shlex.quote, scheduler.script_wrapper_lang))
        ]
        script.extend(options_list)
        script.append("")
        script.append("set -u -e -E -o pipefail")
        script.append("")
        script.append("# BEGIN: setup script")
        script.append(scheduler.setup_script)
        script.append("# END: setup script")
        script.append("")
        script.append(self.launch(scheduler))
        return "\n".join(script)


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

    __slots__ = Scheduler.__slots__ + ("qsub", "qdel", "qstat")

    def __init__(self, *args, **kwargs):
        self.qsub = kwargs.pop("qsub", "qsub")
        self.qdel = kwargs.pop("qdel", "qdel")
        self.qstat = kwargs.pop("qstat", "qstat")
        super().__init__(*args, **kwargs)

    async def submit_job(self, job: Job) -> str:
        """Submit a job to SGE

        Args:
            job: The job

        Returns:
            The job id
        """
        proc = await asyncio.create_subprocess_exec(
            self.qsub,
            runnable(job.wrapped_script(self)),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        if proc.returncode != 0:  # pragma: no cover
            raise RuntimeError(f"Can't submit job to SGE: {stderr.decode()}")

        # Your job 613815 (...) has been submitted
        try:
            job_id = stdout.decode().split()[2]
        except Exception:  # pragma: no cover
            raise RuntimeError("Can't get job id from qsub output.", stdout, stderr)

        return job_id

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
            jid = job.jid_file.read_text().strip()
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
        return await proc.wait() == 0
