"""The scheduler to run jobs on SGE"""
import asyncio
import hashlib
import shlex
from typing import Type

from cloudpathlib import CloudPath

from ..job import Job
from ..scheduler import Scheduler
from ..defaults import (
    JobStatus,
    JOBCMD_WRAPPER_LANG,
    JOBCMD_WRAPPER_TEMPLATE,
)
from ..utils import localize
from ..plugin import plugin


class SgeJob(Job):
    """SGE job"""

    def wrap_script(self, scheduler: Scheduler) -> str:
        """Make the shebang with options

        Args:
            scheduler: The scheduler

        Returns:
            The shebang with options
        """
        options = scheduler.config.copy()
        sha = hashlib.sha256(str(scheduler.workdir).encode()).hexdigest()[:8]
        options["N"] = f"{scheduler.jobname_prefix}-{sha}-{self.index}"
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

        jobcmd_init = plugin.hooks.on_jobcmd_init(scheduler, self)
        jobcmd_prep = plugin.hooks.on_jobcmd_prep(scheduler, self)
        jobcmd_end = plugin.hooks.on_jobcmd_end(scheduler, self)
        shebang = " ".join(map(shlex.quote, JOBCMD_WRAPPER_LANG)) + "\n"
        shebang += "\n".join(options_list) + "\n"
        return JOBCMD_WRAPPER_TEMPLATE.format(
            shebang=shebang,
            status=JobStatus,
            job=self,
            jobcmd_init="\n".join(jobcmd_init),
            jobcmd_prep="\n".join(jobcmd_prep),
            jobcmd_end="\n".join(jobcmd_end),
            cmd=shlex.join(self.cmd),
            prescript=scheduler.prescript,
            postscript=scheduler.postscript,
            keep_jid_file=False,
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
        ...: other Scheduler args. List or tuple options will be expanded.
            For example: `sge_l=['hvmem=2G', 'gpu=1']` will be expaned into
            `-l h_vmem=2G -l gpu=1`
    """

    name: str = "sge"
    job_class: Type[Job] = SgeJob

    __slots__ = Scheduler.__slots__ + ("qsub", "qdel", "qstat")

    def __init__(self, *args, **kwargs):
        self.qsub = kwargs.pop("qsub", "qsub")
        self.qdel = kwargs.pop("qdel", "qdel")
        self.qstat = kwargs.pop("qstat", "qstat")
        super().__init__(*args, **kwargs)
        if isinstance(self.workdir, CloudPath):
            raise ValueError("'local' scheduler does not support cloud path as workdir")

    async def submit_job(self, job: Job) -> str:
        """Submit a job to SGE

        Args:
            job: The job

        Returns:
            The job id
        """
        proc = await asyncio.create_subprocess_exec(
            self.qsub,
            localize(job.wrapped_script(self)),
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
        except FileNotFoundError:  # pragma: no cover
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
