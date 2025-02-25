"""The scheduler to run jobs on SGE"""

import asyncio
import hashlib

from ..job import Job
from ..scheduler import Scheduler


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

    __slots__ = Scheduler.__slots__ + ("qsub", "qdel", "qstat")

    def __init__(self, *args, **kwargs):
        self.qsub = kwargs.pop("qsub", "qsub")
        self.qdel = kwargs.pop("qdel", "qdel")
        self.qstat = kwargs.pop("qstat", "qstat")
        super().__init__(*args, **kwargs)

    def jobcmd_shebang(self, job) -> str:
        options = self.config.copy()
        sha = hashlib.sha256(str(self.workdir).encode()).hexdigest()[:8]
        options["N"] = f"{self.jobname_prefix}-{sha}-{job.index}"
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

        return super().jobcmd_shebang(job) + "\n" + "\n".join(options_list)

    async def submit_job(self, job: Job) -> str:
        """Submit a job to SGE

        Args:
            job: The job

        Returns:
            The job id
        """
        proc = await asyncio.create_subprocess_exec(
            self.qsub,
            self.wrapped_job_script(job).fspath,
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
