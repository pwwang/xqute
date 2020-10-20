"""The scheduler to run jobs on SGE"""
import asyncio
from typing import Type
from ..job import Job
from ..scheduler import Scheduler
from ..utils import a_write_text, a_read_text, JobStatus

class SgeJob(Job):
    """SGE job"""
    async def wrap_cmd(self, scheduler: Scheduler) -> None:
        """Wrap the command to enable status, returncode, cleaning when
        job exits

        Args:
            scheduler: The scheduler
        """
        wrapt_script = self.wrapped_script(scheduler)
        options = {key[4:]: val for key, val in scheduler.config.items()
                   if key.startswith('sge_')}
        options['N'] = self.name(scheduler)
        options['cwd'] = True
        options['o'] = self.stdout_file
        options['e'] = self.stderr_file

        options_list = []
        for key, val in options.items():
            if val is True:
                options_list.append(f"#$ -{key}")
            elif isinstance(val, (tuple, list)):
                for optval in val:
                    options_list.append(f"#$ -{key} {optval}")
            else:
                options_list.append(f"#$ -{key} {val}")
        options_str = '\n'.join(options_list)

        await a_write_text(wrapt_script, self.CMD_WRAPPER_TEMPLATE.format(
            shebang=f'#!{self.CMD_WRAPPER_SHELL}\n{options_str}\n',
            job=self,
            status=JobStatus
        ))

        self._wrapped_cmd = wrapt_script

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
    name: str = 'sge'
    job_class: Type[Job] = SgeJob

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.qsub = self.config.get('qsub', 'qsub')
        self.qdel = self.config.get('qdel', 'qdel')
        self.qstat = self.config.get('qstat', 'qstat')

    async def submit_job(self, job: Job) -> str:
        """Submit a job to SGE

        Args:
            job: The job

        Returns:
            The job id
        """
        proc = await asyncio.create_subprocess_exec(
            self.qsub, str(job.wrapped_cmd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
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
            self.qdel, job.uid,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await proc.wait()

    async def job_is_running(self, job: Job) -> bool:
        """Tell if a job is really running, not only the job.lock_file

        In case where the lockfile is not cleaned when job is done.

        Args:
            job: The job

        Returns:
            True if it is, otherwise False
        """
        try:
            uid = await a_read_text(job.lock_file)
        except FileNotFoundError:
            return False

        proc = await asyncio.create_subprocess_exec(
            self.qstat, '-j', uid,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await proc.wait()
        return proc.returncode == 0
