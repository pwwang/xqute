"""Hook specifications for scheduler plugins"""

from __future__ import annotations

import signal
from typing import TYPE_CHECKING
from simplug import Simplug, SimplugResult  # type: ignore

if TYPE_CHECKING:  # pragma: no cover
    from .xqute import Xqute
    from .job import Job
    from .scheduler import Scheduler


plugin = Simplug("xqute")


@plugin.spec
def on_init(xqute: Xqute):
    """When xqute is initialized

    Note that this hook will run at the same time when producer and consumer
    start. So they are not ensured to be started at this point.

    Args:
        xqute: The xqute object
    """


@plugin.spec(result=SimplugResult.TRY_ALL_FIRST_AVAIL)
def on_shutdown(xqute: Xqute, sig: signal.Signals | None):
    """When xqute is shutting down

    Return False to stop shutting down, but you have to shut it down
    by yourself, for example, `xqute.task.cancel()`

    Only the first return value will be used.

    Args:
        xqute: The xqute object
        sig: The signal. `None` means a natural shutdown
    """


@plugin.spec
async def on_job_init(scheduler: Scheduler, job: Job):
    """When the job is initialized

    Args:
        scheduler: The scheduler object
        job: The job object
    """


@plugin.spec
async def on_job_queued(scheduler: Scheduler, job: Job):
    """When the job is queued

    Args:
        scheduler: The scheduler object
        job: The job object
    """


@plugin.spec(result=SimplugResult.TRY_ALL_FIRST_AVAIL)
async def on_job_submitting(scheduler: Scheduler, job: Job):
    """When the job is to be submitted

    Return False to cancel submitting. Only the first return value is used.

    Args:
        scheduler: The scheduler object
        job: The job object
    """


@plugin.spec
async def on_job_submitted(scheduler: Scheduler, job: Job):
    """When the job is submitted

    Args:
        scheduler: The scheduler object
        job: The job object
    """


@plugin.spec
async def on_job_started(scheduler: Scheduler, job: Job):
    """When the job starts to run.

    Note that this is not when exactly the job starts to run, but when the
    scheduler starts to wait for the job to finish. So this hook is not
    suitable for measuring the time of the job.

    Args:
        scheduler: The scheduler object
        job: The job object
    """


@plugin.spec
async def on_job_polling(scheduler: Scheduler, job: Job):
    """When the system is polling job status

    Args:
        scheduler: The scheduler object
        job: The job object
    """


@plugin.spec(result=SimplugResult.TRY_ALL_FIRST_AVAIL)
async def on_job_killing(scheduler: Scheduler, job: Job):
    """When the job is being killed

    Return False to stop killing the job.

    Args:
        scheduler: The scheduler object
        job: The job object
    """


@plugin.spec
async def on_job_killed(scheduler: Scheduler, job: Job):
    """When the job is killed

    Args:
        scheduler: The scheduler object
        job: The job object
    """


@plugin.spec
async def on_job_failed(scheduler: Scheduler, job: Job):
    """When the job is failed

    Args:
        scheduler: The scheduler object
        job: The job object
    """


@plugin.spec
async def on_job_succeeded(scheduler: Scheduler, job: Job):
    """When the job is succeeded

    Args:
        scheduler: The scheduler object
        job: The job object
    """


@plugin.spec(result=SimplugResult.ALL_AVAILS)
def on_jobcmd_init(scheduler: Scheduler, job: Job) -> str:
    """When the job command wrapper script is initialized before the prescript is run
    This should return a piece of bash code to be inserted in the wrapped job
    script (template), which is a python template string, with the following
    variables available: `status` and `job`. `status` is the class `JobStatus` from
    `xqute.defaults.py` and `job` is the `Job` instance.
    For multiple plugins, the code will be inserted in the order of the plugin priority.
    Args:
        scheduler: The scheduler object
        job: The job object
    Returns:
        The bash code to be inserted
    """


@plugin.spec(result=SimplugResult.ALL_AVAILS)
def on_jobcmd_prep(scheduler: Scheduler, job: Job) -> str:
    """When the job command right about to be run
    This should return a piece of bash code to be inserted in the wrapped job
    script (template), which is a python template string, with the following
    variables available: `status` and `job`. `status` is the class `JobStatus` from
    `xqute.defaults.py` and `job` is the `Job` instance.
    The bash variable `$cmd` is accessible in the context. It is also possible to
    modify the `cmd` variable. Just remember to assign the modified value to `cmd`.
    For multiple plugins, the code will be inserted in the order of the plugin priority.
    Keep in mind that the `$cmd` may be modified by other plugins.
    Args:
        scheduler: The scheduler object
        job: The job object
    Returns:
        The bash code to be inserted
    """


@plugin.spec(result=SimplugResult.ALL_AVAILS)
def on_jobcmd_end(scheduler: Scheduler, job: Job) -> str:
    """When the job command finishes and after the postscript is run
    This should return a piece of bash code to be inserted in the wrapped job
    script (template), which is a python template string, with the following
    variables available: `status` and `job`. `status` is the class `JobStatus` from
    `xqute.defaults.py` and `job` is the `Job` instance.
    The bash variable `$rc` is accessible in the context, which is the return code
    of the job command.
    For multiple plugins, the code will be inserted in the order of the plugin priority.
    Args:
        scheduler: The scheduler object
        job: The job object
    Returns:
        The bash code to be inserted
    """


plugin.load_entrypoints()
