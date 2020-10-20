"""Hook specifications for scheduler plugins"""
from simplug import Simplug

# pylint: disable=unused-argument,invalid-name

simplug = Simplug('xqute')

@simplug.spec
async def on_init(scheduler: "Scheduler"):
    """Right after scheduler object is initialized

    Args:
        scheduler: The scheduler object
    """


@simplug.spec
async def on_shutdown(scheduler: "Scheduler", consumer: "Consumer"):
    """When scheduler is shutting down

    Return False to stop shutting down, but you have to shut it down
    by yourself, for example, scheduler.kill_running_jobs()

    Args:
        scheduler: The scheduler object
        sig: The signal
    """

@simplug.spec
async def on_job_init(scheduler: "Scheduler", job: "Job"):
    """When the job is initialized

    Args:
        scheduler: The scheduler object
        job: The job object
    """

@simplug.spec
async def on_job_queued(scheduler: "Scheduler", job: "Job"):
    """When the job is queued

    Args:
        scheduler: The scheduler object
        job: The job object
    """

@simplug.spec
async def on_job_submitted(scheduler: "Scheduler", job: "Job"):
    """When the job is submitted

    Args:
        scheduler: The scheduler object
        job: The job object
    """

@simplug.spec
async def on_job_killing(scheduler: "Scheduler", job: "Job"):
    """When the job is being killed

    Return False to stop killing the job.

    Args:
        scheduler: The scheduler object
        job: The job object
    """

@simplug.spec
async def on_job_killed(scheduler: "Scheduler", job: "Job"):
    """When the job is killed

    Args:
        scheduler: The scheduler object
        job: The job object
    """

@simplug.spec
async def on_job_failed(scheduler: "Scheduler", job: "Job"):
    """When the job is failed

    Args:
        scheduler: The scheduler object
        job: The job object
    """

@simplug.spec
async def on_job_succeeded(scheduler: "Scheduler", job: "Job"):
    """When the job is succeeded

    Args:
        scheduler: The scheduler object
        job: The job object
    """

@simplug.spec
async def on_complete(scheduler: "Scheduler"):
    """When all jobs complete

    Args:
        scheduler: The scheduler object
    """

priority = 99 # pylint: disable=invalid-name

# pylint: disable=function-redefined
@simplug.impl
async def on_job_failed(scheduler: "Scheduler", job: "Job"):
    """When the job is failed. Have to make sure it's only called once

    Because we don't have a trigger in memory to determine this status change.
    It is obtained by polling.

    Args:
        scheduler: The scheduler object
        job: The job object
    """
    job.hook_done = True

@simplug.impl
async def on_job_succeeded(scheduler: "Scheduler", job: "Job"):
    """When the job is succeeded. Have to make sure it's only called once

    Because we don't have a trigger in memory to determine this status change.
    It is obtained by polling.

    Args:
        scheduler: The scheduler object
        job: The job object
    """
    job.hook_done = True

simplug.register(__name__)
