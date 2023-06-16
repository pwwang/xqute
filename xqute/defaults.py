"""Default settings and utilities for xqute

Attributes:
    DEFAULT_JOB_METADIR: The default meta directory for jobs
    DEFAULT_JOB_ERROR_STRATEGY: The default strategy when there is
        error happened
    DEFAULT_JOB_NUM_RETRIES: Default number of retries when
        DEFAULT_JOB_ERROR_STRATEGY is retry
    DEFAULT_JOB_CMD_WRAPPER_SHELL: The default shell for job wrapper
    DEFAULT_JOB_CMD_WRAPPER_TEMPLATE: The template for job cmd wrapping
    DEFAULT_SCHEDULER_FORKS: Default number of job forks for scheduler
    DEFAULT_JOB_SUBMISSION_BATCH: Default consumer workers
"""
from __future__ import annotations

from pathlib import Path
from typing import Tuple
import uvloop

uvloop.install()


class JobErrorStrategy:
    """The strategy when error happen from jobs

    Attributes:
        IGNORE: ignore and run next jobs
        RETRY: retry the job
        HALT: halt the whole program
    """
    IGNORE: str = 'ignore'
    RETRY: str = 'retry'
    HALT: str = 'halt'


class JobStatus:
    """The status of a job

    Life cycles:
    ........................queued in scheduler
    INIT -> QUEUED -> SUBMITTED -> RUNNING -> FINISHED (FAILED)
    INIT -> QUEUED -> SUBMITTED -> RUNNING -> KILLING -> FINISHED
    INIT -> QUEUED -> SUBMITTED -> KILLING -> FINISHED
    INIT -> QUEUED -> (CANELED)

    Attributes:
        INIT: When a job is initialized
        RETRYING: When a job is to be retried
        QUEUED: When a job is queued
        SUBMITTED: When a job is sumitted
        RUNNING: When a job is running
        KILLING: When a job is being killed
        FINISHED: When a job is finished
        FAILED: When a job is failed
    """
    INIT: int = 0
    RETRYING: int = 1
    QUEUED: int = 2
    SUBMITTED: int = 3
    RUNNING: int = 4
    KILLING: int = 5
    FINISHED: int = 6
    FAILED: int = 7

    @classmethod
    def get_name(cls, *statuses: int) -> Tuple[str, ...] | str:
        """Get the name of the status

        Args:
            *statuses: The status values

        Returns:
            The name of the status if a single status is passed, otherwise
            a tuple of names
        """
        ret_dict = {}
        for name, value in cls.__dict__.items():
            if value in statuses:
                ret_dict[value] = name
        ret_tuple = tuple(ret_dict[status] for status in statuses)
        if len(ret_tuple) > 1:
            return ret_tuple
        return ret_tuple[0]  # pragma: no cover


LOGGER_NAME = 'XQUTE'

DEFAULT_JOB_METADIR: Path = Path('./.xqute')
DEFAULT_JOB_ERROR_STRATEGY: str = JobErrorStrategy.IGNORE
DEFAULT_JOB_NUM_RETRIES: int = 3
DEFAULT_JOB_SUBMISSION_BATCH: int = 8
DEFAULT_JOB_CMD_WRAPPER_SHELL: str = '/bin/bash'
DEFAULT_JOB_CMD_WRAPPER_TEMPLATE: str = r"""{shebang}

set -u -e -E -o pipefail

# Trap command to capture status, rc
# And remove job id file
cleanup() {{
    rc=$?
    echo $rc > {job.rc_file}
    if [[ $rc -eq 0 ]]; then
        echo {status.FINISHED} > {job.status_file}
    else
        echo {status.FAILED} > {job.status_file}
    fi
    rm -f {job.jid_file}
    exit $rc
}}
trap "cleanup" EXIT

# Update job status
echo {status.RUNNING} > {job.status_file}

# Pre-command place holder
{prescript}

# Run the command
{job.strcmd} \
    1>{job.stdout_file} \
    2>{job.stderr_file}

# Post-command place holder
{postscript}
"""

DEFAULT_SCHEDULER_FORKS: int = 1
