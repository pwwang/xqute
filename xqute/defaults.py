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
"""
from pathlib import Path
from .utils import JobErrorStrategy

# pylint: disable=invalid-name

DEFAULT_JOB_METADIR: Path = Path('./.xqute')
DEFAULT_JOB_METADIR.mkdir(parents=True, exist_ok=True)
DEFAULT_JOB_ERROR_STRATEGY: str = JobErrorStrategy.IGNORE
DEFAULT_JOB_NUM_RETRIES: int = 3
DEFAULT_JOB_CMD_WRAPPER_SHELL: str = '/bin/bash'
DEFAULT_JOB_CMD_WRAPPER_TEMPLATE: str = r"""{shebang}

# Trap command to capture status, rc
# And remove lock file
cleanup() {{
    rc=$?
    echo $rc > {job.rc_file}
    if [[ $rc -eq 0 ]]; then
        echo {status.FINISHED} > {job.status_file}
    else
        echo {status.FAILED} > {job.status_file}
    fi
    rm -f {job.lock_file}
    exit $rc
}}
trap "cleanup" EXIT

# Update job status
echo {status.RUNNING} > {job.status_file}

# Pre-command place holder
## XQUTE PRE-COMMAND ##

# Run the command
{job.strcmd} \
    1>{job.stdout_file} \
    2>{job.stderr_file}

# Post-command place holder
## XQUTE POST-COMMAND ##
"""

DEFAULT_SCHEDULER_FORKS: int = 1
