"""Default settings and utilities for xqute

Attributes:
    DEFAULT_WORKDIR: The default work directory for jobs to save the metadata
    DEFAULT_ERROR_STRATEGY: The default strategy when there is
        error happened
    DEFAULT_NUM_RETRIES: Default number of retries when
        DEFAULT_ERROR_STRATEGY is retry
    DEFAULT_JOB_CMD_WRAPPER_SHELL: The default shell for job wrapper
    DEFAULT_SCHEDULER_FORKS: Default number of job forks for scheduler
"""

from __future__ import annotations

import asyncio
import textwrap
from typing import Tuple
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class JobErrorStrategy:
    """The strategy when error happen from jobs

    Attributes:
        IGNORE: ignore and run next jobs
        RETRY: retry the job
        HALT: halt the whole program
    """

    IGNORE: str = "ignore"
    RETRY: str = "retry"
    HALT: str = "halt"


class JobStatus:
    """The status of a job

    Life cycles:
    ........................queued in scheduler
    INIT -> QUEUED -> SUBMITTED -> RUNNING -> FINISHED (FAILED)
    INIT -> QUEUED -> SUBMITTED -> RUNNING -> KILLING -> FINISHED
    INIT -> QUEUED -> SUBMITTED -> KILLING -> FINISHED
    INIT -> QUEUED -> (CANCELLED)

    Note that RETRYING is a transient status used when a job is being retried.
    RUNNING, FINISHED and FAILED are the statuses that are polled from the scheduler.
    They can not be set directly by xqute; they are set in the job wrapper script.

    Attributes:
        INIT: When a job is initialized
        RETRYING: When a job is to be retried
        QUEUED: When a job is queued
        SUBMITTED: When a job is submitted
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
        return ret_tuple[0]


LOGGER_NAME = "XQUTE"
LOGGER_LEVEL = "INFO"

DEFAULT_SCHEDULER_FORKS: int = 1
DEFAULT_WORKDIR = "./.xqute"
DEFAULT_ERROR_STRATEGY: str = JobErrorStrategy.IGNORE
DEFAULT_NUM_RETRIES: int = 3
DEFAULT_SUBMISSION_BATCH: int = 8
DEFAULT_CLOUD_FSPATH: str = "/tmp/xqute_cloud"
# DEFAULT_SUBMISSION_BATCH: int = 8
JOBCMD_WRAPPER_LANG: str = "/bin/bash"

# Sleep intervals (in seconds)
# When producer hits max forks
SLEEP_INTERVAL_PRODUCER_MAX_FORKS: float = 1.0
# Polling interval for job status
SLEEP_INTERVAL_POLLING_JOBS: float = 1.0
# Polling interval for keep_feeding mode
SLEEP_INTERVAL_KEEP_FEEDING: float = 0.1
# Wait after job submission to ensure process is running
# Wait for cloud file existence check
SLEEP_INTERVAL_CLOUD_FILE_CHECK: float = 2.0
# Wait for GBatch status check
SLEEP_INTERVAL_GBATCH_STATUS_CHECK: float = 1.0
JOBCMD_WRAPPER_TEMPLATE: str = r"""#!{shebang}
set -x -u -E -o pipefail
# exec >"{job.metadir.mounted}/job.wrapped.log" 2>&1
# TODO: make it work for cloud workdir

{scheduler.jobcmd_wrapper_init}

update_metafile "{status.RUNNING}" "{job.status_file.mounted}"
update_metafile "" "{job.stdout_file.mounted}"

# plugins.on_jobcmd_init
{jobcmd_init}


cleanup() {{
    rc=$?
    update_metafile "$rc" "{job.rc_file.mounted}"
    if [[ $rc -eq 0 ]]; then
        update_metafile "{status.FINISHED}" "{job.status_file.mounted}"
    else
        update_metafile "{status.FAILED}" "{job.status_file.mounted}"
    fi

    remove_metafile "{job.jid_file.mounted}"

    # postscript
    {scheduler.postscript}

    # plugins.on_jobcmd_end
    {jobcmd_end}

    exit $rc
}}

# register trap
trap "cleanup" EXIT

# prescript
{scheduler.prescript}

cmd=$(compose_cmd "{cmd}" "{job.stdout_file.mounted}" "{job.stderr_file.mounted}")

# plugins.on_jobcmd_prep
{jobcmd_prep}

# Run the command, the real job
eval "$cmd"
"""  # noqa: E501


def get_jobcmd_wrapper_init(local: bool) -> str:
    """Get the job command wrapper initialization script

    Args:
        local: Whether the job is running locally

    Returns:
        The job command wrapper initialization script
    """
    if local:
        rm_file = 'mv "$file" "${file}.used"'
        return textwrap.dedent(
            f"""
            export META_ON_CLOUD=0

            update_metafile() {{
                local content=$1
                local file=$2
                echo "$content" > "$file"
            }}

            remove_metafile() {{
                local file=$1
                {rm_file}
            }}

            compose_cmd() {{
                local cmd=$1
                local stdout_file=$2
                local stderr_file=$3
                echo "$cmd 1>$stdout_file 2>$stderr_file"
            }}
            """
        )
    else:
        rm_file = 'cloudsh mv "$file" "${file}_used"'
        return textwrap.dedent(
            f"""
            export META_ON_CLOUD=1

            # Check if cloudsh is installed
            if ! command -v cloudsh &> /dev/null; then
                echo "cloudsh is not installed to support cloud workdir, please install it first" 1>&2
                exit 1
            fi

            update_metafile() {{
                local content=$1
                local file=$2
                echo "$content" | cloudsh sink "$file"
            }}

            remove_metafile() {{
                local file=$1
                {rm_file}
            }}

            compose_cmd() {{
                local cmd=$1
                local stdout_file=$2
                local stderr_file=$3
                # create temp files to save stderr
                stderrtmp=$(mktemp)
                echo "$cmd 2>$stderrtmp | cloudsh sink $stdout_file; \\
                    rc=\\$?; \\
                    cloudsh mv $stderrtmp $stderr_file; \\
                    exit \\$rc"
            }}
            """  # noqa: E501
        )
