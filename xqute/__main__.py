"""Provides the command-line interface to launch a job.

This is running on the scheduler system.
xqute requires the scheduler system to have this package installed.

It will do two tasks:

1. Check if we have the same version of xqute and plugins installed.
2. Launch the job.

It's executed in the wrapped script of a job while assembled by Job.launch().
"""

from __future__ import annotations

import asyncio
import sys
import traceback
from typing import TYPE_CHECKING
from warnings import warn

from argx import ArgumentParser
from cloudpathlib import AnyPath

from . import __version__
from .defaults import JobStatus
from .plugin import plugin

if TYPE_CHECKING:  # pragma: no cover
    from argx import Namespace


class SchedulerXquteInconsistencyWarning(UserWarning):
    """Warning for xqute inconsistency on the scheduler system"""


def _update_status(metadir: str, status: JobStatus) -> None:
    """Update the job status

    Args:
        metadir: The metadir of the job
        status: The status
    """
    AnyPath(metadir).joinpath("job.status").write_text(str(status))


def check_version_and_plugins(args: Namespace) -> None:
    """Check the version and plugins

    Args:
        args: The arguments
    """
    if args.version != __version__:
        warn(
            f"The xqute on the scheduler system is {__version__}, "
            f"but the job is created with {args.version}.",
            SchedulerXquteInconsistencyWarning,
        )

    sched_plugins = plugin.get_enabled_plugins()
    for plug in (args.plugin or ()):
        name, version = plug.split(":")
        if name not in sched_plugins:
            warn(
                f"Xqute plugin {name!r} is not installed or enabled "
                "on the scheduler system.",
                SchedulerXquteInconsistencyWarning,
            )
        elif sched_plugins[name] != version:
            warn(
                f"Xqute plugin version mismatch: {name} {version} != "
                f"{sched_plugins[name]}",
                SchedulerXquteInconsistencyWarning,
            )


async def launch_job(args: Namespace) -> None:
    """Launch the job

    Args:
        args: The arguments
    """

    await plugin.hooks.on_jobsched_started(args)

    stdout_file = AnyPath(args.metadir).joinpath("job.stdout")
    stderr_file = AnyPath(args.metadir).joinpath("job.stderr")
    stdout_file.touch()
    stderr_file.touch()
    rc_file = AnyPath(args.metadir).joinpath("job.rc")
    try:
        p = await asyncio.create_subprocess_exec(
            *args.cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await p.communicate()
    except asyncio.CancelledError:
        _update_status(args.metadir, JobStatus.FAILED)
        rc = 1
        rc_file.write_text(str(rc))
        stderr_file.write_text("Job was cancelled.\n")
        raise
    except Exception:
        _update_status(args.metadir, JobStatus.FAILED)
        rc = 1
        rc_file.write_text(str(rc))
        stderr_file.write_text(traceback.format_exc())
    else:
        rc = p.returncode
        stdout_file.write_bytes(stdout)
        stderr_file.write_bytes(stderr)
        rc_file.write_text(str(rc))

        if rc == 0:
            _update_status(args.metadir, JobStatus.FINISHED)
        else:
            _update_status(args.metadir, JobStatus.FAILED)
    finally:
        AnyPath(args.metadir).joinpath("job.jid").unlink(missing_ok=True)

    await plugin.hooks.on_jobsched_ended(args, rc)

    if rc != 0:
        sys.exit(rc)


async def main(args) -> None:
    """The main function

    Args:
        args: The arguments
    """

    # Update the job status to RUNNING
    _update_status(args.metadir, JobStatus.RUNNING)

    # Check the version and plugins
    check_version_and_plugins(args)

    # Launch the job
    await launch_job(args)


if __name__ == "__main__":
    parser = ArgumentParser(description=__doc__, prefix_chars="+")
    parser.add_argument("++metadir", required=True, help="The metadir of the job")
    parser.add_argument("++scheduler", required=True, help="The scheduler name")
    parser.add_argument(
        "++version",
        required=True,
        help="The version of xqute with which the job is created",
    )
    parser.add_argument(
        "++plugin",
        action="clear_append",
        help="The enabled plugins with their versions",
    )
    parser.add_argument(
        "++cmd", action="clear_extend", nargs="+", help="The command of the job"
    )

    args = parser.parse_args()
    asyncio.run(main(args))
