"""This script is used to wrap the command for the scheduler to submit and run

It is used by the ssh scheduler to submit jobs to remote servers
and print the pid of the job to stdout.
The real command is run in a subprocess without waiting for the results.

The script is executed by the scheduler, not the user. And it's not imported
by xqute directly.

Find a way to pass envs?
"""

import os
import sys
import time
import subprocess

if __name__ == "__main__":
    server, cwd, *cmds = sys.argv[1:]

    proc = subprocess.Popen(
        cmds,
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=os.setpgrp,
    )
    stdout = proc.stdout.read().decode()
    stderr = proc.stderr.read().decode()
    sys.stdout.write(f"{proc.pid}@{server}")
    # wait for a while to make sure the process is running
    time.sleep(0.1)
    rc = proc.poll()
    if rc is None or rc == 0:
        # still running or already finished
        sys.exit(0)
    else:  # pragma: no cover
        sys.stderr.write(f"STDOUT: {stdout}\nSTDERR: {stderr}")
        sys.exit(rc)
