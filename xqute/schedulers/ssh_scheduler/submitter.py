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
    sys.stdout.write(f"{server}/{proc.pid}")
    if stderr:
        sys.stderr.write(f"STDOUT: {stdout}\nSTDERR: {stderr}")
        # Do not wait for the return code, as we want it to keep running
        # for the real job
        rc = 1
    else:
        rc = 0

    sys.exit(rc)
