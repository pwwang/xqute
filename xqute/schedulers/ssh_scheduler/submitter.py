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
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        preexec_fn=os.setpgrp,
    )
    print(f"{server}/{proc.pid}")
