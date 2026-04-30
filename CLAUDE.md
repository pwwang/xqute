# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

xqute is an async-first Python job management framework. It schedules, submits, monitors, and manages batch jobs across multiple backends (local, SGE, Slurm, SSH, Google Cloud Batch, Docker/Podman/Apptainer). See [AGENTS.md](AGENTS.md) for commands, code style, and testing conventions.

## Architecture

### Producer-consumer core ([xqute/xqute.py](xqute/xqute.py))

`Xqute` is the main entry point. Internally it runs three concurrent asyncio tasks:

1. **Producer** — pulls jobs from a `deque` buffer, checks fork limits via `scheduler.count_running_jobs()`, enqueues into `asyncio.Queue`.
2. **Consumers** (N = `submission_batch`) — pull from the queue, call `scheduler.submit_job_and_update_status()`.
3. **Polling** — periodically reads each job's `job.status` file to detect state transitions.

Two modes: `run_until_complete()` blocks until all jobs finish; `run_until_complete(keep_feeding=True)` returns immediately and `stop_feeding()` waits for completion.

### File-based status tracking (critical design decision)

Jobs DO NOT report status through scheduler APIs. Instead, every job runs inside a **bash wrapper script** (template in [xqute/defaults.py](xqute/defaults.py)) that uses `trap cleanup EXIT` to write status files (`job.status`, `job.stdout`, `job.stderr`, `job.rc`, `job.jid`) into a per-job `metadir` under `workdir`. The polling loop reads these files. This makes the system robust against network failures and scheduler quirks.

### Status state machine ([xqute/defaults.py](xqute/defaults.py), [xqute/scheduler.py](xqute/scheduler.py))

```
INIT -> QUEUED -> SUBMITTED -> RUNNING -> FINISHED
                                    \-> FAILED
INIT -> CANCELLED (sentinel)
RUNNING -> KILLING -> FINISHED
```

`RUNNING`, `FINISHED`, `FAILED` are written by the wrapper script's trap handler, not by xqute code. `transition_job_status()` in the Scheduler base class handles all transitions and ensures `RUNNING` is always visited before terminal states (for hook correctness).

### Scheduler abstraction ([xqute/scheduler.py](xqute/scheduler.py))

Abstract base class. Subclasses must implement: `submit_job()`, `kill_job()`, `job_is_running()`. The base class provides `submit_job_and_update_status()`, `wrap_job_script()`, `transition_job_status()`, `retry_job()`, `kill_job_and_update_status()`, and the polling loop.

Scheduler implementations live in [xqute/schedulers/](xqute/schedulers/). `get_scheduler()` in `__init__.py` resolves by name or class. Notable: `SshScheduler` and `ContainerScheduler` extend `LocalScheduler` (not the base `Scheduler`) because they ultimately run subprocesses.

### Path duality: SpecPath / MountedPath ([xqute/path.py](xqute/path.py))

Every path tracked by xqute has two representations:
- **SpecPath**: path as seen by the orchestrator (local machine)
- **MountedPath**: path as seen by the executor (remote/container)

This enables transparent translation when jobs run on a different machine (SSH, container, cloud VM). Cloud paths (`gs://`, `az://`, `s3://`) auto-download to a local cache on access.

### Plugin hooks ([xqute/plugin.py](xqute/plugin.py))

13 lifecycle hooks via `simplug`. Hooks are sync or async. Some hooks can cancel operations by returning `False` (`on_shutdown`, `on_job_submitting`, `on_job_killing`). `on_jobcmd_init`/`on_jobcmd_prep`/`on_jobcmd_end` inject bash code into the wrapper script.

### Job error strategy

Three options defined in `JobErrorStrategy`:
- `IGNORE` (default): log and continue
- `RETRY`: re-queue up to `num_retries` times
- `HALT`: send SIGTERM to the Xqute process, stopping the entire pipeline

## Key constraints

- Xqute must be initialized inside a running event loop (uses `asyncio.get_running_loop()`).
- `uvloop` is set as the default event loop policy at import time ([xqute/defaults.py](xqute/defaults.py)).
- SSH servers must share the same filesystem and use keyfile authentication.
- Version is declared in both [pyproject.toml](pyproject.toml) and [xqute/__init__.py](xqute/__init__.py) — a pre-commit hook enforces consistency.
