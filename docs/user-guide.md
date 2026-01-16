# User Guide

Comprehensive guide for using Xqute effectively.

## Table of Contents

- [Core Concepts](#core-concepts)
- [Initialization](#initialization)
- [Job Management](#job-management)
- [Execution Modes](#execution-modes)
- [Error Handling](#error-handling)
- [Working Directories](#working-directories)
- [Environment Variables](#environment-variables)
- [Job Output](#job-output)
- [Monitoring](#monitoring)
- [Best Practices](#best-practices)

## Core Concepts

### Xqute

The main class that manages jobs and coordinates their execution. It acts as both a producer and consumer pattern:

- **Producer**: Feeds jobs into the buffer queue
- **Consumer**: Submits jobs to the scheduler
- **Monitor**: Polls job status and handles completion

### Job

Represents a single unit of work with:
- Command to execute
- Metadata directory for logs and status
- Environment variables
- Status tracking

### Scheduler

Abstracts job execution for different backends. Each scheduler implementation handles:
- Job submission
- Status monitoring
- Job cancellation

## Initialization

### Basic Initialization

```python
from xqute import Xqute

# Default: local scheduler, 1 fork
xqute = Xqute()

# Specify concurrency
xqute = Xqute(forks=5)
```

### Full Initialization Options

```python
from xqute import Xqute

xqute = Xqute(
    scheduler='local',           # Scheduler type or class
    plugins=None,                # Plugin list (e.g., ['myplugin'])
    workdir='./.xqute',         # Job metadata directory
    submission_batch=5,          # Parallel job submissions
    error_strategy='retry',       # Error handling strategy
    num_retries=3,               # Max retry attempts
    forks=10,                   # Max concurrent jobs
    scheduler_opts={},            # Scheduler-specific options
    jobname_prefix='myjob',      # Job name prefix
    recheck_interval=10,          # Recheck running jobs every N polls
)
```

### Parameter Details

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `scheduler` | str/Scheduler | `'local'` | Scheduler backend |
| `plugins` | list | `None` | Plugins to enable/disable |
| `workdir` | str/path | `'./.xqute'` | Job metadata directory |
| `submission_batch` | int | `None` | Parallel job submissions |
| `error_strategy` | str | `'retry'` | Error strategy (`'retry'` or `'halt'`) |
| `num_retries` | int | `3` | Max retry attempts |
| `forks` | int | `1` | Max concurrent jobs |
| `scheduler_opts` | dict | `{}` | Scheduler-specific options |
| `jobname_prefix` | str | scheduler name | Job name prefix |
| `recheck_interval` | int | `10` | Recheck interval (in polls) |

## Job Management

### Feeding Jobs

#### Simple Commands

```python
# String command
await xqute.feed('echo "Hello World"')

# List command
await xqute.feed(['echo', 'Hello World'])
```

#### With Environment Variables

```python
await xqute.feed(
    ['bash', '-c', 'echo $MY_VAR'],
    envs={'MY_VAR': 'value'}
)
```

#### Multiple Jobs

```python
for i in range(100):
    await xqute.feed(['python', 'process.py', str(i)])
```

### Job Objects

Create jobs directly:

```python
from xqute import Job

job = Job(
    index=0,
    cmd=['echo', 'test'],
    workdir='./.xqute',
    error_retry=True,
    num_retries=3,
)
await xqute.feed(job)
```

### Job Metadata

Each job has a metadata directory containing:

```
.xqute/
├── 0/                  # Job index
│   ├── job.stdout       # Standard output
│   ├── job.stderr       # Standard error
│   ├── job.status       # Job status
│   ├── job.rc          # Return code
│   ├── job.jid        # Scheduler job ID
│   ├── job.retry/     # Retry history (if retried)
│   └── job.wrapped.local  # Wrapped script
└── 1/
    └── ...
```

## Execution Modes

### Traditional Mode (Default)

Add all jobs first, then wait for completion:

```python
async def main():
    xqute = Xqute(forks=5)

    # Add all jobs
    for i in range(100):
        await xqute.feed(['echo', f'Job {i}'])

    # Wait for all to complete
    await xqute.run_until_complete()
```

### Daemon Mode (Keep Feeding)

Add jobs while running:

```python
async def main():
    xqute = Xqute(forks=5)

    # Start in background
    await xqute.run_until_complete(keep_feeding=True)

    # Add jobs dynamically
    for i in range(100):
        await xqute.feed(['echo', f'Job {i}'])

    # Signal completion and wait
    await xqute.stop_feeding()
```

### Checking Feeding Status

```python
if xqute.is_feeding():
    await xqute.stop_feeding()
```

## Error Handling

### Error Strategies

#### Retry Strategy (Default)

Automatically retry failed jobs:

```python
xqute = Xqute(
    error_strategy='retry',
    num_retries=3,
)
```

When a job fails:
1. Job metadata is backed up to `.xqute/<index>/job.retry/<trial>`
2. Job status is reset to QUEUED
3. Job is resubmitted

#### Halt Strategy

Stop execution on first failure:

```python
xqute = Xqute(
    error_strategy='halt',
)
```

When a job fails:
1. All running jobs are killed
2. No more jobs are submitted
3. Execution terminates with SIGTERM

### Per-Job Error Handling

Override error strategy per job:

```python
from xqute import Job

job = Job(
    index=0,
    cmd=['risky-command'],
    workdir='./.xqute',
    error_retry=False,  # Disable retry for this job
)
```

## Working Directories

### Local Working Directory

```python
xqute = Xqute(workdir='./myjobs')
```

### Cloud Working Directory

```python
xqute = Xqute(
    workdir='gs://my-bucket/jobs',
    scheduler_opts={
        'mounted_workdir': '/mnt/jobs',  # Mount point
    }
)
```

### Mounted Paths

For cloud storage, specify mount point:

```python
xqute = Xqute(
    workdir='gs://bucket/jobs',
    scheduler_opts={
        'mounted_workdir': '/mnt/gs',
    }
)
```

## Environment Variables

### Built-in Variables

Xqute automatically sets these environment variables for each job:

| Variable | Description |
|----------|-------------|
| `XQUTE_JOB_INDEX` | Job index number |
| `XQUTE_METADIR` | Main metadata directory |
| `XQUTE_JOB_METADIR` | Job-specific metadata directory |

### Custom Variables

```python
await xqute.feed(
    ['bash', '-c', 'echo $MY_CUSTOM_VAR'],
    envs={
        'MY_CUSTOM_VAR': 'value',
        'ANOTHER_VAR': 'another value',
    }
)
```

### Accessing in Jobs

```python
await xqute.feed(
    ['python', '-c', '''
import os
print(f"Job index: {os.getenv('XQUTE_JOB_INDEX')}")
print(f"Custom: {os.getenv('MY_VAR')}")
'''],
    envs={'MY_VAR': 'custom'}
)
```

## Job Output

### Reading Output After Completion

```python
async def main():
    xqute = Xqute(forks=3)

    await xqute.feed(['echo', 'Hello'])
    await xqute.run_until_complete()

    # Read output
    job = xqute.jobs[0]
    stdout = await job.stdout_file.a_read_text()
    stderr = await job.stderr_file.a_read_text()
    rc = await job.get_rc()

    print(f"Output: {stdout}")
    print(f"Errors: {stderr}")
    print(f"Return code: {rc}")
```

### Stream Output During Execution

Use plugins to stream output (see [Plugins](plugins.md)).

### Output File Locations

```python
job = xqute.jobs[0]
print(f"stdout: {job.stdout_file}")
print(f"stderr: {job.stderr_file}")
print(f"status: {job.status_file}")
print(f"rc: {job.rc_file}")
```

## Monitoring

### Job Status

```python
from xqute.defaults import JobStatus

status = await job.get_status()

status_name = JobStatus.get_name(status)
print(f"Job status: {status_name}")
```

### Status Values

| Status | Value | Description |
|--------|-------|-------------|
| `INIT` | 0 | Job initialized |
| `QUEUED` | 1 | Job queued for submission |
| `SUBMITTED` | 2 | Job submitted to scheduler |
| `RUNNING` | 3 | Job is running |
| `FINISHED` | 4 | Job completed successfully |
| `FAILED` | 5 | Job failed |
| `KILLING` | 6 | Job is being killed |

### Checking Multiple Jobs

```python
import asyncio

async def check_all_jobs():
    statuses = await asyncio.gather(
        *[job.get_status(refresh=True) for job in xqute.jobs]
    )

    for job, status in zip(xqute.jobs, statuses):
        status_name = JobStatus.get_name(status)
        print(f"Job {job.index}: {status_name}")
```

### Completion Tracking

```python
# Wait for specific job to complete
from xqute.defaults import JobStatus

while True:
    status = await xqute.jobs[0].get_status(refresh=True)
    if status in (JobStatus.FINISHED, JobStatus.FAILED):
        break
    await asyncio.sleep(1)
```

## Best Practices

### 1. Choose Appropriate Concurrency

```python
# Local machine: match CPU cores
import os
xqute = Xqute(forks=os.cpu_count())

# Slurm: match allocated resources
xqute = Xqute(forks=100)
```

### 2. Use Submission Batching

```python
# For high-latency schedulers
xqute = Xqute(
    scheduler='slurm',
    submission_batch=10,  # Submit 10 jobs at once
)
```

### 3. Monitor Resource Usage

```python
# Check running jobs before adding more
if len([j for j in xqute.jobs if j._status == JobStatus.RUNNING]) < 100:
    await xqute.feed(['next-job'])
```

### 4. Handle Large Output

```python
# Write output to files instead of stdout
await xqute.feed([
    'python', 'process.py',
    '--output', f'{job.metadir}/output.txt'
])
```

### 5. Use Descriptive Job Names

```python
xqute = Xqute(
    jobname_prefix='myproject',
    scheduler_opts={'job_name': 'batch1'}
)
```

### 6. Cleanup Old Jobs

```python
# Clean up old metadata
import shutil
if os.path.exists('./.xqute'):
    shutil.rmtree('./.xqute')
```

### 7. Logging Configuration

```python
from xqute.utils import logger
import logging

logger.setLevel(logging.DEBUG)
```

### 8. Error Recovery

```python
try:
    await xqute.run_until_complete()
except Exception as e:
    logger.error(f"Failed: {e}")

    # Check failed jobs
    failed = [
        job for job in xqute.jobs
        if await job.get_status() == JobStatus.FAILED
    ]
    logger.info(f"Failed jobs: {len(failed)}")
```

### 9. Graceful Shutdown

```python
# Signal handler
async def shutdown_handler():
    logger.info("Shutting down...")
    await xqute.stop_feeding()

# Handle Ctrl+C
import signal
loop = asyncio.get_running_loop()
loop.add_signal_handler(signal.SIGINT, lambda: asyncio.create_task(shutdown_handler()))
```

### 10. Cloud Storage Best Practices

```python
# Use local cache for cloud paths
xqute = Xqute(
    workdir='gs://bucket/jobs',
    scheduler_opts={
        'mounted_workdir': '/mnt/gs',
        'cache_dir': '/tmp/cache',  # Local cache
    }
)
```

## Troubleshooting

### Jobs Stuck in SUBMITTED Status

Check if job failed before running:

```bash
# Check scheduler logs
qstat -j <job_id>  # SGE
squeue -j <job_id>  # Slurm
```

### High Memory Usage

Reduce concurrency:

```python
xqute = Xqute(forks=5)  # Reduce from default
```

### Slow Job Submission

Increase submission batch:

```python
xqute = Xqute(
    scheduler='slurm',
    submission_batch=20,
)
```

## Next Steps

- [Schedulers](schedulers.md) - Learn about specific scheduler backends
- [Plugins](plugins.md) - Extend Xqute functionality
- [Advanced Usage](advanced.md) - Advanced patterns and customization
- [API Reference](api/xqute.md) - Complete API documentation
