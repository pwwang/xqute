# Advanced Usage

Advanced patterns and customization for power users.

## Table of Contents

- [Custom Schedulers](#custom-schedulers)
- [Job Dependencies](#job-dependencies)
- [Job Arrays](#job-arrays)
- [Progress Tracking](#progress-tracking)
- [Streaming Output](#streaming-output)
- [Custom Error Handling](#custom-error-handling)
- [Job Templates](#job-templates)
- [Batch Processing](#batch-processing)
- [Integration with Other Tools](#integration-with-other-tools)
- [Performance Optimization](#performance-optimization)

## Custom Schedulers

### Creating a Custom Scheduler

Implement the `Scheduler` abstract class:

```python
from xqute import Scheduler

class CustomScheduler(Scheduler):
    """Custom scheduler implementation"""
    name = 'custom'

    async def submit_job(self, job):
        """Submit a job and return its unique ID"""
        # Your submission logic here
        job_id = self._submit_to_custom_system(job)
        return job_id

    async def kill_job(self, job):
        """Kill a job"""
        jid = await job.get_jid()
        # Your kill logic here
        self._kill_in_custom_system(jid)

    async def job_is_running(self, job):
        """Check if a job is running"""
        jid = await job.get_jid()
        # Your status check logic here
        return self._check_status(jid) == 'running'
```

### Using Custom Scheduler

```python
from xqute import Xqute

xqute = Xqute(
    scheduler=CustomScheduler,
    forks=10,
    scheduler_opts={
        'custom_param': 'value',
    }
)

await xqute.feed(['echo', 'Hello'])
await xqute.run_until_complete()
```

### Advanced Custom Scheduler Example

```python
from xqute import Scheduler
from typing import Dict, Any
import subprocess

class MyClusterScheduler(Scheduler):
    """Scheduler for custom cluster"""

    name = 'mycluster'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.jobs: Dict[str, Any] = {}

    async def submit_job(self, job):
        """Submit job to custom cluster"""
        cmd = self._build_submit_command(job)
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()

        if proc.returncode != 0:
            raise RuntimeError(f"Submission failed: {stderr.decode()}")

        job_id = stdout.decode().strip()
        self.jobs[job_id] = {
            'job': job,
            'status': 'submitted',
        }
        return job_id

    async def kill_job(self, job):
        """Kill job"""
        jid = await job.get_jid()
        cmd = ['mycluster-kill', jid]
        await asyncio.create_subprocess_exec(*cmd)
        if jid in self.jobs:
            del self.jobs[jid]

    async def job_is_running(self, job):
        """Check if job is running"""
        jid = await job.get_jid()
        if jid not in self.jobs:
            return False

        cmd = ['mycluster-status', jid]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
        )
        stdout, _ = await proc.communicate()
        status = stdout.decode().strip()

        self.jobs[jid]['status'] = status
        return status == 'running'

    def _build_submit_command(self, job):
        """Build submission command"""
        return [
            'mycluster-submit',
            '--job-name', f'{self.jobname_prefix}-{job.index}',
            '--output', str(job.stdout_file),
            '--error', str(job.stderr_file),
        ]
```

## Job Dependencies

### Sequential Dependencies

Run jobs in order using daemon mode:

```python
import asyncio
from xqute import Xqute

async def run_with_dependencies():
    xqute = Xqute(forks=1)  # Single fork for sequential execution

    await xqute.run_until_complete(keep_feeding=True)

    # Job 1
    await xqute.feed(['prepare_data.sh'])
    await xqute.run_until_complete()

    # Job 2 (depends on Job 1)
    await xqute.feed(['process_data.sh'])
    await xqute.run_until_complete()

    # Job 3 (depends on Job 2)
    await xqute.feed(['finalize.sh'])

    await xqute.stop_feeding()
```

### Dependency Tracking with Plugins

```python
from xqute import simplug as pm

dependencies = {}  # job_index -> [parent_indices]

@pm.impl
async def on_job_queued(scheduler, job):
    """Check if dependencies are satisfied"""
    if job.index not in dependencies:
        return

    deps = dependencies[job.index]
    for dep_idx in deps:
        dep_job = scheduler.xqute.jobs[dep_idx]
        status = await dep_job.get_status()
        if status != 4:  # Not FINISHED
            # Skip for now, will be retried later
            return False
```

## Job Arrays

### Simple Job Array

```python
import asyncio
from xqute import Xqute

async def main():
    xqute = Xqute(forks=10)

    # Submit job array
    for i in range(100):
        await xqute.feed(['process.sh', str(i)])

    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

### Job Array with Slurm

```python
from xqute import Xqute

xqute = Xqute(
    scheduler='slurm',
    forks=100,
    scheduler_opts={
        'partition': 'compute',
        'array': '1-100',  # Job array
    }
)

# Single submission for array
await xqute.feed(['process.sh', '$SLURM_ARRAY_TASK_ID'])
await xqute.run_until_complete()
```

## Progress Tracking

### Simple Progress Bar

```python
import asyncio
from xqute import Xqute
from xqute.defaults import JobStatus
from tqdm import tqdm

async def track_progress():
    xqute = Xqute(forks=5)

    # Submit jobs
    for i in range(100):
        await xqute.feed(['sleep', '1'])

    # Start progress bar
    progress = tqdm(total=100, desc="Processing")

    async def update_progress():
        while True:
            completed = sum(
                1 for job in xqute.jobs
                if await job.get_status() in (JobStatus.FINISHED, JobStatus.FAILED)
            )
            progress.n = completed
            progress.refresh()

            if completed == 100:
                progress.close()
                break
            await asyncio.sleep(1)

    # Run jobs with progress tracking
    import asyncio
    await asyncio.gather(
        xqute.run_until_complete(),
        update_progress(),
    )
```

### Detailed Progress Tracking

```python
import asyncio
from xqute import Xqute
from xqute.defaults import JobStatus
import time

class ProgressTracker:
    def __init__(self, xqute):
        self.xqute = xqute
        self.start_time = time.time()

    async def track(self):
        while True:
            status_counts = {}
            for status in JobStatus:
                count = sum(
                    1 for job in self.xqute.jobs
                    if await job.get_status() == status
                )
                status_counts[status] = count

            elapsed = time.time() - self.start_time
            total = len(self.xqute.jobs)
            completed = status_counts.get(JobStatus.FINISHED, 0)
            failed = status_counts.get(JobStatus.FAILED, 0)

            print(f"[{elapsed:.1f}s] "
                  f"Completed: {completed}/{total} "
                  f"Failed: {failed} "
                  f"Running: {status_counts.get(JobStatus.RUNNING, 0)}")

            if completed + failed == total:
                break

            await asyncio.sleep(2)

async def main():
    xqute = Xqute(forks=5)

    for i in range(50):
        await xqute.feed(['sleep', str(i % 5)])

    tracker = ProgressTracker(xqute)
    await asyncio.gather(
        xqute.run_until_complete(),
        tracker.track(),
    )
```

## Streaming Output

### Stream Output to Console

```python
from xqute import simplug as pm
import asyncio

@pm.impl
async def on_job_succeeded(scheduler, job):
    """Stream output after job completes"""
    stdout = await job.stdout_file.a_read_text()
    if stdout:
        print(f"Job {job.index} output:\n{stdout}")
```

### Real-time Output Streaming

```python
from xqute import simplug as pm
import aiofiles

@pm.impl
async def on_job_polling(scheduler, job, counter):
    """Stream output during polling"""
    try:
        async with aiofiles.open(job.stdout_file, 'r') as f:
            # Seek to end
            await f.seek(0, 2)
            # Read new lines
            while True:
                line = await f.readline()
                if not line:
                    break
                print(f"[Job {job.index}] {line.strip()}")
    except FileNotFoundError:
        pass
```

## Custom Error Handling

### Conditional Retry Logic

```python
from xqute import simplug as pm

@pm.impl
async def on_job_failed(scheduler, job):
    """Custom retry logic based on error type"""
    stderr = await job.stderr_file.a_read_text()

    # Don't retry on certain errors
    if 'SyntaxError' in stderr:
        from xqute.utils import logger
        logger.warning(f"Syntax error in job {job.index}, not retrying")
        # Disable retry for this job
        job._error_retry = False
        job._num_retries = 0
```

### Error Aggregation

```python
from xqute import simplug as pm

error_summary = {
    'syntax_errors': 0,
    'runtime_errors': 0,
    'timeout_errors': 0,
}

@pm.impl
async def on_job_failed(scheduler, job):
    """Aggregate error types"""
    stderr = await job.stderr_file.a_read_text()

    if 'SyntaxError' in stderr:
        error_summary['syntax_errors'] += 1
    elif 'TimeoutError' in stderr:
        error_summary['timeout_errors'] += 1
    else:
        error_summary['runtime_errors'] += 1

@pm.impl
def on_shutdown(scheduler, sig):
    """Print error summary"""
    print("Error Summary:")
    for error_type, count in error_summary.items():
        print(f"  {error_type}: {count}")
```

## Job Templates

### Template with Environment Variables

```python
from xqute import Xqute

def create_template_job(index, params):
    """Create job from template"""
    return {
        'cmd': ['python', 'process.py'],
        'envs': {
            'INPUT_FILE': params['input'],
            'OUTPUT_DIR': params['output'],
            'PARAM_A': str(params.get('param_a', 1)),
        }
    }

async def main():
    xqute = Xqute(forks=5)

    # Create jobs from templates
    jobs = [
        {
            'input': f'/data/input_{i}.txt',
            'output': f'/data/output_{i}/',
            'param_a': i,
        }
        for i in range(10)
    ]

    for i, params in enumerate(jobs):
        job = create_template_job(i, params)
        await xqute.feed(job['cmd'], envs=job['envs'])

    await xqute.run_until_complete()
```

### Template with Command Generation

```python
def generate_job_commands(config):
    """Generate job commands from configuration"""
    commands = []

    for dataset in config['datasets']:
        for model in config['models']:
            cmd = [
                'python', 'train.py',
                '--dataset', dataset,
                '--model', model,
                '--epochs', str(config.get('epochs', 10)),
                '--batch-size', str(config.get('batch_size', 32)),
            ]
            commands.append(cmd)

    return commands

# Usage
config = {
    'datasets': ['imagenet', 'cifar10'],
    'models': ['resnet50', 'vgg16'],
    'epochs': 50,
    'batch_size': 64,
}

xqute = Xqute(forks=4)

for cmd in generate_job_commands(config):
    await xqute.feed(cmd)

await xqute.run_until_complete()
```

## Batch Processing

### Process Files in Batches

```python
import asyncio
from xqute import Xqute
from pathlib import Path

async def process_files(file_list, batch_size=10):
    """Process files in batches"""
    xqute = Xqute(forks=5)

    for i in range(0, len(file_list), batch_size):
        batch = file_list[i:i+batch_size]

        for file_path in batch:
            await xqute.feed(['python', 'process.py', file_path])

        # Wait for batch to complete
        await xqute.run_until_complete()
        xqute = Xqute(forks=5)  # Create new Xqute for next batch

# Usage
files = list(Path('/data').glob('*.txt'))
await process_files(files, batch_size=10)
```

### Batch Processing with Results

```python
import asyncio
from xqute import Xqute

async def process_with_results(items, process_func):
    """Process items and collect results"""
    xqute = Xqute(forks=5)

    # Submit all jobs
    for item in items:
        await xqute.feed(['python', process_func, item])

    # Wait for completion
    await xqute.run_until_complete()

    # Collect results
    results = []
    for job in xqute.jobs:
        stdout = await job.stdout_file.a_read_text()
        results.append((job.index, stdout))

    return results

# Usage
items = ['item1', 'item2', 'item3']
results = await process_with_results(items, 'processor.py')
```

## Integration with Other Tools

### Integration with Airflow

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
import asyncio
from xqute import Xqute
from datetime import datetime, timedelta

def run_xqute_jobs(**context):
    """Run Xqute jobs in Airflow task"""

    async def process():
        xqute = Xqute(
            scheduler='slurm',
            forks=10,
            scheduler_opts={
                'partition': 'airflow',
            }
        )

        # Get files from Airflow context
        files = context['params']['files']

        for file_path in files:
            await xqute.feed(['python', 'process.py', file_path])

        await xqute.run_until_complete()

    asyncio.run(process())

with DAG(
    'xqute_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 1, 1),
        'retries': 1,
    },
    schedule_interval=timedelta(days=1),
) as dag:

    run_jobs = PythonOperator(
        task_id='run_xqute_jobs',
        python_callable=run_xqute_jobs,
        params={
            'files': ['/data/file1.txt', '/data/file2.txt'],
        },
    )
```

### Integration with Dask

```python
import dask
from xqute import Xqute
import asyncio

@dask.delayed
def run_xqute_task(task_id, command):
    """Run Xqute job as Dask task"""

    async def process():
        xqute = Xqute(forks=1)
        await xqute.feed(command)
        await xqute.run_until_complete()

    asyncio.run(process())

# Create Dask workflow
tasks = [
    run_xqute_task(i, ['python', 'process.py', str(i)])
    for i in range(100)
]

# Execute with Dask
results = dask.compute(*tasks)
```

## Performance Optimization

### Optimize Concurrency

```python
import os
import psutil

def calculate_optimal_forks():
    """Calculate optimal concurrency based on resources"""
    cpu_count = os.cpu_count()
    mem_gb = psutil.virtual_memory().total / (1024**3)

    # Adjust based on job type
    if mem_gb > 64:
        return cpu_count * 2
    elif mem_gb > 32:
        return cpu_count
    else:
        return max(1, cpu_count // 2)

xqute = Xqute(forks=calculate_optimal_forks())
```

### Batch Submissions

```python
# Reduce scheduler overhead by batching submissions
xqute = Xqute(
    scheduler='slurm',
    forks=1000,
    submission_batch=50,  # Submit 50 jobs at once
)
```

### Reduce Polling Frequency

```python
# Poll less frequently for long-running jobs
xqute = Xqute(
    scheduler='slurm',
    recheck_interval=50,  # Check every 50 polls instead of 10
)
```

### Optimize Working Directory

```python
# Use local directory instead of cloud for speed
xqute = Xqute(
    workdir='./.xqute',  # Local, not 'gs://bucket/jobs'
)

# Or use mounted path
xqute = Xqute(
    workdir='gs://bucket/jobs',
    scheduler_opts={
        'mounted_workdir': '/mnt/gs',  # Faster access
    },
)
```

### Memory Management

```python
from xqute import simplug as pm

@pm.impl
async def on_job_succeeded(scheduler, job):
    """Clean up large output files to save space"""
    # Remove stdout if it's too large
    if await job.stdout_file.a_size() > 10_000_000:  # 10MB
        await job.stdout_file.a_unlink()

    # Remove stderr
    await job.stderr_file.a_unlink()
```

## Next Steps

- [User Guide](user-guide.md) - Comprehensive usage guide
- [Schedulers](schedulers.md) - Learn about different scheduler backends
- [Plugins](plugins.md) - Extend functionality
- [API Reference](api/xqute.md) - Complete API documentation
