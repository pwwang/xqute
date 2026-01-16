# Quick Start

Get up and running with Xqute in minutes.

## Installation

### Basic Installation

```bash
pip install xqute
```

### Optional Dependencies

For additional features, install with extras:

```bash
# Google Cloud Storage support
pip install xqute[gs]

# Cloud shell support
pip install xqute[cloudsh]
```

## Your First Job

### Minimal Example

Create a file `first_job.py`:

```python
import asyncio
from xqute import Xqute

async def main():
    # Create Xqute instance with default local scheduler
    xqute = Xqute(forks=3)

    # Add a simple job
    await xqute.feed(['echo', 'Hello, Xqute!'])

    # Wait for all jobs to complete
    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

Run it:

```bash
python first_job.py
```

Output:

```
Job metadata is stored at: .xqute
...
Hello, Xqute!
...
Done!
```

### Multiple Jobs

```python
import asyncio
from xqute import Xqute

async def main():
    # Run 3 jobs concurrently
    xqute = Xqute(forks=3)

    # Add multiple jobs
    for i in range(10):
        await xqute.feed(['echo', f'Job {i}'])

    # Wait for completion
    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

## Daemon Mode (Keep Feeding)

For scenarios where jobs need to be added dynamically:

```python
import asyncio
from xqute import Xqute

async def main():
    xqute = Xqute(forks=3)

    # Start in daemon mode
    await xqute.run_until_complete(keep_feeding=True)

    # Add jobs while running
    for i in range(10):
        await xqute.feed(['sleep', '1'])
        await asyncio.sleep(0.1)

    # Signal completion and wait
    await xqute.stop_feeding()

if __name__ == '__main__':
    asyncio.run(main())
```

## Using Different Schedulers

### SGE (Sun Grid Engine)

```python
import asyncio
from xqute import Xqute

async def main():
    xqute = Xqute(
        scheduler='sge',
        forks=100,
        scheduler_opts={
            'q': '1-day',  # Queue name
            'l': ['h_vmem=2G'],  # Resource requirements
        }
    )

    # Add jobs to SGE
    for i in range(100):
        await xqute.feed(['echo', f'Job {i}'])

    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

### Slurm

```python
import asyncio
from xqute import Xqute

async def main():
    xqute = Xqute(
        scheduler='slurm',
        forks=100,
        scheduler_opts={
            'partition': 'compute',
            'time': '01:00:00',
            'mem': '2G',
        }
    )

    # Add jobs to Slurm
    for i in range(100):
        await xqute.feed(['echo', f'Job {i}'])

    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

### Google Batch

```python
import asyncio
from xqute import Xqute

async def main():
    xqute = Xqute(
        scheduler='gbatch',
        forks=100,
        scheduler_opts={
            'project': 'your-project-id',
            'location': 'us-central1',
            'taskGroups': [
                {
                    'taskSpec': {
                        'runnables': [
                            {
                                'container': {
                                    'imageUri': 'ubuntu',
                                    'entrypoint': 'bash',
                                    'commands': ['-c', 'echo "Hello from Google Batch"']
                                }
                            }
                        ]
                    },
                    'taskCount': 10,
                    'parallelism': 3
                }
            ]
        }
    )

    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

### Docker Containers

```python
import asyncio
from xqute import Xqute

async def main():
    xqute = Xqute(
        scheduler='container',
        forks=3,
        scheduler_opts={
            'image': 'python:3.11',
            'entrypoint': '/bin/bash',
            'bin': 'docker',
        }
    )

    # Run jobs in containers
    for i in range(10):
        await xqute.feed(['python', '-c', f'print({i})'])

    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

## Error Handling

### Retry Strategy

```python
import asyncio
from xqute import Xqute

async def main():
    xqute = Xqute(
        error_strategy='retry',  # Retry failed jobs
        num_retries=3,           # Maximum 3 attempts
    )

    # This job will be retried if it fails
    await xqute.feed(['python', '-c', 'import sys; sys.exit(1)'])

    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

### Halt Strategy

```python
import asyncio
from xqute import Xqute

async def main():
    xqute = Xqute(
        error_strategy='halt',  # Stop on first failure
    )

    await xqute.feed(['echo', 'Job 1'])
    await xqute.feed(['python', '-c', 'import sys; sys.exit(1)'])  # Fails
    await xqute.feed(['echo', 'Job 3'])  # Won't run

    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

## Environment Variables

Pass environment variables to jobs:

```python
import asyncio
from xqute import Xqute

async def main():
    xqute = Xqute(forks=3)

    # Pass environment variables
    await xqute.feed(
        ['bash', '-c', 'echo "MY_VAR: $MY_VAR"'],
        envs={'MY_VAR': 'custom_value'}
    )

    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

## Checking Job Status

```python
import asyncio
from xqute import Xqute

async def main():
    xqute = Xqute(forks=3)

    # Add jobs
    for i in range(5):
        await xqute.feed(['sleep', str(i)])

    # Access job information
    job = xqute.jobs[0]
    print(f"Job index: {job.index}")
    print(f"Job command: {job.cmd}")
    print(f"Job metadir: {job.metadir}")
    print(f"Status file: {job.status_file}")
    print(f"Stdout file: {job.stdout_file}")
    print(f"Stderr file: {job.stderr_file}")

    # Run jobs
    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

## Next Steps

- [User Guide](user-guide.md) - Comprehensive usage guide
- [Schedulers](schedulers.md) - Learn about different scheduler backends
- [Plugins](plugins.md) - Create and use plugins
- [Advanced Usage](advanced.md) - Advanced patterns and customization
- [API Reference](api/xqute.md) - Full API documentation
