# Schedulers

Xqute supports multiple scheduler backends for different computing environments. Each scheduler provides a unified interface while handling backend-specific details.

## Available Schedulers

- [Local Scheduler](#local-scheduler) - Run jobs on the local machine
- [SGE Scheduler](#sge-scheduler) - Sun Grid Engine clusters
- [Slurm Scheduler](#slurm-scheduler) - Slurm workload manager
- [SSH Scheduler](#ssh-scheduler) - Distribute jobs across SSH servers
- [Google Batch Scheduler](#google-batch-scheduler) - Google Cloud Batch
- [Container Scheduler](#container-scheduler) - Docker containers

## Local Scheduler

### Overview

Runs jobs directly on the local machine. Best for:
- Development and testing
- Small-scale processing
- Single-machine workloads

### Basic Usage

```python
from xqute import Xqute

xqute = Xqute(
    scheduler='local',
    forks=4,  # Max 4 concurrent jobs
)

await xqute.feed(['echo', 'Hello'])
await xqute.run_until_complete()
```

### Configuration Options

```python
xqute = Xqute(
    scheduler='local',
    forks=4,              # Max concurrent jobs
    workdir='./.xqute',   # Job metadata directory
)
```

### Use Cases

- Running small batches of jobs
- Testing job pipelines
- Processing data on a single machine
- Development and debugging

### Limitations

- Limited to local machine resources
- No cluster management
- No job priority queues

---

## SGE Scheduler

### Overview

Integrates with Sun Grid Engine (SGE) for HPC cluster job management. Best for:
- University and research clusters
- Traditional HPC environments
- Job queue management with resource allocation

### Basic Usage

```python
from xqute import Xqute

xqute = Xqute(
    scheduler='sge',
    forks=100,
    scheduler_opts={
        'q': '1-day',  # Queue name
    }
)

for i in range(100):
    await xqute.feed(['echo', f'Job {i}'])

await xqute.run_until_complete()
```

### Configuration Options

```python
xqute = Xqute(
    scheduler='sge',
    forks=100,
    scheduler_opts={
        # Command paths
        'qsub': '/usr/bin/qsub',
        'qdel': '/usr/bin/qdel',
        'qstat': '/usr/bin/qstat',

        # Queue configuration (shortcut for qsub_q)
        'q': '1-day',

        # Resource requirements
        'l': ['h_vmem=2G', 'gpu=1'],

        # Job arrays
        't': '1-100',

        # Additional qsub options
        'cwd': True,
        'notify': False,

        # Direct qsub parameters
        'qsub_q': '1-day',
        'qsub_l': ['h_vmem=2G'],
        'qsub_N': 'myjob',
    }
)
```

### Resource Requirements

Specify resources using the `l` option:

```python
scheduler_opts={
    'l': [
        'h_vmem=4G',      # Memory per job
        'gpu=1',          # GPU count
        'h_rt=24:00:00',  # Runtime
    ]
}
```

### Common SGE Options

| Parameter | Description | Example |
|-----------|-------------|---------|
| `q` | Queue name | `'1-day'` |
| `l` | Resource list | `['h_vmem=2G']` |
| `t` | Task array | `'1-100'` |
| `N` | Job name | `'myjob'` |
| `cwd` | Current working directory | `True` |
| `notify` | Email notification | `False` |

### Use Cases

- University cluster jobs
- Large-scale data processing
- HPC workflows
- Resource-constrained jobs

### Notes

- Jobs are submitted via `qsub`
- Status checked via `qstat`
- Jobs cancelled via `qdel`
- Jobs fail before running if resources are unavailable

---

## Slurm Scheduler

### Overview

Integrates with Slurm workload manager. Best for:
- Modern HPC clusters
- Cloud-based HPC (e.g., Google Cloud Slurm)
- Job arrays and dependency management

### Basic Usage

```python
from xqute import Xqute

xqute = Xqute(
    scheduler='slurm',
    forks=100,
    scheduler_opts={
        'partition': 'compute',
    }
)

for i in range(100):
    await xqute.feed(['echo', f'Job {i}'])

await xqute.run_until_complete()
```

### Configuration Options

```python
xqute = Xqute(
    scheduler='slurm',
    forks=100,
    scheduler_opts={
        # Command paths
        'sbatch': '/usr/bin/sbatch',
        'scancel': '/usr/bin/scancel',
        'squeue': '/usr/bin/squeue',

        # Job configuration
        'partition': 'compute',
        'time': '01:00:00',
        'mem': '2G',
        'cpus-per-task': 1,

        # Additional options
        'job-name': 'myjob',
        'output': 'job-%j.out',
        'error': 'job-%j.err',
    }
)
```

### Resource Allocation

```python
scheduler_opts={
    'partition': 'gpu',
    'time': '24:00:00',        # Max runtime
    'mem': '8G',                # Memory
    'cpus-per-task': 4,        # CPUs per job
    'gres': 'gpu:1',           # GPU resources
    'nodes': 1,                # Number of nodes
}
```

### Common Slurm Options

| Parameter | Description | Example |
|-----------|-------------|---------|
| `partition` | Queue/partition name | `'compute'` |
| `time` | Runtime limit | `'01:00:00'` |
| `mem` | Memory | `'2G'` |
| `cpus-per-task` | CPUs per job | `4` |
| `gres` | Generic resources | `'gpu:1'` |
| `job-name` | Job name | `'myjob'` |
| `output` | Stdout file | `'job-%j.out'` |
| `error` | Stderr file | `'job-%j.err'` |

### Use Cases

- Modern HPC clusters
- GPU-accelerated computing
- Job arrays
- Large-scale parallel processing

### Notes

- Jobs are submitted via `sbatch`
- Status checked via `squeue`
- Jobs cancelled via `scancel`
- Supports job arrays and dependencies

---

## SSH Scheduler

### Overview

Distributes jobs across multiple SSH-accessible servers. Best for:
- Cluster of Linux servers
- Load balancing across machines
- Using existing SSH infrastructure

### Basic Usage

```python
from xqute import Xqute

xqute = Xqute(
    scheduler='ssh',
    forks=100,
    scheduler_opts={
        'servers': {
            'server1': {
                'user': 'username',
                'host': 'server1.example.com',
                'port': 22,
                'keyfile': '/path/to/keyfile',
            },
            'server2': {
                'user': 'username',
                'host': 'server2.example.com',
            },
        }
    }
)

for i in range(100):
    await xqute.feed(['echo', f'Job {i}'])

await xqute.run_until_complete()
```

### Configuration Options

```python
xqute = Xqute(
    scheduler='ssh',
    forks=100,
    scheduler_opts={
        # SSH command path
        'ssh': '/usr/bin/ssh',

        # Server configuration
        'servers': {
            'server1': {
                'user': 'username',
                'host': 'server1.example.com',
                'port': 22,
                'keyfile': '/path/to/keyfile',
                'ctrl_persist': 600,  # Connection persistence
                'ctrl_dir': '/tmp/ssh',
            },
            'server2': {
                'user': 'username',
                'host': 'server2.example.com',
            },
        }
    }
)
```

### Server Configuration

| Parameter | Description | Example |
|-----------|-------------|---------|
| `user` | SSH username | `'username'` |
| `host` | Server hostname | `'server1.example.com'` |
| `port` | SSH port | `22` |
| `keyfile` | SSH private key path | `'/path/to/key'` |
| `ctrl_persist` | Keep-alive time (seconds) | `600` |
| `ctrl_dir` | Control socket directory | `'/tmp/ssh'` |

### Use Cases

- Load balancing across servers
- Using existing SSH infrastructure
- Distributed computing on multiple machines
- Resource utilization across a cluster

### Notes

- **Requirement**: All servers must share the same filesystem
- **Authentication**: Key-based authentication required
- **Connection**: Uses SSH control sockets for efficiency
- **Workdir**: Must be accessible from all servers

---

## Google Batch Scheduler

### Overview

Integrates with Google Cloud Batch for cloud-based job execution. Best for:
- Cloud-native workflows
- Large-scale parallel processing
- Auto-scaling compute resources

### Basic Usage

```python
from xqute import Xqute

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
```

### Configuration Options

```python
xqute = Xqute(
    scheduler='gbatch',
    forks=100,
    scheduler_opts={
        # Google Cloud configuration
        'project': 'your-project-id',
        'location': 'us-central1',
        'gcloud': '/path/to/gcloud',

        # Job configuration
        'taskGroups': [
            {
                'taskSpec': {
                    'runnables': [
                        {
                            'container': {
                                'imageUri': 'python:3.11',
                                'entrypoint': 'python',
                                'commands': ['-c', 'print("Hello")']
                            }
                        }
                    ],
                    'environment': {
                        'variables': {
                            'MY_VAR': 'value'
                        }
                    }
                },
                'taskCount': 100,
                'parallelism': 10,
                'taskCountPerNode': 2,
            }
        ],

        # Additional options
        'allocationPolicy': {
            'instances': [
                {
                    'policy': {
                        'machineType': 'n1-standard-4',
                    }
                }
            ]
        },
    }
)
```

### Task Group Configuration

```python
'taskGroups': [
    {
        'taskSpec': {
            'runnables': [
                {
                    'container': {
                        'imageUri': 'python:3.11',
                        'entrypoint': 'python',
                        'commands': ['-c', 'print("Hello")']
                    }
                }
            ],
            'environment': {
                'variables': {
                    'MY_VAR': 'value'
                }
            },
            'maxRetryCount': 3,
        },
        'taskCount': 100,        # Total number of tasks
        'parallelism': 10,       # Max parallel tasks
        'taskCountPerNode': 2,   # Tasks per VM
    }
]
```

### Use Cases

- Cloud-native processing
- Auto-scaling workloads
- Large-scale data processing
- Serverless batch jobs

### Notes

- Requires Google Cloud project and authentication
- Uses Google Cloud Batch API
- Supports container-based jobs
- Auto-scales based on parallelism

---

## Container Scheduler

### Overview

Runs jobs in Docker containers. Best for:
- Isolated job execution
- Consistent environments
- Reproducible workflows

### Basic Usage

```python
from xqute import Xqute

xqute = Xqute(
    scheduler='container',
    forks=3,
    scheduler_opts={
        'image': 'python:3.11',
        'entrypoint': '/bin/bash',
    }
)

for i in range(10):
    await xqute.feed(['python', '-c', f'print({i})'])

await xqute.run_until_complete()
```

### Configuration Options

```python
xqute = Xqute(
    scheduler='container',
    forks=3,
    scheduler_opts={
        # Container image
        'image': 'python:3.11',

        # Container entrypoint
        'entrypoint': '/bin/bash',

        # Container runtime
        'bin': 'docker',

        # Volume mounts
        'volumes': [
            '/host/path:/container/path:ro',
            '/data:/data:rw',
        ],

        # Environment variables
        'envs': {
            'MY_VAR': 'value',
        },

        # Additional options
        'remove': True,  # Remove container after completion
        'bin_args': ['--hostname', 'my-job'],
    }
)
```

### Volume Mounts

Mount host directories into containers:

```python
'volumes': [
    # Format: host:container:mode
    '/data:/data:rw',        # Read-write
    '/scripts:/scripts:ro',   # Read-only
    '/tmp:/tmp',             # Default read-write
]
```

### Environment Variables

```python
'envs': {
    'MY_VAR': 'value',
    'ANOTHER_VAR': 'another value',
}
```

### Additional Docker Arguments

```python
'bin_args': [
    '--hostname', 'my-job',
    '--network', 'host',
    '--gpus', 'all',
]
```

### Use Cases

- Isolated job execution
- Reproducible environments
- Testing across different environments
- Container-based workflows

### Notes

- Requires Docker installed
- Containers are removed after completion if `remove=True`
- Volumes must be accessible on all execution nodes
- Supports all Docker run options via `bin_args`

---

## Choosing a Scheduler

| Scheduler | Best For | Scale | Setup Complexity |
|-----------|-----------|-------|------------------|
| **Local** | Development, small jobs | Low | None |
| **SGE** | University clusters | High | Low |
| **Slurm** | Modern HPC | Very High | Low |
| **SSH** | Multi-server clusters | Medium | Medium |
| **Google Batch** | Cloud workloads | Very High | High |
| **Container** | Isolated environments | Medium | Low |

## Custom Schedulers

You can implement custom schedulers by subclassing the `Scheduler` base class:

```python
from xqute import Scheduler

class MyScheduler(Scheduler):
    name = 'mysched'

    async def submit_job(self, job):
        # Submit job to your scheduler
        return job_id

    async def kill_job(self, job):
        # Kill the job
        pass

    async def job_is_running(self, job):
        # Check if job is running
        return True

# Use your scheduler
xqute = Xqute(scheduler=MyScheduler)
```

For more details, see [Advanced Usage](advanced.md#custom-schedulers).

## Next Steps

- [User Guide](user-guide.md) - Comprehensive usage guide
- [Plugins](plugins.md) - Extend scheduler functionality
- [Advanced Usage](advanced.md) - Custom schedulers and advanced patterns
- [API Reference](api/xqute.md) - Complete API documentation
