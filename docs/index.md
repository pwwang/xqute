# Xqute

A powerful, async-first job management system for Python designed to simplify job scheduling and execution across multiple backends.

## Overview

Xqute provides a unified interface for managing computational jobs across different computing environments, from local machines to high-performance computing clusters and cloud services. Its plugin-based architecture and async implementation make it both flexible and performant.

## Key Features

### 🚀 Async-First Design
Built with Python's `asyncio` for maximum performance and non-blocking execution, allowing efficient job management even with thousands of concurrent jobs.

### 🔌 Plugin System
Extensible plugin architecture via `simplug` that allows you to customize job lifecycle hooks, logging, error handling, and more without modifying core code.

### 🎯 Multiple Scheduler Backends
Support for various compute environments out of the box:

- **Local Scheduler**: Run jobs on your local machine
- **SGE Scheduler**: Integrate with Sun Grid Engine clusters
- **Slurm Scheduler**: Work with Slurm workload manager
- **SSH Scheduler**: Distribute jobs across multiple SSH-accessible servers
- **Google Batch Scheduler**: Leverage Google Cloud Batch jobs
- **Container Scheduler**: Run jobs in Docker containers

### 🛡️ Error Handling & Retry
Built-in error strategies with automatic retry logic:
- **Halt Strategy**: Stop execution when a job fails
- **Retry Strategy**: Automatically retry failed jobs up to specified limit
- Custom retry counts and per-job error handling

### ☁️ Cloud-Based Working Directories
Support for cloud storage (Google Cloud Storage) for job metadata, enabling seamless job management across distributed systems.

### 📊 Job Monitoring
Real-time job status tracking with detailed logging, including:
- Job lifecycle state tracking (INIT → QUEUED → SUBMITTED → RUNNING → FINISHED/FAILED)
- STDOUT/STDERR capture
- Return code tracking
- Progress monitoring

## Quick Example

```python
import asyncio
from xqute import Xqute

async def main():
    # Initialize Xqute with 3 concurrent jobs
    xqute = Xqute(forks=3)

    # Add 10 jobs
    for i in range(10):
        await xqute.feed(['echo', f'Job {i}'])

    # Wait for all jobs to complete
    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

## Use Cases

Xqute is ideal for:

- **Bioinformatics Pipelines**: Process thousands of samples in parallel
- **Machine Learning Training**: Run hyperparameter tuning experiments
- **Data Processing**: Batch process large datasets across clusters
- **CI/CD Automation**: Manage build and test jobs
- **Scientific Computing**: Execute simulations and computations on HPC clusters
- **Cloud Workflows**: Orchestrate batch jobs on cloud platforms

## Installation

```bash
pip install xqute
```

For additional features:

```bash
# Google Cloud Storage support
pip install xqute[gs]

# Cloud shell support
pip install xqute[cloudsh]
```

## Documentation

- [Quick Start](quickstart.md) - Get up and running in minutes
- [User Guide](user-guide.md) - Comprehensive usage guide
- [Schedulers](schedulers.md) - Learn about different scheduler backends
- [Plugins](plugins.md) - Create and use plugins
- [Advanced Usage](advanced.md) - Advanced patterns and customization
- [API Reference](api/xqute.md) - Full API documentation

## License

MIT License - see [LICENSE](https://github.com/pwwang/xqute/blob/main/LICENSE) for details.

## Contributing

Contributions are welcome! Please see the [contributing guidelines](https://github.com/pwwang/xqute/blob/main/CONTRIBUTING.md) for more information.

## Links

- [GitHub Repository](https://github.com/pwwang/xqute)
- [PyPI Package](https://pypi.org/project/xqute/)
- [Issue Tracker](https://github.com/pwwang/xqute/issues)
