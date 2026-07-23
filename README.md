<div align="center">

<img src="docs/logo.png" width="128" />

**An async-first job management and scheduling framework for Python.**

[![PyPI version](https://img.shields.io/pypi/v/xqute.svg?color=blue)](https://pypi.org/project/xqute/)
[![Python versions](https://img.shields.io/pypi/pyversions/xqute.svg)](https://pypi.org/project/xqute/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

</div>

---

xqute schedules, submits, monitors, and manages batch jobs across local, HPC, cloud, and container backends — all through a single async Python API. It's built for bioinformatics pipelines, ML hyperparameter sweeps, batch data processing, and any workload that needs to fan out across heterogeneous compute.

## ✨ Features

- **Blazingly fast** — built on `asyncio` with `uvloop`; thousands of jobs, minimal overhead
- **Six scheduler backends** — local, SGE, Slurm, SSH, Google Cloud Batch, Docker/Podman/Apptainer
- **Plugin system** — 14 lifecycle hooks let you add logging, notifications, or custom logic without touching core code
- **Error strategies** — automatic retry with configurable limits, or halt-the-world on first failure
- **File-based status tracking** — jobs self-report via status files; survives network failures and scheduler quirks
- **Daemon mode** — `keep_feeding` lets you add jobs dynamically at any point
- **Cloud storage** — workdirs on GCS (`gs://`), Azure (`az://`), or S3 (`s3://`)
- **Path translation** — seamless SpecPath / MountedPath duality for cross-machine execution
- **Timeouts** — per-job timeout enforcement via `coreutils timeout`

## 📦 Installation

```bash
pip install xqute
```

With optional extras:

```bash
pip install 'xqute[gs]'      # Google Cloud Storage support
pip install 'xqute[cloudsh]'  # Cloud shell support
```

## 🚀 Quick start

### Default (local scheduler)

```python
import asyncio
from xqute import Xqute

async def main():
    xqute = Xqute(forks=3)
    for _ in range(10):
        await xqute.feed(["sleep", "1"])
    await xqute.run_until_complete()

asyncio.run(main())
```

### Daemon mode — add jobs while running

```python
xqute = Xqute(forks=3)

# Start — returns immediately
await xqute.run_until_complete(keep_feeding=True)

# Feed jobs dynamically
for i in range(100):
    await xqute.feed(["python", "train.py", str(i)])
    await asyncio.sleep(0.1)

# Signal done and wait for everything to finish
await xqute.stop_feeding()
```

## 🎯 Scheduler backends

xqute ships with six schedulers. Swap the `scheduler` argument to switch.

### Slurm

```python
xqute = Xqute(
    scheduler="slurm",
    forks=100,
    scheduler_opts={
        "partition": "gpu",
        "time": "24:00:00",
        "mem": "8G",
        "gres": "gpu:1",
    },
)
```

### SGE (Sun Grid Engine)

```python
xqute = Xqute(
    scheduler="sge",
    forks=100,
    scheduler_opts={
        "q": "1-day",
        "l": ["h_vmem=4G", "gpu=1"],
    },
)
```

### SSH (multi-server)

```python
xqute = Xqute(
    scheduler="ssh",
    forks=100,
    scheduler_opts={
        "servers": {
            "node1": {"user": "alice", "host": "node1.example.com", "keyfile": "/home/alice/.ssh/id_rsa"},
            "node2": {"user": "alice", "host": "node2.example.com", "keyfile": "/home/alice/.ssh/id_rsa"},
        }
    },
)
```

> **Note:** SSH servers must share the same filesystem and use key-based auth.

### Google Cloud Batch

```python
xqute = Xqute(
    scheduler="gbatch",
    forks=100,
    scheduler_opts={
        "project": "my-gcp-project",
        "location": "us-central1",
        "taskGroups": [{
            "taskSpec": {
                "runnables": [{
                    "container": {"imageUri": "ubuntu", "entrypoint": "bash", "commands": ["-c", "..."]}
                }]
            },
            "taskCount": 500,
            "parallelism": 100,
        }],
    },
)
```

### Container (Docker / Podman / Apptainer)

```python
xqute = Xqute(
    scheduler="container",
    forks=10,
    scheduler_opts={
        "image": "docker://python:3.12",
        "entrypoint": "/bin/bash",
        "bin": "docker",
        "volumes": ["/data:/data"],
        "envs": {"TF_CPP_MIN_LOG_LEVEL": "2"},
    },
)
```

## 🔌 Plugins

14 lifecycle hooks via `simplug`. Example — send Slack notifications on failures:

```python
from xqute import simplug as pm

@pm.impl
async def on_job_failed(scheduler, job):
    import requests
    requests.post(WEBHOOK, json={"text": f"Job {job.index} failed"})
```

See the [Plugins](https://pwwang.github.io/xqute/plugins/) page for the full list of hooks and more examples.

## 📖 Documentation

Full documentation is at **[pwwang.github.io/xqute](https://pwwang.github.io/xqute/)**:

- [Quick Start](https://pwwang.github.io/xqute/quickstart/) — get running in minutes
- [User Guide](https://pwwang.github.io/xqute/user-guide/) — initialization, error handling, monitoring
- [Schedulers](https://pwwang.github.io/xqute/schedulers/) — all six backends with config reference
- [Plugins](https://pwwang.github.io/xqute/plugins/) — lifecycle hooks and plugin authoring
- [Advanced](https://pwwang.github.io/xqute/advanced/) — custom schedulers, Dask/Airflow integration, perf tuning
- [API Reference](https://pwwang.github.io/xqute/api/xqute/) — auto-generated from source

## 🛠️ Custom scheduler

Implement three async methods to add your own backend:

```python
from xqute import Scheduler

class MyScheduler(Scheduler):
    name = "mycluster"

    async def submit_job(self, job):
        """Submit and return a unique job ID."""

    async def kill_job(self, job):
        """Kill the job given its JID."""

    async def job_is_running(self, job):
        """Return True if the job is still running."""
```

Then pass it directly: `Xqute(scheduler=MyScheduler, ...)`.

## 📊 Architecture

Jobs are wrapped in a bash template with an `EXIT` trap that writes status files (`job.status`, `job.rc`, `job.stdout`, `job.stderr`) into a per-job `metadir`. The polling loop reads these files — no scheduler API calls for status. This design makes xqute resilient to network hiccups and scheduler oddities.

```
INIT → QUEUED → SUBMITTED → RUNNING → FINISHED
                              ↓           ↓
                          KILLING →   FAILED
```

## 🤝 Contributing

Issues and PRs welcome on [GitHub](https://github.com/pwwang/xqute). See [AGENTS.md](AGENTS.md) for dev setup and conventions.

## 📝 License

MIT — see [LICENSE](LICENSE).
