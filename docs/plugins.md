# Plugins

Extend Xqute's functionality using the plugin system based on `simplug`.

## Overview

Xqute uses a hook-based plugin system that allows you to:

- Customize job lifecycle behavior
- Add custom logging and monitoring
- Integrate with external services
- Modify job commands dynamically
- Implement custom error handling

## Available Hooks

### Scheduler Hooks

| Hook | Type | Description |
|------|------|-------------|
| `on_init` | sync | Called when scheduler is initialized |
| `on_shutdown` | sync | Called when scheduler shuts down |

### Job Lifecycle Hooks

| Hook | Type | Description |
|------|------|-------------|
| `on_job_init` | async | Called when a job is created |
| `on_job_queued` | async | Called when job is queued |
| `on_job_submitted` | async | Called after job is submitted |
| `on_job_started` | async | Called when job starts running |
| `on_job_polling` | async | Called during status polling |
| `on_job_killing` | async | Called when job is being killed |
| `on_job_killed` | async | Called after job is killed |
| `on_job_failed` | async | Called when job fails |
| `on_job_succeeded` | async | Called when job succeeds |

### Job Command Hooks

| Hook | Type | Description |
|------|------|-------------|
| `on_jobcmd_init` | sync | Returns code to prepend to job script |
| `on_jobcmd_prep` | sync | Returns code before job command |
| `on_jobcmd_end` | sync | Returns code after job command |

## Creating a Plugin

### Basic Plugin Structure

```python
from xqute import simplug as pm

@pm.impl
def on_init(scheduler):
    """Called when scheduler is initialized"""
    print(f"Scheduler {scheduler.name} initialized")

@pm.impl
async def on_job_submitted(scheduler, job):
    """Called when job is submitted"""
    print(f"Job {job.index} submitted with JID: {job._jid}")
```

### Complete Example

Create a file `myplugin.py`:

```python
"""Custom plugin for Xqute"""
from xqute import simplug as pm
from xqute.defaults import JobStatus
import logging

# Setup logging
logger = logging.getLogger('myplugin')

@pm.impl
def on_init(scheduler):
    """Initialize plugin with scheduler"""
    logger.info(f"MyPlugin loaded for {scheduler.name}")

@pm.impl
async def on_job_init(scheduler, job):
    """Called when job is initialized"""
    logger.info(f"Job {job.index} initialized: {job.cmd}")

@pm.impl
async def on_job_queued(scheduler, job):
    """Called when job is queued"""
    logger.info(f"Job {job.index} queued")

@pm.impl
async def on_job_submitted(scheduler, job):
    """Called after job is submitted"""
    jid = await job.get_jid()
    logger.info(f"Job {job.index} submitted (JID: {jid})")

@pm.impl
async def on_job_started(scheduler, job):
    """Called when job starts running"""
    logger.info(f"Job {job.index} started")

@pm.impl
async def on_job_polling(scheduler, job, counter):
    """Called during status polling"""
    if counter % 10 == 0:
        status = await job.get_status()
        status_name = JobStatus.get_name(status)
        logger.debug(f"Job {job.index} polling: {status_name}")

@pm.impl
async def on_job_failed(scheduler, job):
    """Called when job fails"""
    logger.error(f"Job {job.index} failed")

@pm.impl
async def on_job_succeeded(scheduler, job):
    """Called when job succeeds"""
    logger.info(f"Job {job.index} succeeded")
```

## Using Plugins

### Enable Plugin

Import the plugin module before creating Xqute:

```python
import asyncio
from xqute import Xqute

# Import your plugin
import myplugin

async def main():
    xqute = Xqute(forks=3)

    await xqute.feed(['echo', 'Hello'])
    await xqute.run_until_complete()

if __name__ == '__main__':
    asyncio.run(main())
```

### Enable Multiple Plugins

```python
# Import multiple plugins
import logging_plugin
import monitoring_plugin

# Enable specific plugins
xqute = Xqute(plugins=['logging_plugin', 'monitoring_plugin'])
```

### Disable Plugin

```python
# Import all plugins
import myplugin
import another_plugin

# Disable specific plugin
xqute = Xqute(plugins=['-myplugin'])
```

## Plugin Examples

### 1. Email Notifications

```python
"""Send email notifications for job completion"""
import smtplib
from email.mime.text import MIMEText
from xqute import simplug as pm

SMTP_SERVER = 'smtp.example.com'
SMTP_PORT = 587
SMTP_USER = 'user@example.com'
SMTP_PASSWORD = 'password'

def send_email(to, subject, body):
    """Send email notification"""
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = SMTP_USER
    msg['To'] = to

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)

@pm.impl
async def on_job_failed(scheduler, job):
    """Send email on job failure"""
    stderr = await job.stderr_file.a_read_text()
    subject = f"Job {job.index} Failed"
    body = f"Job command: {job.cmd}\n\nStderr:\n{stderr}"
    send_email('admin@example.com', subject, body)
```

### 2. Database Logging

```python
"""Log job information to database"""
import sqlite3
from xqute import simplug as pm

DB_PATH = 'jobs.db'

def get_db():
    """Get database connection"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS jobs (
            index INTEGER PRIMARY KEY,
            command TEXT,
            status INTEGER,
            submitted_at TIMESTAMP,
            completed_at TIMESTAMP
        )
    ''')
    return conn

@pm.impl
async def on_job_submitted(scheduler, job):
    """Log job submission"""
    import datetime
    conn = get_db()
    conn.execute(
        'INSERT INTO jobs (index, command, status, submitted_at) VALUES (?, ?, ?, ?)',
        (job.index, str(job.cmd), 2, datetime.datetime.now())
    )
    conn.commit()
    conn.close()

@pm.impl
async def on_job_succeeded(scheduler, job):
    """Update job completion"""
    import datetime
    conn = get_db()
    conn.execute(
        'UPDATE jobs SET status = 4, completed_at = ? WHERE index = ?',
        (datetime.datetime.now(), job.index)
    )
    conn.commit()
    conn.close()
```

### 3. Slack Notifications

```python
"""Send Slack notifications"""
import requests
from xqute import simplug as pm

SLACK_WEBHOOK = 'https://hooks.slack.com/services/YOUR/WEBHOOK/URL'

def send_slack(message):
    """Send message to Slack"""
    requests.post(SLACK_WEBHOOK, json={'text': message})

@pm.impl
async def on_job_failed(scheduler, job):
    """Send Slack notification on failure"""
    send_slack(f":x: Job {job.index} failed: {job.cmd}")

@pm.impl
async def on_job_succeeded(scheduler, job):
    """Send Slack notification on success"""
    send_slack(f":white_check_mark: Job {job.index} completed")
```

### 4. Custom Command Wrapping

```python
"""Wrap job commands with additional setup"""
from xqute import simplug as pm

@pm.impl
def on_jobcmd_init(scheduler, job):
    """Add initialization code to job script"""
    return f"""
# Custom initialization
set -e
export CUSTOM_VAR="value"
"""

@pm.impl
def on_jobcmd_prep(scheduler, job):
    """Add preparation code before job command"""
    return f"""
# Preparation
cd /tmp/workdir
echo "Preparing job {job.index}"
"""

@pm.impl
def on_jobcmd_end(scheduler, job):
    """Add cleanup code after job command"""
    return f"""
# Cleanup
rm -rf /tmp/workdir
echo "Job {job.index} completed"
"""
```

### 5. Resource Monitoring

```python
"""Monitor job resource usage"""
import psutil
import time
from xqute import simplug as pm

MONITOR_FILE = 'job_resources.txt'

@pm.impl
async def on_job_started(scheduler, job):
    """Start resource monitoring"""
    # In real implementation, start background monitoring
    pass

@pm.impl
async def on_job_polling(scheduler, job, counter):
    """Log resource usage"""
    # Log CPU, memory usage
    cpu_percent = psutil.cpu_percent()
    memory = psutil.virtual_memory()

    with open(MONITOR_FILE, 'a') as f:
        f.write(f"{job.index},{counter},{cpu_percent},{memory.percent}\n")
```

### 6. Conditional Job Submission

```python
"""Conditionally submit jobs based on criteria"""
from xqute import simplug as pm

@pm.impl
async def on_job_submitting(scheduler, job):
    """Decide whether to submit job"""
    # Skip jobs with index 5 and 10
    if job.index in [5, 10]:
        print(f"Skipping job {job.index}")
        return False  # Cancel submission

    # Proceed with submission
    return None
```

## Plugin Development Tips

### 1. Error Handling

Always handle errors gracefully:

```python
@pm.impl
async def on_job_failed(scheduler, job):
    try:
        # Your code here
        pass
    except Exception as e:
        # Log error but don't crash
        from xqute.utils import logger
        logger.error(f"Plugin error: {e}")
```

### 2. Async vs Sync Hooks

Use async hooks for I/O operations:

```python
# Async: Use for file I/O, network calls
@pm.impl
async def on_job_submitted(scheduler, job):
    await job.stdout_file.a_read_text()

# Sync: Use for simple operations
@pm.impl
def on_init(scheduler):
    print("Initialized")
```

### 3. Hook Order

Multiple plugins can implement the same hook. They execute in import order:

```python
# plugin1.py
@pm.impl
def on_init(scheduler):
    print("First")

# plugin2.py
@pm.impl
def on_init(scheduler):
    print("Second")

# Output when imported:
# First
# Second
```

### 4. Canceling Hooks

Return `False` from specific hooks to cancel actions:

```python
@pm.impl
async def on_job_submitting(scheduler, job):
    # Cancel submission
    return False

@pm.impl
async def on_job_killing(scheduler, job):
    # Prevent job from being killed
    return False
```

### 5. Hook Return Values

Some hooks expect return values:

```python
# on_jobcmd_* hooks return code strings
@pm.impl
def on_jobcmd_init(scheduler, job):
    return "# My initialization code"
```

## Plugin Distribution

### Package as Python Package

Create directory structure:

```
xqute-myplugin/
├── setup.py
├── pyproject.toml
├── README.md
└── xqute_myplugin/
    └── __init__.py
```

`xqute_myplugin/__init__.py`:

```python
"""Xqute plugin for custom functionality"""
from xqute import simplug as pm

@pm.impl
def on_init(scheduler):
    print("MyPlugin loaded")
```

`setup.py`:

```python
from setuptools import setup, find_packages

setup(
    name='xqute-myplugin',
    version='0.1.0',
    packages=find_packages(),
    install_requires=['xqute'],
    entry_points={
        'xqute.plugins': [
            'myplugin = xqute_myplugin',
        ],
    },
)
```

### Install Plugin

```bash
pip install xqute-myplugin

# Use plugin
from xqute import Xqute
xqute = Xqute(plugins=['myplugin'])
```

## Testing Plugins

### Unit Testing

```python
import pytest
from xqute import Xqute, simplug as pm

# Define test plugin
@pm.impl
def on_init(scheduler):
    scheduler.test_flag = True

async def test_plugin():
    xqute = Xqute()
    assert hasattr(xqute.scheduler, 'test_flag')
    assert xqute.scheduler.test_flag is True
```

### Integration Testing

```python
import pytest
import asyncio

async def test_plugin_integration():
    # Import plugin
    import myplugin

    xqute = Xqute(forks=1)
    await xqute.feed(['echo', 'test'])
    await xqute.run_until_complete()

    # Verify plugin behavior
    # ... assertions
```

## Best Practices

1. **Error Handling**: Always catch exceptions in hooks
2. **Logging**: Use `xqute.utils.logger` for consistent logging
3. **Performance**: Avoid blocking operations in async hooks
4. **Idempotency**: Hooks should be safe to call multiple times
5. **Documentation**: Document your hooks and behavior
6. **Testing**: Write tests for your plugin functionality
7. **Compatibility**: Test with multiple Xqute versions

## Next Steps

- [Advanced Usage](advanced.md) - Advanced patterns and customization
- [User Guide](user-guide.md) - Comprehensive usage guide
- [API Reference](api/xqute.md) - Complete API documentation
