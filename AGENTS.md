# AGENTS.md

This file contains guidelines and commands for AI coding agents working in the xqute repository.

## Project Overview

Xqute is a Python job management system with async-first design, supporting multiple scheduler backends (Local, SGE, Slurm, SSH, Google Batch, Containers). It uses Poetry for dependency management and follows Python best practices.

## Build/Test/Lint Commands

### Core Commands
```bash
# Install dependencies
poetry install

# Run all tests
poetry run pytest

# Run a single test file
poetry run pytest tests/test_xqute.py

# Run a specific test
poetry run pytest tests/test_xqute.py::test_xqute_init

# Run tests with coverage
poetry run pytest --cov=xqute --cov-report term-missing

# Run tests in parallel
poetry run pytest -n auto

# Lint code
poetry run flake8 xqute

# Format code
poetry run black xqute

# Run pre-commit hooks
poetry run pre-commit run --all-files
```

### Development Workflow
```bash
# Start development shell
poetry shell

# Build package
poetry build

# Install in development mode
pip install -e .
```

## Code Style Guidelines

### Formatting
- **Line length**: 88 characters (Black default)
- **Formatter**: Black
- **Quote style**: Double quotes for strings and docstrings
- **Import style**: `isort` compatible (grouped: stdlib, third-party, local)

### Type Hints
- Use `from __future__ import annotations` in all modules
- Use `TYPE_CHECKING` for forward references
- Return types should be specified for public functions
- Use `Optional[T]` for nullable types

### Import Organization
```python
# Standard library imports
import asyncio
from typing import TYPE_CHECKING, Any

# Third-party imports
from rich.console import Console

# Local imports
from .defaults import JobStatus
from .utils import logger

if TYPE_CHECKING:
    from .job import Job
```

### Naming Conventions
- **Classes**: `PascalCase` (e.g., `Xqute`, `Job`, `Scheduler`)
- **Functions/variables**: `snake_case` (e.g., `get_scheduler`, `job_status`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `DEFAULT_WORKDIR`, `JobStatus`)
- **Private members**: Prefix with `_` (e.g., `_status`, `_cancelling`)

### Documentation
- All modules should have a module docstring
- Public classes and functions should have docstrings
- Use Google-style or Sphinx-style docstrings
- Include `Args:` and `Returns:` sections for functions

### Error Handling
- Use specific exception types when possible
- Log errors using the `logger` from `xqute.utils`
- Use `try/except` blocks for expected error conditions
- Implement retry logic where appropriate (see `Job` class)

### Async/Await Patterns
- Use `asyncio` for concurrent operations
- Use `async with` for context managers
- Use `await` for async calls
- Prefer `asyncio.gather()` for concurrent operations

### Class Design
- Use dataclasses for simple data containers
- Implement `__repr__` for debugging
- Use properties for computed attributes
- Keep classes focused on single responsibility

## Testing Guidelines

### Test Structure
- Test files: `test_*.py` in `/tests` directory
- Test functions: `test_*` prefix
- Use fixtures from `conftest.py` for common setup
- Async tests: use `@pytest.mark.asyncio`

### Test Commands
```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_xqute.py

# Run with verbose output
pytest -vv

# Run with coverage
pytest --cov=xqute

# Run in parallel
pytest -n auto
```

### Test Patterns
- Mock external dependencies
- Use parametrized tests for multiple scenarios
- Test both success and error paths
- Use fixtures for common test data

## Configuration Files

### Key Files
- `pyproject.toml`: Main configuration (Poetry, Black, pytest, mypy)
- `tox.ini`: flake8 configuration
- `.pre-commit-config.yaml`: Pre-commit hooks
- `.coveragerc`: Coverage settings

### Tool Settings
- **Black**: Line length 88, target Python 3.9-3.13
- **flake8**: Ignore E203, W503, E731; max line length 88
- **pytest**: Verbose output, coverage, async mode auto
- **mypy**: Relaxed settings, ignore missing imports

## Development Notes

### Plugin System
- Uses `simplug` for plugin architecture
- Plugins can be enabled/disabled via `plugins` parameter
- See `xqute/plugin.py` for plugin interface

### Scheduler System
- Abstract base class: `Scheduler` in `xqute/scheduler.py`
- Implementations in `xqute/schedulers/`
- Use `get_scheduler()` factory function

### Path Handling
- Uses `panpath` for path utilities
- Supports both local and cloud paths
- See `xqute/path.py` for path operations

### Logging
- Use `logger` from `xqute.utils`
- Configurable log levels
- Structured logging for job tracking

## Common Patterns

### Job Creation
```python
from xqute import Xqute

xqute = Xqute()
job = xqute.put_job("echo 'Hello World'")
```

### Scheduler Implementation
```python
from xqute.scheduler import Scheduler

class MyScheduler(Scheduler):
    async def submit_job(self, job: Job) -> str:
        # Implementation here
        pass
```

### Error Strategy
```python
from xqute.defaults import JobErrorStrategy

# Set error strategy for job
job.error_strategy = JobErrorStrategy.RETRY
```

## Version Management

- Version defined in both `pyproject.toml` and `xqute/__init__.py`
- Use pre-commit hook to ensure version consistency
- Follow semantic versioning (MAJOR.MINOR.PATCH)

## CI/CD

- GitHub Actions for testing and linting
- Multi-Python version testing (3.9-3.13)
- Automatic PyPI deployment on tags
- Documentation deployment to GitHub Pages

## Performance Considerations

- Use `uvloop` for better async performance
- Implement job batching where appropriate
- Use connection pooling for external services
- Monitor memory usage for large job arrays