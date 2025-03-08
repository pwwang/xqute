"""A job management system for python"""

from .defaults import JobStatus, JobErrorStrategy
from .xqute import Xqute
from .plugin import plugin
from .utils import logger
from .job import Job
from .scheduler import Scheduler

__version__ = "0.9.0"
