"""Utilities for xqute"""
import logging
from pathlib import Path
from os import PathLike
from typing import Callable, Coroutine, Tuple, Union

import asyncio
from functools import partial, wraps

import uvloop
import aiofiles as aiof
# pylint: disable=invalid-name
uvloop.install()

# pylint: disable=invalid-name
DEBUG = True
LOGGER_NAME = 'XQUTE'

class JobErrorStrategy:
    """The strategy when error happen from jobs

    Attributes:
        IGNORE: ignore and run next jobs
        RETRY: retry the job
        HALT: halt the whole program
    """
    IGNORE: str = 'ignore'
    RETRY: str = 'retry'
    HALT: str = 'halt'

class JobStatus:
    """The status of a job

    Life cycles:
    ........................queued in scheduler
    INIT -> QUEUED -> SUBMITTED -> RUNNING -> FINISHED (FAILED)
    INIT -> QUEUED -> SUBMITTED -> RUNNING -> KILLING -> FINISHED
    INIT -> QUEUED -> SUBMITTED -> KILLING -> FINISHED
    INIT -> QUEUED -> (CANELED)

    Attributes:
        INIT: When a job is initialized
        RETRYING: When a job is to be retried
        QUEUED: When a job is queued
        SUBMITTED: When a job is sumitted
        RUNNING: When a job is running
        KILLING: When a job is being killed
        FINISHED: When a job is finished
        FAILED: When a job is failed
    """
    INIT: int = 0
    RETRYING: int = 1
    QUEUED: int = 2
    SUBMITTED: int = 3
    RUNNING: int = 4
    KILLING: int = 5
    FINISHED: int = 6
    FAILED: int = 7

    @classmethod
    def get_name(cls, *statuses: Tuple[int]) -> Union[Tuple[str], str]:
        """Get the name of the status

        Args:
            *statuses: The status values

        Returns:
            The name of the status if a single status is passed, otherwise
            a tuple of names
        """
        ret_dict = {}
        for name, value in cls.__dict__.items():
            if value in statuses:
                ret_dict[value] = name
        ret_tuple = tuple(ret_dict[status] for status in statuses)
        if len(ret_tuple) > 1:
            return ret_tuple
        return ret_tuple[0] # pragma: no cover

# helper functions to read and write the whole content of the file
async def a_read_text(path: PathLike) -> str:
    """Read the text from a file asyncly

    Args:
        path: The path of the file

    Returns:
        The content of the file
    """
    async with aiof.open(path, mode='rt') as file:
        return await file.read()

async def a_write_text(path: PathLike, content: str):
    """Write the text to a file asyncly

    Args:
        path: The path to the file
        content: The content to be written to the file
    """
    async with aiof.open(path, mode='wt') as file:
        await file.write(content)

def asyncify(func: Callable) -> Coroutine:
    """Turn a sync function into a Coroutine, can be used as a decorator

    Args:
        func: The sync function

    Returns:
        The Coroutine
    """
    @wraps(func)
    async def run(*args, loop=None, executor=None, **kwargs):
        loop = loop or asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return await loop.run_in_executor(executor, pfunc)

    return run

@asyncify
def a_mkdir(path: PathLike, *args, **kwargs):
    """Make a directory asyncly

    Args:
        path: The path to the directory to be made
        *args: args for `Path(path).mkdir(...)`
        **kwargs: kwargs for `Path(path).mkdir(...)`
    """
    Path(path).mkdir(*args, **kwargs)

logger = logging.getLogger(LOGGER_NAME)
logger.setLevel(logging.INFO)
if DEBUG:
    from rich.logging import RichHandler
    logger.addHandler(RichHandler(show_path=False))
