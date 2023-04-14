"""Utilities for xqute"""
import logging
from os import PathLike
from typing import Callable

import asyncio
from functools import partial, wraps

import aiopath as aiop  # type: ignore
import aiofile as aiof
from rich.logging import RichHandler

from .defaults import LOGGER_NAME


# helper functions to read and write the whole content of the file
async def a_read_text(path: PathLike) -> str:
    """Read the text from a file asyncly

    Args:
        path: The path of the file

    Returns:
        The content of the file
    """
    async with aiof.async_open(path, mode='rt') as file:  # type: ignore
        return await file.read()


async def a_write_text(path: PathLike, content: str):
    """Write the text to a file asyncly

    Args:
        path: The path to the file
        content: The content to be written to the file
    """
    async with aiof.async_open(path, mode='wt') as file:  # type: ignore
        await file.write(content)


def asyncify(func: Callable) -> Callable:
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


async def a_mkdir(path: PathLike, *args, **kwargs):
    """Make a directory asyncly

    Args:
        path: The path to the directory to be made
        *args: args for `Path(path).mkdir(...)`
        **kwargs: kwargs for `Path(path).mkdir(...)`
    """
    await aiop.AsyncPath(path).mkdir(*args, **kwargs)


class DuplicateFilter(logging.Filter):
    def __init__(self):
        super().__init__()
        self.prev_msg = None

    def filter(self, record):
        message = record.getMessage()
        if message == self.prev_msg:
            return False
        self.prev_msg = message
        return True


logger = logging.getLogger(LOGGER_NAME)
logger.addHandler(RichHandler(show_path=False, omit_repeated_times=False))
logger.addFilter(DuplicateFilter())
logger.setLevel(logging.INFO)
