"""Utilities for xqute"""
import logging
from pathlib import Path
from os import PathLike
from typing import Callable, Coroutine

import asyncio
from functools import partial, wraps

import aiofiles as aiof

from .defaults import DEBUG, LOGGER_NAME


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
