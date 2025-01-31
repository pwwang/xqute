"""Utilities for xqute"""

import logging
import os
import stat
import shutil
from pathlib import Path
from typing import Union, Tuple, List

from cloudpathlib import CloudPath, AnyPath
from rich.logging import RichHandler

from .defaults import LOGGER_NAME

PathType = Union[str, os.PathLike, CloudPath]
CommandType = Union[str, Tuple[str], List[str]]


def rmtree(path: PathType):
    """Remove a directory tree, including all its contents

    Args:
        path: The path to the directory
    """
    path = AnyPath(path)
    if isinstance(path, Path):
        shutil.rmtree(path)
    else:
        path.rmtree()


def localize(script: PathType) -> str:
    """Make a script to the local file system, so that it is runnable

    Args:
        script: The script

    Returns:
        The path to the runnable script
    """
    script = AnyPath(script)
    if isinstance(script, Path):
        return str(script)
    # CloudPath
    return script.fspath


def chmodx(path: str) -> str:
    """Make a file executable

    Args:
        path: The path to the file

    Returns:
        The path to the file
    """
    st = os.stat(path)
    os.chmod(path, st.st_mode | stat.S_IEXEC)
    return path


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
