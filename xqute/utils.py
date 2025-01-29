"""Utilities for xqute"""

import logging
from os import PathLike
from tempfile import NamedTemporaryFile
from pathlib import Path
from typing import Union

from cloudpathlib import CloudPath, AnyPath
from rich.logging import RichHandler

from .defaults import LOGGER_NAME

PathType = Union[str, PathLike, CloudPath]


def rmtree(path: PathType):
    """Remove a directory tree, including all its contents

    Args:
        path: The path to the directory
    """
    path = AnyPath(path)
    if path.is_dir():
        for child in path.iterdir():
            rmtree(child)
        path.rmdir()
    else:
        path.unlink()


def runnable(script: PathType) -> str:
    """Make a script runnable (e.g. be able to submit to a real scheduler)

    Args:
        script: The script

    Returns:
        The path to the runnable script
    """
    script = AnyPath(script)
    if isinstance(script, Path):
        return str(script)
    # CloudPath
    with NamedTemporaryFile(
        delete=False, prefix=f"xqute-{script.stem}.", suffix=script.suffix
    ) as tmp:
        tmp.write(script.read_bytes())

    return tmp.name


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
