"""Utilities for xqute"""

from __future__ import annotations

import os
import logging
from typing import Union, Tuple, List

from rich.logging import RichHandler

from .defaults import LOGGER_NAME, LOGGER_LEVEL

CommandType = Union[str, Tuple[str], List[str]]


logger = logging.getLogger(LOGGER_NAME)
logger.addHandler(RichHandler(show_path=False, omit_repeated_times=False))

loglevel = os.getenv("XQUTE_LOG_LEVEL", LOGGER_LEVEL).upper()
logger.setLevel(loglevel)
