"""Utilities for xqute"""
from __future__ import annotations

import logging
from typing import Union, Tuple, List

from rich.logging import RichHandler

from .defaults import LOGGER_NAME

CommandType = Union[str, Tuple[str], List[str]]


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
