"""Utilities for xqute"""

from __future__ import annotations

import os
import logging
import re
from typing import Union, Tuple, List, Sequence
from pathlib import Path

from panpath import PanPath
from rich.logging import RichHandler

from .defaults import LOGGER_NAME, LOGGER_LEVEL

CommandType = Union[str, Tuple[str], List[str]]
NAMED_MOUNT_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*=.+$")

logger = logging.getLogger(LOGGER_NAME)
logger.addHandler(RichHandler(show_path=False, omit_repeated_times=False))

loglevel = os.getenv("XQUTE_LOG_LEVEL", LOGGER_LEVEL).upper()
logger.setLevel(loglevel)


async def sanitize_mounts(
    mounts: Union[str, Sequence[str], None],
    mount_root: str,
    named_mounts: str = "NAMED_MOUNTS",
) -> Tuple[List[Tuple[PanPath, Path]], dict[str, str]]:
    """Sanitize the mounts

    Args:
        mounts: The mounts
        mount_root: The root of the mounts

    Returns:
        A tuple of the sanitized mounts and a dictionary of named mounts
    """
    mounts = mounts or []
    if not isinstance(mounts, Sequence) or isinstance(mounts, str):
        mounts = [mounts]

    sanitized_mounts = []
    named_mounts_dict = {}
    for mount in mounts:
        if NAMED_MOUNT_RE.match(mount):
            name, host_path = mount.split("=", 1)
            host_path = PanPath(host_path)
            if await host_path.a_is_file():
                sanitized_mounts.append(
                    (
                        host_path.parent,
                        Path(mount_root) / named_mounts / name / host_path.parent.name,
                    )
                )
                named_mounts_dict[name] = str(
                    Path(mount_root)
                    / named_mounts
                    / name
                    / host_path.parent.name
                    / host_path.name
                )
            else:
                sanitized_mounts.append(
                    (PanPath(host_path), Path(mount_root) / named_mounts / name)
                )
                named_mounts_dict[name] = str(Path(mount_root) / named_mounts / name)
        else:
            host_path, mount_path = mount.rpartition(":")[::2]
            if not host_path or not mount_path:
                raise ValueError(
                    f"Invalid mount format: {mount}. Must be in the format of "
                    "host_path:mount_path or name=host_path"
                )
            sanitized_mounts.append((PanPath(host_path), Path(mount_path)))

    # Check if there are duplicate mount paths
    # 1. if host paths are the same, remove one
    mount_paths = [mount[1] for mount in sanitized_mounts]
    to_remove = set()
    for i, mount_path in enumerate(mount_paths):
        for j, other_mount_path in enumerate(mount_paths):
            if i >= j:
                continue

            if (
                mount_path == other_mount_path
                and sanitized_mounts[i][0] != sanitized_mounts[j][0]
            ):
                raise ValueError(
                    f"Duplicate mount path: {mount_path} with different host paths. "
                    f"Host paths: {sanitized_mounts[i][0]} and {sanitized_mounts[j][0]}"
                )

            if (
                mount_path == other_mount_path
                and sanitized_mounts[i][0] == sanitized_mounts[j][0]
            ):
                # remove the duplicate mount path
                to_remove.add(j)

    sanitized_mounts = [
        mount for i, mount in enumerate(sanitized_mounts) if i not in to_remove
    ]
    # 2. if one mount path is a subpath of another, and the relative part is the same,
    #    remove the longer one
    to_remove = set()
    for i, (host_path, mount_path) in enumerate(sanitized_mounts):
        for j, (other_host_path, other_mount_path) in enumerate(sanitized_mounts):
            if i == j:
                continue

            if mount_path.is_relative_to(other_mount_path):
                mount_relative_part = mount_path.relative_to(other_mount_path)
                host_relative_part = host_path.relative_to(other_host_path)
                if mount_relative_part == host_relative_part:
                    to_remove.add(i)

    sanitized_mounts = [
        mount for i, mount in enumerate(sanitized_mounts) if i not in to_remove
    ]
    # 3. if one mount path is a subpath of another, and the relative part is different,
    #    keep both. Nothing to do here, just a note for clarity.

    return sanitized_mounts, named_mounts_dict
