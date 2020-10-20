"""Builtin schedulers"""
from importlib import import_module
from typing import Type
from ..scheduler import Scheduler

def get_scheduler(sched_name: str) -> Type[Scheduler]:
    """Get the scheduler class

    Args:
        sched_name: The scheduler name
            Defined in the scheduler class

    Returns:
        The scheduler class
    """
    module = import_module(f'{__name__}.{sched_name}_scheduler')
    return getattr(module, f'{sched_name[0].upper()}{sched_name[1:]}Scheduler')
