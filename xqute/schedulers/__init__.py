"""Builtin schedulers"""
from importlib import import_module
from typing import Type, Union
from ..scheduler import Scheduler


def get_scheduler(scheduler: Union[str, Type[Scheduler]]) -> Type[Scheduler]:
    """Get the scheduler class

    Args:
        sched_name: The scheduler name
            Defined in the scheduler class

    Returns:
        The scheduler class
    """
    if isinstance(scheduler, str):
        module = import_module(f'{__name__}.{scheduler}_scheduler')
        return getattr(module,
                       f'{scheduler[0].upper()}{scheduler[1:]}Scheduler')
    return scheduler
