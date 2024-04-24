__version__ = "0.1.1"
__author__ = 'Joshua Prettyman'
__credits__ = 'Blink SEO'


from typing import Callable
from .jobs import JobMaster
from .tasks import Dependency, SameParameter
from .decorator import task
from . import utils

same = SameParameter()


def dep(_task_: str | Callable, _hours_: float = None, **kwargs):
    return Dependency(_task_, _hours_, **kwargs)
