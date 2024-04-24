import functools

from .tasks import Dependency, Task, TaskMaster

__TASKMASTER__ = TaskMaster()


def task(_func=None, *,
         process_units: int = 1,
         write_all: str | list[str] = None,
         type_key: str = None,
         dependencies: Dependency | list[Dependency] = None
         ):

    if dependencies is None:
        dependencies = []
    if isinstance(dependencies, Dependency):
        dependencies = [dependencies]

    def decorator_task(func):
        _task: Task = Task(
            func,
            write_all=write_all,
            type_key=type_key,
            process_units=process_units,
            dependencies=dependencies
        )
        __TASKMASTER__.append(_task)

        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return func_wrapper

    if _func is None:
        return decorator_task
    else:
        return decorator_task(_func)