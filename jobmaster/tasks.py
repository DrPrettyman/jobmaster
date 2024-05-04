import inspect
import json
import re
from typing import Callable

from . import utils


class SameParameter:
    def __init__(self):
        self.operations = list()

    def __bool__(self):
        return True

    def __repr__(self):
        return "same"

    def __add__(self, other):
        new = SameParameter()
        new.operations = self.operations + [('+', other)]
        return new

    def __sub__(self, other):
        new = SameParameter()
        new.operations = self.operations + [('-', other)]
        return new

    def __mul__(self, other):
        new = SameParameter()
        new.operations = self.operations + [('*', other)]
        return new

    def __truediv__(self, other):
        new = SameParameter()
        new.operations = self.operations + [('/', other)]
        return new

    @property
    def has_operations(self):
        return bool(self.operations)

    def value(self, _value):
        try:
            for _op, _val in self.operations:
                if _op == '+':
                    _value += _val
                elif _op == '-':
                    _value -= _val
                elif _op == '*':
                    _value *= _val
                elif _op == '/':
                    _value /= _val
            return _value
        except TypeError:
            raise ValueError(f"Invalid operation for SameParameter: {self.operations}")


class Dependency:
    """
    A class to represent a dependency between two tasks.

    The dependency is defined by the task, the time since the task was completed, and any specific arguments.
    """
    def __init__(self,
                 _task_: str | Callable,
                 _hours_: float = None,
                 **kwargs
                 ):
        """
        Create a new dependency object.

        :param _task_:  Either a string in the format 'type.task' or a callable function.
        :param _hours_: Hours since the task was completed. This cannot be less than 2 minutes or more than 30 days.
                        If a value less than 2 minutes is provided, it will be set to 2 minutes.
                        If a value more than 30 days is provided, it will be set to 30 days.
        :param kwargs:  Any specific arguments required for the task.
        """

        if isinstance(_task_, str):
            _keys = tuple(_task_.split('.'))
            if len(_keys) == 2:
                self.type_key, self.task_key = _keys
                self.task_func_module = None
            else:
                raise ValueError(f"Invalid task: {_task_}")
        elif isinstance(_task_, Callable):
            self.task_key = _task_.__name__
            self.type_key = None
            self.task_func_module = _task_.__module__
        else:
            raise ValueError(f"Invalid task arg: {_task_}")

        if not _hours_:
            _hours_ = 24*30
        if _hours_ > 24*30:
            _hours_ = 24*30
        if _hours_ < 2/60:
            _hours_ = 2/60
        self.time = _hours_

        self.all_arguments = kwargs
        args_same = []
        args_specified = []
        for _k, _v in kwargs.items():
            if _k == '_hours_':
                _k = 'hours'
            if isinstance(_v, SameParameter):
                args_same.append((_k, _v))
            else:
                args_specified.append((_k, _v))

        self.args_same = args_same
        self.args_specified = args_specified

    def set_type_key(self, type_key: str):
        self.type_key = type_key

    def set_task_key(self, task_key: str):
        self.task_key = task_key

    def __repr__(self):
        return f"Dependency: {self.type_key}.{self.task_key}"


class Parameter:
    def __init__(self,
                 name: str,
                 value_type: str,
                 help_str: str,
                 select_from: list,
                 required: bool,
                 default_value,
                 write_all: bool):
        self.name = name
        self.value_type = value_type
        self.help = help_str
        self.required = required
        self.default_value = default_value
        self.select_from = select_from
        self.write_all = write_all

    def __repr__(self):
        return f"Parameter: {self.name} ({self.value_type})"

    @property
    def db_value(self):
        if self.select_from is None:
            select_from_array = "NULL"
        else:
            _select_from_json = [f"'{_v}'::json" for _v in self.select_from]
            select_from_array = f"ARRAY[{', '.join(_select_from_json)}]"

        if self.default_value is None:
            default_value = "NULL"
        else:
            default_value = f"'{self.default_value}'::json"
        return f"""
        '{self.name}', '{self.value_type}', {select_from_array}, {default_value}, '{int(self.required)}', '{int(self.write_all)}', '{self.help}'
        """.strip()


class Task:
    def __init__(self,
                 function,
                 write_all: str | list[str] = None,
                 type_key: str = None,
                 process_units: int = 1,
                 dependencies: list[Dependency] = None):

        if isinstance(write_all, str):
            write_all = [_.strip() for _ in write_all.split(',')]
        if write_all is None:
            write_all = []

        assert callable(function), f"function must be callable, not {type(function)}"
        assert isinstance(write_all, list), f"write_all must be a list, not {type(write_all)}"
        assert all(isinstance(_, str) for _ in write_all), f"write_all must be a list of strings, not {write_all}"
        assert isinstance(process_units, int), f"process_units must be an integer, not {type(process_units)}"

        if not type_key:
            type_key = function.__module__.split('.')[-1]

        if dependencies is None:
            dependencies = []

        self.function = function
        self.dependencies = dependencies

        self.type_key = type_key
        self.key = function.__name__

        self.module = function.__module__
        self.absfile = inspect.getabsfile(function)

        self.write_all = write_all
        self.process_units = process_units

        self._parameters = None
        self._help = None

    def __eq__(self, other):
        return self.module + "." + self.key == other.module + "." + other.key

    def __call__(self, *args, **kwargs):
        return self.function(*args, **kwargs)

    def __repr__(self):
        s = f"- Task: {self.type_key}.{self.key}"
        if self.process_units:
            s += f"\n  Process units: {self.process_units}"
        if self.parameters:
            s += f"\n  Parameters:"
            for i, _param in enumerate(self.parameters):
                s += f"\n\t  {i+1}. {_param.name}: {_param.value_type} ({_param.help})"
        if self.dependencies:
            s += f"\n  Dependencies:"
            for i, _dep in enumerate(self.dependencies):
                _args = ", ".join(
                    [
                        f"{_arg}={_val}"
                        if not isinstance(_val, str)
                        else f"{_arg}=\"{_val}\""
                        for _arg, _val in _dep.all_arguments.items()
                    ]
                )
                s += f"\n\t  {i+1}. {_dep.type_key}.{_dep.task_key}("
                s += _args
                s += ")"
        return s

    def _parse_parameters(self) -> list[Parameter]:
        _docstring = inspect.getdoc(self.function)
        _signature = inspect.signature(self.function)

        params_from_doc = utils.parse_params_from_doc(_docstring)
        params_from_sig = utils.parse_params_from_signature(_signature)
        parameters = []
        for _arg_name in params_from_sig.keys():
            if _arg_name in ('self', 'cls', 'kwargs', 'args'):
                continue

            _param = params_from_sig[_arg_name]
            for _k, _v in params_from_doc.get(_arg_name, dict()).items():
                if _v:
                    if not _param.get(_k):
                        _param[_k] = _v

            if _param.get('help_str') is None:
                _param['help_str'] = "No documentation for parameter"

            if _arg_name in self.write_all and isinstance(_param.get('select_from'), list):
                _param['write_all'] = True
            else:
                _param['write_all'] = False

            _param_object = Parameter(**_param)

            parameters.append(_param_object)

        return parameters

    def _parse_help(self) -> str:
        doc = inspect.getdoc(self.function)
        if not doc:
            return "No docstring provided"
        else:
            return re.sub(r"\s+", " ", re.split(r"\n\s*\n", doc)[0]).strip()

    @property
    def parameters(self):
        if self._parameters is None:
            self._parameters = self._parse_parameters()
        return self._parameters

    @property
    def parameter_keys(self):
        return [_.name for _ in self.parameters]

    @property
    def parameter_dict(self):
        return {_.name: _ for _ in self.parameters}

    @property
    def help(self):
        if self._help is None:
            self._help = self._parse_help()
        return self._help

    @property
    def db_dependency_rows(self) -> tuple[list[str], list[str], list[str]]:
        dep_rows = [
            f"'{self.type_key}', '{self.key}', '{_dep.type_key}', '{_dep.task_key}', {_dep.time}"
            for _dep in self.dependencies
        ]

        dep_args_same_rows = [
            f"'{self.type_key}', '{self.key}', '{_dep.type_key}', '{_dep.task_key}', '{_arg}', '{int(_sp.has_operations)}'"
            for _dep in self.dependencies
            for _arg, _sp in _dep.args_same
        ]

        dep_args_specified_rows = [
            f"'{self.type_key}', '{self.key}', '{_dep.type_key}', '{_dep.task_key}', '{_arg[0]}', '{json.dumps(_arg[1])}'"
            for _dep in self.dependencies
            for _arg in _dep.args_specified
        ]

        return dep_rows, dep_args_same_rows, dep_args_specified_rows

    @property
    def db_parameter_rows(self) -> list[str]:
        return [f"'{self.type_key}', '{self.key}', " + _p.db_value for _p in self.parameters]

    @property
    def db_row(self) -> str:
        if not self.write_all:
            _write_all = "NULL"
        else:
            _write_all = f"ARRAY['{', '.join(self.write_all)}']"

        return f"'{self.type_key}', '{self.key}', '{self.help}', {self.process_units}, {_write_all}"


class TaskType:
    def __init__(self, key: str, help_str: str = None):
        self._key: str = key
        self._help_str: str = help_str
        self._tasks: dict[str, Task] = dict()

    @property
    def key(self):
        return self._key

    @property
    def tasks(self):
        return self._tasks

    def __getitem__(self, key: str):
        return self._tasks.get(key)

    def append(self, task: Task):
        self._tasks[task.key] = task


class TaskMaster:
    def __init__(self):
        self._tasks_types: dict[str: TaskType] = dict()
        self._dependencies_validated: bool = False

    @property
    def task_types(self):
        return self._tasks_types

    @property
    def dependencies_validated(self) -> bool:
        return self._dependencies_validated

    def __getitem__(self, key: str | tuple[str, str]):
        if isinstance(key, str):
            keys = tuple(key.split('.'))
        elif isinstance(key, tuple):
            keys = key
        else:
            raise ValueError(f"bad task key {key}")

        if len(keys) == 2:
            _tt = self._tasks_types.get(keys[0])
            if _tt is None:
                return None
            else:
                return _tt[keys[1]]
        elif len(keys) == 1:
            return self._tasks_types.get(key)
        else:
            raise ValueError(f"bad task key {key}")

    @property
    def _task_lookup(self):
        _lookup = dict()
        for _type_key, _task_type in self._tasks_types.items():
            for _task_key, _task in _task_type.tasks.items():
                _lookup[_task.module+'.'+_task.key] = (_type_key, _task_key)
        return _lookup

    def all_tasks(self):
        for _type_key, _task_type in self._tasks_types.items():
            for _task_key, _task in _task_type.tasks.items():
                yield _type_key, _task_key, _task

    def print_tasks(self):
        for _type_key, _task_key, _task in self.all_tasks():
            print(_task)
            print('')

    def append(self, task: Task):
        if task.type_key not in self._tasks_types.keys():
            self._tasks_types[task.type_key] = TaskType(task.type_key)
        self._tasks_types[task.type_key].append(task)

    def validate_dependencies(self):
        for _type_key, _task_type in self._tasks_types.items():
            for _task_key, _task in _task_type.tasks.items():
                for _dep in _task.dependencies:
                    if _dep.type_key is None:
                        __type, __task = self._task_lookup.get(_dep.task_func_module+'.'+_dep.task_key, (None, None))
                        _dep.set_type_key(__type)
                    if not self.__getitem__((_dep.type_key, _dep.task_key)):
                        raise ValueError(f"Dependency {_dep.type_key}.{_dep.task_key} for "
                                         f"task {_type_key}.{_task_key} not recognised")

        self._dependencies_validated = True
