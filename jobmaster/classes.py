import datetime
import inspect
import json
import re
from typing import Callable

from .utils import _parse_params_from_signature, _parse_params_from_doc


class SameParameter:
    def __bool__(self):
        return True


class Dependency:
    def __init__(self,
                 _task_: str | Callable,
                 _hours_: float = 0.0,
                 **kwargs
                 ):

        if isinstance(_task_, str):
            _keys = tuple(_task_.split('.'))
            if len(_keys) == 2:
                self.type_key, self.task_key = _keys
                self.task_func_repr = None
            else:
                raise ValueError(f"Invalid task: {_task_}")
        elif isinstance(_task_, Callable):
            self.task_key = _task_.__name__
            self.type_key = None
            self.task_func_source = re.sub(r'\s+', '', inspect.getsource(_task_))
        else:
            raise ValueError(f"Invalid task: {_task_}")

        if _hours_ < 0:
            raise ValueError(f"Dependency time cannot be negative")
        if _hours_ == 0:
            _hours_ = 24*30
        self.time = _hours_

        args_same = []
        args_specified = []
        for _k, _v in kwargs.items():
            if _k == '_hours_':
                _k = 'hours'
            if isinstance(_v, SameParameter):
                args_same.append(_k)
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
                 help: str,
                 select_from: list,
                 required: bool,
                 default_value,
                 write_all: bool):
        self.name = name
        self.value_type = value_type
        self.help = help
        self.required = required
        self.default_value = default_value
        self.select_from = select_from
        self.write_all = write_all

    def __repr__(self):
        return f"Parameter: {self.name}"

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
                 process_limit: int = None,
                 dependencies: list[Dependency] = None):

        if isinstance(write_all, str):
            write_all = [_.strip() for _ in write_all.split(',')]
        if write_all is None:
            write_all = []

        if process_limit is not None and not isinstance(process_limit, int):
            raise ValueError(f"process_limit must be an integer, not {type(process_limit)}")

        if not type_key:
            type_key = function.__module__

        if dependencies is None:
            dependencies = []

        self.function = function
        self.dependencies = dependencies

        self.type_key = type_key
        self.key = function.__name__

        self.module = function.__module__
        self.absfile = inspect.getabsfile(function)
        self.task_func_source = re.sub(r'\s+', '', inspect.getsource(function))

        self.write_all = write_all
        self.process_limit = process_limit

        self._parameters = None
        self._help = None

    def __eq__(self, other):
        return self.absfile == other.absfile

    def __call__(self, *args, **kwargs):
        return self.function(*args, **kwargs)

    def _parse_parameters(self):
        _docstring = inspect.getdoc(self.function)
        _signature = inspect.signature(self.function)

        params_from_doc = _parse_params_from_doc(_docstring)
        params_from_sig = _parse_params_from_signature(_signature)
        parameters = []
        for _arg_name in params_from_sig.keys():
            if _arg_name in ('self', 'cls', 'kwargs', 'args'):
                continue

            _param = params_from_sig[_arg_name]
            for _k, _v in params_from_doc.get(_arg_name, dict()).items():
                if _v:
                    if not _param.get(_k):
                        _param[_k] = _v

            if _param.get('help') is None:
                _param['help'] = "No documentation for parameter"

            if _arg_name in self.write_all and isinstance(_param.get('select_from'), list):
                _param['write_all'] = True
            else:
                _param['write_all'] = False

            _param_object = Parameter(**_param)

            parameters.append(_param_object)

        return parameters

    def _parse_help(self):
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

    def __repr__(self):
        return f"Task: {self.type_key}.{self.key}"

    @property
    def db_dependency_rows(self) -> tuple[list[str], list[str], list[str]]:
        dep_rows = [
            f"'{self.type_key}', '{self.key}', '{_dep.type_key}', '{_dep.task_key}', {_dep.time}"
            for _dep in self.dependencies
        ]

        dep_args_same_rows = [
            f"'{self.type_key}', '{self.key}', '{_dep.type_key}', '{_dep.task_key}', '{_arg}'"
            for _dep in self.dependencies
            for _arg in _dep.args_same
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
        if self.process_limit is None:
            _process_limit = "NULL"
        else:
            _process_limit = self.process_limit

        if not self.write_all:
            _write_all = "NULL"
        else:
            _write_all = f"ARRAY['{', '.join(self.write_all)}']"

        return f"'{self.type_key}', '{self.key}', '{self.help}', {_process_limit}, {_write_all}"

