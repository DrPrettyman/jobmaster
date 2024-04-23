import datetime
import inspect
import json
import re
import sqlalchemy
from uuid import uuid4
from typing import Callable
from itertools import product
from sqlalchemy.sql.elements import TextClause

from . import utils


_status_lookup = {
    0: 'local',
    1: 'waiting',
    2: 'running',
    3: 'complete',
    4: 'failed',
    5: 'cancelled'
}


class SameParameter:
    def __bool__(self):
        return True

    def __repr__(self):
        return "same"


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
        self.process_limit = process_limit

        self._parameters = None
        self._help = None

    def __eq__(self, other):
        return self.module + "." + self.key == other.module + "." + other.key

    def __call__(self, *args, **kwargs):
        return self.function(*args, **kwargs)

    def __repr__(self):
        s = f"- Task: {self.type_key}.{self.key}"
        if self.process_limit:
            s += f"\n  Process limit: {self.process_limit}"
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


class Job:
    def __init__(self,
                 cmd_id: str,
                 system_node_name: str,
                 system_pid: int,
                 db_engine: sqlalchemy.engine.base.Engine,
                 db_schema: str,
                 task: Task,
                 logger: Callable,
                 priority: int = 0,
                 status: int = 0,
                 arguments: dict | str = None,
                 job_id: str = None,
                 created_at: datetime.datetime = None,
                 collect_id: str = None,
                 message: str = None,
                 fill_args: bool = False):

        if job_id is None:
            job_id = uuid4().hex
        self.job_id = job_id

        if arguments is None:
            arguments = dict()
        if isinstance(arguments, str):
            arguments = utils.parse_argument_type(arguments)
        if not isinstance(arguments, dict):
            raise ValueError(f"Invalid arguments: {arguments}")
        self.arguments = arguments

        self.logger = logger

        self._engine = db_engine
        self._schema = db_schema

        self._cmd_id = cmd_id
        self._system_node_name = system_node_name
        self._system_pid = system_pid

        self.task = task
        self.priority = priority

        self._status = status
        if created_at is None:
            created_at = datetime.datetime.utcnow()
        self.created_at = created_at

        self.collect_id = collect_id
        self.message = message

        # fill in required arguments
        if fill_args:
            for _param in self.task.parameters:
                if not _param.required:
                    continue  # If the argument is not required, we don't need to fill it
                if _param.name not in self.arguments.keys():
                    if _param.default_value is not None:
                        self.arguments[_param.name] = _param.default_value
                        self.logger.debug(f"Required argument `{_param.name}` not provided. "
                                          f"Assigned default value `{_param.default_value}` ({type(_param.default_value)}).")
                    elif _param.value_type == 'NoneType':
                        self.arguments[_param.name] = None
                        self.logger.debug(f"Required argument `{_param.name}` not provided. "
                                          f"Assigned value `None`.")
                    elif _param.select_from:
                        self.arguments[_param.name] = _param.select_from[0]
                        self.logger.debug(f"Required argument `{_param.name}` not provided. "
                                          f"Assigned value `{_param.select_from[0]}` ({type(_param.select_from[0])}).")
                    else:
                        self.logger.error(f"Argument `{_param.name}` is required but not provided")

        self.logger.info(f"Initialised local Job instance with ID: {self.job_id}")

    def __bool__(self):
        return True

    def __repr__(self):
        args = ", ".join(
            [f"{_arg}={_val}"
             if not isinstance(_val, str)
             else f"{_arg}=\"{_val}\"" for _arg, _val
             in self.arguments.items()]
        )
        lines = [
            f"Job ID:    {self.job_id}"
            f"\nSignature: {self.task.type_key}.{self.task.key}({args})"
            f"\nStatus:    {self.status_str}"
        ]
        if self.message:
            lines.append(f"\nMessage:   {self.message}")

        return "".join(lines)

    def __str__(self):
        return self.__repr__()

    @property
    def active(self):
        return self._status in (0, 1, 2)

    def set_message(self, message: str, _message_level: str = 'debug', _status: int = None):
        if _status is None:
            _status_str = self.status_str
        else:
            _status_str = _status_lookup.get(_status)

        message = f"[{_status_str}] {message}"

        if _message_level == 'debug':
            self.logger.debug(f"Job {self.job_id}: {message}")
        elif _message_level == 'info':
            self.logger.info(f"Job {self.job_id}: {message}")
        elif _message_level == 'warning':
            self.logger.warning(f"Job {self.job_id}: {message}")
        elif _message_level == 'error':
            self.logger.error(f"Job {self.job_id}: {message}")
        elif _message_level == 'critical':
            self.logger.critical(f"Job {self.job_id}: {message}")

        self.message = message

    @property
    def status(self):
        return self._status

    @property
    def status_str(self):
        return _status_lookup[self._status]

    def arguments_for_spawned_jobs(self) -> list[dict]:
        options = dict()

        parameters_with_all_value = [
            _key
            for _key in self.task.write_all
            if self.arguments.get(_key) == 'ALL'
        ]

        for _param_key in parameters_with_all_value:
            _parameter = self.task.parameter_dict.get(_param_key)
            if _parameter is None:
                self.update(status=4, message=f"Parameter {_param_key} not recognised")
                self.logger.error(f"Parameter {_param_key} not recognised")
                return []
            _arg_options = _parameter.select_from
            if len(_arg_options) == 0:
                self.update(status=4, message=f"Parameter {_param_key} has no options")
                self.logger.error(f"Parameter {_param_key} has no options")
                return []
            options[_param_key] = _arg_options

        if len(options) == 0:
            return []

        # create jobs with all combinations of options
        option_tuples = []
        for k, v in options.items():
            option_tuples.append([(k, sv) for sv in v])
        new_job_options = [{k: v for k, v in _tup} for _tup in [_p for _p in product(*option_tuples)]]
        new_args = []
        for _opt in new_job_options:
            _args = self.arguments.copy()
            _args.update(_opt)
            new_args.append(_args)

        return new_args

    @property
    def _db_rows(self) -> tuple[str, list[str]]:
        if self.collect_id is None:
            _collect_id = "NULL"
        else:
            _collect_id = f"'{self.collect_id}'"

        if self.message is None:
            _message = "NULL"
        else:
            _message = f"'{self.message}'"

        queue_row = f"('{self.job_id}', {_collect_id}, {self._status}, '{self._system_node_name}', {self._system_pid}, '{self._cmd_id}', '{datetime.datetime.utcnow()}', {self.priority}, '{self.task.type_key}', '{self.task.key}', {_message})"
        arg_rows = [f"('{self.job_id}', '{_k}', '{json.dumps(_v)}'::json)" for _k, _v in self.arguments.items()]

        return queue_row, arg_rows

    @property
    def _dependencies_sql_table_simple(self) -> str:
        selects = [
            f"SELECT '{_dep.type_key}' AS dependency_type_key, '{_dep.task_key}' AS dependency_task_key "
            for _dep in self.task.dependencies
        ]
        return " UNION ALL ".join(selects)

    @property
    def _dependencies_sql_table(self) -> str:
        selects = []
        for _dep in self.task.dependencies:
            args = [(_k, _v) for _k, _v in _dep.args_specified]
            for _k in _dep.args_same:
                if _k in self.arguments.keys():
                    args.append((_k, self.arguments[_k]))
                else:
                    raise ValueError(f"Dependency argument {_k} not found in job arguments")

            arg_rows = [f"ROW('{_k}', '{json.dumps(_v)}'::json)::jobmaster_argument" for _k, _v in args]
            arg_array = f"ARRAY[{', '.join(arg_rows)}]"

            sql = f"""
                    SELECT  '{self.task.type_key}' AS type_key,
                            '{self.task.key}' AS task_key,
                            '{_dep.type_key}' AS dependency_type_key,
                            '{_dep.task_key}' AS dependency_task_key,
                            {_dep.time} AS dependency_time,
                            {arg_array} AS arguments
                    """
            selects.append(sql)
        return " UNION ALL ".join(selects)

    def update(self,
               status: int,
               message: str = None,
               _execute: bool = True,
               _message_level: str = 'debug') -> TextClause | None:
        """
        Update the status of the job and write to the database.
        If the status is 0 (local), the arguments will also be written to the database,

        :param status:
        :param message:
        :param _execute:
        :param _message_level:
        :return:
        """

        if status not in _status_lookup.keys():
            raise ValueError(f"Invalid status: {status}")

        if message:
            self.set_message(message, _message_level=_message_level, _status=status)

        if status == self._status:
            return

        if self._status == 0:
            write_args = True
        else:
            write_args = False

        self._status = status
        self.logger.debug(f"Job {self.job_id} status set: {self.status_str}")

        queue_row, arg_rows = self._db_rows

        if write_args:
            sql = sqlalchemy.text(
                f"INSERT INTO {self._schema}.jobs "
                f"VALUES {queue_row}; "
                f"INSERT INTO {self._schema}.arguments "
                f"VALUES {', '.join(arg_rows)}; "
            )
        else:
            sql = sqlalchemy.text(
                f"INSERT INTO {self._schema}.jobs "
                f"VALUES {queue_row}; "
            )

        if _execute:
            with self._engine.connect() as conn:
                conn.execute(sql)
                conn.commit()
            return
        else:
            return sql

    def dependencies_in_queue(self) -> list[dict]:
        with self._engine.connect() as conn:
            _result = conn.execute(
                sqlalchemy.text(
                    f"""
                    WITH jobs_table AS (
                        SELECT *
                        FROM {self._schema}.current_jobs()
                        WHERE status_out IN (1,2,3)
                    ), deps_table AS (
                        {self._dependencies_sql_table_simple}
                    )

                    SELECT  dt.dependency_type_key as type_key,
                            dt.dependency_task_key as task_key,
                            jt.job_id_out as job_id,
                            jt.status_out as status,
                            jt.updated_at_out as updated_at,
                            jt.arguments_out as arguments
                    FROM deps_table dt
                    INNER JOIN jobs_table jt
                    ON dt.dependency_type_key = jt.type_key_out
                    AND dt.dependency_task_key = jt.task_key_out
                    """
                )
            )
            conn.commit()
        rows = _result.all()
        self.logger.debug(f"Dependencies in queue: {len(rows)}")
        if len(rows) > 0:
            results = [
                dict(
                    job_id=row.job_id,
                    status=row.status,
                    updated_at=row.updated_at,
                    type_key=row.type_key,
                    task_key=row.task_key,
                    arguments=utils.parse_argument_type(row.arguments)
                )
                for row in rows
            ]
        else:
            results = []

        return results

    def dependencies_specific(self) -> list[dict]:
        _deps = []
        for i, _dep in enumerate(self.task.dependencies):
            _d = dict(
                type_key=_dep.type_key,
                task_key=_dep.task_key,
                time=_dep.time
            )
            _args = dict()
            for _k in _dep.args_same:
                _args[_k] = self.arguments[_k]
            for _k, _v in _dep.args_specified:
                _args[_k] = _v
            _d['arguments'] = _args
            _deps.append(_d)
        return _deps

    def required_dependency_jobs(self):
        required = []
        for _dep in self.dependencies_specific():
            _dep_is_required = True
            for _qj in self.dependencies_in_queue():
                if _dep['type_key'] == _qj['type_key'] and _dep['task_key'] == _qj['task_key'] and all(
                        (k, v) in _qj['arguments'].items() for k, v in _dep['arguments'].items()):
                    if _qj['status'] in (1, 2):
                        # If the job is already in the queue (waiting/running), it is not required
                        _dep_is_required = False
                    elif _qj['status'] == 3:
                        # If the job is complete, check if it was completed within the required time
                        time_since_complete = (datetime.datetime.utcnow() - _qj['updated_at']).total_seconds() / 3600
                        if time_since_complete < _dep['time']:
                            # If the job was completed within the required time, it is not required
                            _dep_is_required = False
            if _dep_is_required:
                required.append(_dep)
        self.logger.debug(f"Required dependencies: {len(required)}")
        return required

    def safe_execute(self):
        self.logger.debug(f"Executing job {self.job_id}")
        _success = False
        try:
            result = self.task.function(**self.arguments)
            self.update(status=3, message=f"Executed at {datetime.datetime.utcnow()} with result: {str(result)}")
            _success = True
        except Exception as _error:
            self.update(status=4, message=f"Error at {datetime.datetime.utcnow()}: {_error}", _message_level='error')
        return _success
