import json
from typing import Callable
from itertools import product
import sqlalchemy
import functools
import datetime
import os
from uuid import uuid4

from .deploy import deploy
from .classes import Task, Dependency, SameParameter
from . import utils

same = SameParameter()

__SYSTEM_NODE_NAME__ = os.uname().nodename.split('.')[0]
__SYSTEM_PID__ = os.getpid()

__JOB_MASTER_TASKS__ = dict()

_status_lookup = {
    0: 'local',
    1: 'waiting',
    2: 'running',
    3: 'complete',
    4: 'failed',
    5: 'cancelled'
}


def _get_task_type(type_key: str):
    if _job_type_dict := __JOB_MASTER_TASKS__.get(type_key):
        return _job_type_dict
    return dict()


def _get_task(type_key: str, job_key: str) -> Task | None:
    return __JOB_MASTER_TASKS__.get(type_key, dict()).get(job_key, None)


def task(_func=None, *,
         write_all: str | list[str] = None,
         type_key: str = None,
         process_limit: int = None,
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
            process_limit=process_limit,
            dependencies=dependencies
        )
        entry = __JOB_MASTER_TASKS__.get(_task.type_key, {}).get(_task.key)
        if entry is not None and entry != _task:
            raise ValueError(f"A different task with the same signature ({_task.type_key}.{_task.key}) already exists")
        else:
            if __JOB_MASTER_TASKS__.get(_task.type_key):
                __JOB_MASTER_TASKS__[_task.type_key][_task.key] = _task
            else:
                __JOB_MASTER_TASKS__[_task.type_key] = {_task.key: _task}

        @functools.wraps(func)
        def func_wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        return func_wrapper

    if _func is None:
        return decorator_task
    else:
        return decorator_task(_func)


class Job:
    def __init__(self,
                 cmd_id: str,
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
                 message: str = None):

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

        self.task = task
        self.priority = priority

        self._status = status
        if created_at is None:
            created_at = datetime.datetime.utcnow()
        self.created_at = created_at

        self.collect_id = collect_id
        self.message = message

    def __bool__(self):
        return True

    def set_message(self, message: str):
        self.message = message

    @property
    def status(self):
        return _status_lookup[self._status]

    @property
    def parameters_with_all_value(self):
        return [_key for _key in self.task.write_all if self.arguments.get(_key) == 'ALL']

    def arguments_for_spawned_jobs(self) -> list[dict]:
        options = dict()
        for _param_key in self.parameters_with_all_value:
            _parameter = self.task.parameter_dict.get(_param_key)
            if _parameter is None:
                self.set_message(f"Parameter {_param_key} not recognised")
                self.update(4)
                self.logger.error(f"Parameter {_param_key} not recognised")
                return []
            _arg_options = _parameter.select_from
            if len(_arg_options) == 0:
                self.set_message(f"Parameter {_param_key} has no options")
                self.update(4)
                self.logger.error(f"Parameter {_param_key} has no options")
                return []
            options[_param_key] = _arg_options

        if len(options) == 0:
            return []

        # create jobs with all combinations of options
        opt_tups = []
        for k, v in options.items():
            opt_tups.append([(k, sv) for sv in v])
        new_job_options = [{k: v for k, v in _tup} for _tup in [_p for _p in product(*opt_tups)]]
        new_args = []
        for _opt in new_job_options:
            _args = self.arguments.copy()
            _args.update(_opt)
            new_args.append(_args)

        return new_args

    @property
    def _db_rows(self):
        if self.collect_id is None:
            _collect_id = "NULL"
        else:
            _collect_id = f"'{self.collect_id}'"

        if self.message is None:
            _message = "NULL"
        else:
            _message = f"'{self.message}'"

        queue_row = f"('{self.job_id}', {_collect_id}, {self._status}, '{__SYSTEM_NODE_NAME__}', {__SYSTEM_PID__}, '{self._cmd_id}', '{datetime.datetime.utcnow()}', {self.priority}, '{self.task.type_key}', '{self.task.key}', {_message})"
        arg_rows = [f"('{self.job_id}', '{_k}', '{json.dumps(_v)}')" for _k, _v in self.arguments.items()]

        return queue_row, arg_rows

    def update(self, status: int, _execute: bool = True):
        if status == self._status:
            return

        if status not in _status_lookup.keys():
            raise ValueError(f"Invalid status: {status}")

        if self._status == 0:
            write_args = True
        else:
            write_args = False

        self._status = status

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

    @property
    def dependencies_sql_table(self):
        selects = []
        for _dep in self.task.dependencies:
            selects.append(
                f"""
                SELECT  '{self.task.type_key}' AS type_key,
                        '{self.task.key}' AS task_key,
                        '{_dep.type_key}' AS dependency_type_key,
                        '{_dep.task_key}' AS dependency_task_key,
                        {_dep.time} AS time
                """
            )
        return " UNION ALL ".join(selects)

    def safe_execute(self):
        _success = False
        try:
            result = self.task.function(**self.arguments)
            self.message = f"Executed at {datetime.datetime.utcnow()} with result: {str(result)}"
            self.update(3)
            _success = True
        except Exception as _error:
            self.message = f"Error at {datetime.datetime.utcnow()}: {_error}"
            self.update(4)
        return _success


class JobMaster:
    def __init__(self,
                 db_engine: sqlalchemy.engine.base.Engine,
                 schema: str = 'jobmaster',
                 cmd_id: str = None,
                 logger=None):

        if logger is None:
            self.logger = utils.NothingLogger
        elif logger == print:
            self.logger = utils.StdLogger
        elif all(hasattr(logger, _attr) for _attr in ('debug', 'info', 'warning', 'error', 'critical')):
            self.logger = logger
        else:
            raise ValueError("Invalid logger")

        self._engine = db_engine
        self._schema = schema
        self._cmd_id = cmd_id or datetime.datetime.utcnow().isoformat()

        self._dependencies_validated: bool = False

    @property
    def cmd_id(self):
        return self._cmd_id

    def __getitem__(self, key: str | tuple[str, str]):
        if isinstance(key, str):
            if '.' in key:
                key = tuple(key.split('.'))
            else:
                return _get_task_type(type_key=key)

        if isinstance(key, tuple):
            if len(key) == 2:
                return _get_task(type_key=key[0], job_key=key[1])

        raise ValueError(f"Invalid key: {key}")

    @staticmethod
    def all_tasks():
        for _type_key, _task_dict in __JOB_MASTER_TASKS__.items():
            for _task_key, _task in _task_dict.items():
                yield _type_key, _task_key, _task

    @property
    def tasks_keys(self) -> list[tuple[str, str]]:
        return [
            (_type_key, _task_key)
            for _type_key, _task_dict in __JOB_MASTER_TASKS__.items()
            for _task_key in _task_dict.keys()
        ]

    def _validate_dependencies(self):
        task_path_lookup = dict()
        for _type_key, _task_key, _task in self.all_tasks():
            task_path_lookup[_task.task_func_source] = (_type_key, _task_key)

        print('\n'+'*' * 20)
        print('task_path_lookup:')
        print(task_path_lookup)
        print('*'*20 + '\n')

        for _type_key, _task_key, _task in self.all_tasks():
            for _dep in _task.dependencies:
                if _dep.type_key is None:
                    print(_dep.task_func_source)
                    __type, __task = task_path_lookup.get(_dep.task_func_source, (None, None))
                    print(__type, __task)
                    _dep.set_type_key(__type)
                if not self.get((_dep.type_key, _dep.task_key)):
                    raise ValueError(f"Dependency {_dep.type_key}.{_dep.task_key} for "
                                     f"task {_type_key}.{_task_key} not recognised")

        self._dependencies_validated = True

    def check_dependencies(self, job: Job):
        pass

    def get(self, key):
        return self.__getitem__(key)

    def write_tasks(self):
        """
        Writes all tasks to the database. This will overwrite any existing tasks.
        Task data is written to the tables `tasks` and `parameters` in the schema specified at initialisation.
        """

        if not self._dependencies_validated:
            self._validate_dependencies()

        sql0 = sqlalchemy.text(
            f"""
            TRUNCATE {self._schema}.tasks, 
                     {self._schema}.parameters, 
                     {self._schema}.dependencies,
                     {self._schema}.dependency_args_specified,
                     {self._schema}.dependency_args_same;
            """
        )

        task_rows = ["("+_task.db_row+")" for _type_key, _task_key, _task in self.all_tasks()]
        sql1 = sqlalchemy.text(f"INSERT INTO {self._schema}.tasks \nVALUES \n" + ", \n".join(task_rows) + ";")

        raw_param_rows = [_task.db_parameter_rows for _type_key, _task_key, _task in self.all_tasks()]
        param_rows = []
        for _l in raw_param_rows:
            param_rows.extend(["("+_r+")" for _r in _l])
        sql2 = sqlalchemy.text(f"INSERT INTO {self._schema}.parameters \nVALUES \n" + ", \n".join(param_rows) + ";")

        raw_dep_rows = [_task.db_dependency_rows for _type_key, _task_key, _task in self.all_tasks()]
        dep_rows = []
        dep_arg_same_rows = []
        dep_arg_specified_rows = []
        for _tup in raw_dep_rows:
            dep_rows.extend(["(" + _r + ")" for _r in _tup[0]])
            dep_arg_same_rows.extend(["(" + _r + ")" for _r in _tup[1]])
            dep_arg_specified_rows.extend(["(" + _r + ")" for _r in _tup[2]])

        sql3 = sqlalchemy.text(f"INSERT INTO {self._schema}.dependencies \nVALUES \n" + ", \n".join(dep_rows) + ";")
        sql4 = sqlalchemy.text(f"INSERT INTO {self._schema}.dependency_args_same \nVALUES \n" + ", \n".join(dep_arg_same_rows) + ";")
        sql5 = sqlalchemy.text(f"INSERT INTO {self._schema}.dependency_args_specified \nVALUES \n" + ", \n".join(dep_arg_specified_rows) + ";")

        with self._engine.connect() as conn:
            conn.execute(sql0)
            conn.execute(sql1)
            conn.execute(sql2)
            conn.execute(sql3)
            conn.execute(sql4)
            conn.execute(sql5)
            conn.commit()

    def deploy(self, reset: bool = False):

        if not self._dependencies_validated:
            self._validate_dependencies()

        deploy(self._engine, self._schema, reset=reset)
        self.write_tasks()

    def job(self,
            type_key: str,
            task_key: str,
            priority: int = 0,
            status: int = 0,
            arguments: dict = None,
            job_id: str = None,
            created_at: datetime.datetime = None,
            collect_id: str = None,
            message: str = None):

        task = self.get((type_key, task_key))
        if not task:
            raise ValueError(f"Task {type_key}.{task_key} not recognised")

        return Job(
            cmd_id=self._cmd_id,
            db_engine=self._engine,
            db_schema=self._schema,
            task=task,
            priority=priority,
            status=status,
            arguments=arguments,
            job_id=job_id,
            created_at=created_at,
            collect_id=collect_id,
            message=message,
            logger=self.logger
        )

    def write_job(self, job: Job | list[Job], write_args: bool = False):
        if isinstance(job, Job):
            job = [job]

        queue_rows = []
        arg_rows = []

        for _j in job:
            _q, _a = _j._db_rows
            queue_rows.append(_q)
            arg_rows.extend(_a)

        with self._engine.connect() as conn:
            _queue_values = ', \n'.join(queue_rows)
            _arg_values = ', \n'.join(arg_rows)
            conn.execute(sqlalchemy.text(f"INSERT INTO {self._schema}.jobs \nVALUES \n{_queue_values};"))
            if write_args:
                conn.execute(sqlalchemy.text(f"INSERT INTO {self._schema}.arguments \nVALUES \n{_arg_values};"))
            conn.commit()

    def current_jobs(self):
        with self._engine.connect() as conn:
            return conn.execute(sqlalchemy.text(f"SELECT * FROM {self._schema}.jobs")).fetchall()

    def execute(self, job: Job):
        # first check "write all" parameters
        options = job.arguments_for_spawned_jobs()

        if len(options) > 0:
            # create jobs with all combinations of options
            # with status 1
            new_jobs = [
                self.job(
                    type_key=job.task.type_key,
                    task_key=job.task.key,
                    priority=job.priority + 1,
                    arguments=_args,
                    status=1
                )
                for _args in options
            ]
            # write jobs to database
            self.write_job(new_jobs, write_args=True)
            # Update this job to "completed"
            job.set_message(f"Created {len(new_jobs)} jobs with all options")
            job.update(3)
            return True

        # then check dependencies
        # TODO: fill in this part!

        # execute job
        job.safe_execute()
