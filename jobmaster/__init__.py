import json
import functools
import datetime
import os

from uuid import uuid4
from typing import Callable
from itertools import product

import sqlalchemy
from sqlalchemy.sql.elements import TextClause

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
    
    @property
    def active(self):
        return self._status in (0, 1, 2)

    def set_message(self, message: str):
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

        queue_row = f"('{self.job_id}', {_collect_id}, {self._status}, '{__SYSTEM_NODE_NAME__}', {__SYSTEM_PID__}, '{self._cmd_id}', '{datetime.datetime.utcnow()}', {self.priority}, '{self.task.type_key}', '{self.task.key}', {_message})"
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

    def update(self, status: int, message: str = None, _execute: bool = True) -> TextClause | None:
        """
        Update the status of the job and write to the database.
        If the status is 0 (local), the arguments will also be written to the database,

        :param status:
        :param message:
        :param _execute:
        :return:
        """
        if message:
            self.set_message(message)

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

    def dependencies_in_queue(self) -> list[dict]:
        with self._engine.connect() as conn:
            _result = conn.execute(
                sqlalchemy.text(
                    f"""
                    WITH jobs_table AS (
                        SELECT *
                        FROM {self._schema}.current_jobs()
                        WHERE status_out IN [1,2,3]
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
                    LEFT JOIN jobs_table jt
                    ON dt.dependency_type_key = jt.type_key_out
                    AND dt.dependency_task_key = jt.task_key_out
                    """
                )
            )
            conn.commit()

        results = [
            dict(
                job_id=row.job_id,
                status=row.status,
                updated_at=row.updated_at,
                time=(datetime.datetime.utcnow() - row.updated_at).hours,
                type_key=row.type_key,
                task_key=row.task_key,
                arguments=utils.parse_argument_type(row.arguments)
            )
            for row in _result.all()
        ]

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
            for _qj in self.dependencies_in_queue():
                if _dep['type_key'] == _qj['type_key'] and _dep['task_key'] == _qj['task_key'] and all((k, v) in _qj['arguments'].items() for k, v in _dep['arguments'].items()):
                    if _qj['status'] in (1, 2):
                        continue
                    elif _qj['status'] == 3 and _qj['time'] < _dep['time']:
                        continue
                    else:
                        required.append(_dep)
        return required

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
            task_path_lookup[_task.module+'.'+_task.key] = (_type_key, _task_key)

        # print('\n'+'*' * 20)
        # print('task_path_lookup:')
        # print(task_path_lookup)
        # print('*'*20 + '\n')

        for _type_key, _task_key, _task in self.all_tasks():
            for _dep in _task.dependencies:
                if _dep.type_key is None:
                    __type, __task = task_path_lookup.get(_dep.task_func_module+'.'+_dep.task_key, (None, None))
                    _dep.set_type_key(__type)
                if not self.get((_dep.type_key, _dep.task_key)):
                    raise ValueError(f"Dependency {_dep.type_key}.{_dep.task_key} for "
                                     f"task {_type_key}.{_task_key} not recognised")

        self._dependencies_validated = True

    def get(self, key):
        return self.__getitem__(key)

    def write_tasks(self):
        """
        Writes all tasks to the database. This will overwrite any existing tasks.
        Task data is written to the tables `tasks` and `parameters` in the schema specified at initialisation.
        """

        if not self._dependencies_validated:
            self._validate_dependencies()

        statements = []

        _sql = sqlalchemy.text(
            f"""
            TRUNCATE {self._schema}.tasks, 
                     {self._schema}.parameters, 
                     {self._schema}.dependencies,
                     {self._schema}.dependency_args_specified,
                     {self._schema}.dependency_args_same;
            """
        )

        statements.append(_sql)

        task_rows = ["("+_task.db_row+")" for _type_key, _task_key, _task in self.all_tasks()]
        if len(task_rows) > 0:
            _sql = sqlalchemy.text(f"INSERT INTO {self._schema}.tasks \nVALUES \n" + ", \n".join(task_rows) + ";")
            statements.append(_sql)

        raw_param_rows = [_task.db_parameter_rows for _type_key, _task_key, _task in self.all_tasks()]
        param_rows = []
        for _l in raw_param_rows:
            param_rows.extend(["("+_r+")" for _r in _l])

        if len(param_rows) > 0:
            _sql = sqlalchemy.text(f"INSERT INTO {self._schema}.parameters \nVALUES \n" + ", \n".join(param_rows) + ";")
            statements.append(_sql)

        raw_dep_rows = [_task.db_dependency_rows for _type_key, _task_key, _task in self.all_tasks()]
        dep_rows = []
        dep_arg_same_rows = []
        dep_arg_specified_rows = []
        for _tup in raw_dep_rows:
            dep_rows.extend(["(" + _r + ")" for _r in _tup[0]])
            dep_arg_same_rows.extend(["(" + _r + ")" for _r in _tup[1]])
            dep_arg_specified_rows.extend(["(" + _r + ")" for _r in _tup[2]])

        if len(dep_rows) > 0:
            _sql = sqlalchemy.text(f"INSERT INTO {self._schema}.dependencies \nVALUES \n" + ", \n".join(dep_rows) + ";")
            statements.append(_sql)
        if len(dep_arg_same_rows) > 0:
            _sql = sqlalchemy.text(f"INSERT INTO {self._schema}.dependency_args_same \nVALUES \n" + ", \n".join(dep_arg_same_rows) + ";")
            statements.append(_sql)
        if len(dep_arg_specified_rows) > 0:
            _sql = sqlalchemy.text(f"INSERT INTO {self._schema}.dependency_args_specified \nVALUES \n" + ", \n".join(dep_arg_specified_rows) + ";")
            statements.append(_sql)

        with self._engine.connect() as conn:
            for _sql in statements:
                conn.execute(_sql)
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

    def update(self, jobs: Job | list[Job], status: int):
        """
        Performs the same function as Job.update(), but for potentially multiple jobs.
        If the status of a job is 0 (local), the arguments will be written to the database,
        otherwise only the job status will be updated.
        
        :param jobs: a Job or list of Jobs
        :param status: The status to update to
        :return: 
        """
        
        if isinstance(jobs, Job):
            jobs = [jobs]

        queue_rows = []
        arg_rows = []
        for _j in jobs:
            _old_status = _j._status
            _j.update(status, _execute=False)
            _q, _a = _j._db_rows
            queue_rows.append(_q)
            if _old_status == 0:
                arg_rows.extend(_a)

        with self._engine.connect() as conn:
            _queue_values = ', \n'.join(queue_rows)
            conn.execute(sqlalchemy.text(f"INSERT INTO {self._schema}.jobs \nVALUES \n{_queue_values};"))
            if arg_rows:
                _arg_values = ', \n'.join(arg_rows)
                conn.execute(sqlalchemy.text(f"INSERT INTO {self._schema}.arguments \nVALUES \n{_arg_values};"))
            conn.commit()

    def execute(self, job: Job) -> Job:
        # update the job status to "running" if not already
        job.update(2)

        # first check "write all" parameters
        new_job_args = job.arguments_for_spawned_jobs()
        if len(new_job_args) > 0:
            # create jobs with all combinations of options
            new_jobs = [
                self.job(
                    type_key=job.task.type_key,
                    task_key=job.task.key,
                    priority=job.priority,
                    arguments=_args
                )
                for _args in new_job_args
            ]
            # write jobs to database
            self.update(new_jobs, status=1)
            # Update this job to "completed"
            job.update(status=3, message=f"Created {len(new_jobs)} new jobs with all options")

        if job.status != 2:
            return job

        # then check dependencies
        new_job_specs = job.required_dependency_jobs()
        if len(new_job_args) > 0:
            # create jobs to satisfy dependencies with higher priority
            new_jobs = [
                self.job(
                    type_key=_nj['type_key'],
                    task_key=_nj['task.key'],
                    priority=job.priority + 1,
                    arguments=_nj['arguments']
                )
                for _nj in new_job_specs
            ]
            # write jobs to database
            self.update(new_jobs, status=1)
            # Update this job to "waiting"
            job.update(status=1, message=f"Back in queue. Created {len(new_jobs)} new jobs to satisfy dependencies")

        if job.status != 2:
            return job

        # execute job
        job.safe_execute()
        return job
