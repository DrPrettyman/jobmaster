import datetime
import json
import os
import uuid
import sqlalchemy
from uuid import uuid4
from typing import Callable
from itertools import product
from sqlalchemy.sql.elements import TextClause

from . import utils
from . import tasks
from .decorator import __TASKMASTER__
from .deploy import deploy


__SYSTEM_NODE_NAME__ = os.uname().nodename.split('.')[0]
__SYSTEM_PID__ = os.getpid()
_status_lookup = {
    0: 'local',
    1: 'waiting',
    2: 'running',
    3: 'complete',
    4: 'failed',
    5: 'cancelled'
}


class Job:
    def __init__(self,
                 cmd_id: str,
                 db_engine: sqlalchemy.engine.base.Engine,
                 db_schema: str,
                 task: tasks.Task,
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
            _m = self.message.replace("'", "`")
            _message = f"'{_m}'"

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
            for _k, _s in _dep.args_same:
                if _k in self.arguments.keys():
                    args.append((_k, _s.value(self.arguments[_k])))
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
            for _k, _sp in _dep.args_same:
                if _k in self.arguments.keys():
                    try:
                        _args[_k] = _sp.value(self.arguments[_k])
                    except ValueError:
                        raise ValueError(f"Dependency parameter '{_k}' value not valid")
                else:
                    raise ValueError(f"Dependency parameter '{_k}' not found in job arguments")
            for _k, _v in _dep.args_specified:
                _args[_k] = _v
            _d['arguments'] = _args
            _deps.append(_d)
        return _deps

    def required_dependency_jobs(self):
        required = []
        try:
            dependencies_specific = self.dependencies_specific()
        except ValueError:
            self.update(status=4, message="Error parsing dependencies", _message_level='error')
            return []
        for _dep in dependencies_specific:
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
            result = self.task.execute(**self.arguments)
            self.update(status=3, message=f"Executed at {datetime.datetime.utcnow()} with result: {result}")
            _success = True
        except tasks.MissingArgument as _error:
            self.update(status=4, message=f"Missing argument: {_error.arg_key}", _message_level='error')
        except tasks.MissingClassArgument as _error:
            self.update(status=4, message=f"Missing class argument: {_error.arg_key}", _message_level='error')
        except Exception as _error:
            self.update(status=4, message=f"Error at {datetime.datetime.utcnow()}: {_error.message}", _message_level='error')
        return _success


class JobMaster:
    def __init__(self,
                 db_engine: sqlalchemy.engine.base.Engine,
                 schema: str = 'jobmaster',
                 cmd_id: str = None,
                 system_process_units: int = None,
                 logger=None,
                 _validate_dependencies: bool = False):

        if logger is None:
            self.logger = utils.NothingLogger()
        elif logger == print:
            self.logger = utils.StdLogger()
        elif all(hasattr(logger, _attr) for _attr in ('debug', 'info', 'warning', 'error', 'critical')):
            self.logger = logger
        else:
            raise ValueError("Invalid logger")

        self._engine = db_engine
        self._schema = schema
        self._cmd_id = cmd_id or datetime.datetime.utcnow().isoformat()

        if system_process_units is None:
            _ = os.environ.get('JOBMASTER_SYSTEM_PROCESS_UNITS', '10_000')
            try:
                system_process_units = int(_)
            except ValueError:
                system_process_units = 10_000

        assert isinstance(system_process_units, int), "system_process_units must be an integer"

        self._system_process_units = system_process_units

        if _validate_dependencies:
            self._validate_dependencies()

    @property
    def cmd_id(self):
        return self._cmd_id

    @property
    def system_process_units(self):
        return self._system_process_units

    def __getitem__(self, key: str | tuple[str, str]):
        return __TASKMASTER__[key]

    @property
    def dependencies_validated(self) -> bool:
        return __TASKMASTER__.dependencies_validated

    @staticmethod
    def _validate_dependencies():
        __TASKMASTER__.validate_dependencies()

    @staticmethod
    def print_tasks():
        __TASKMASTER__.print_tasks()

    def write_tasks(self):
        """
        Writes all tasks to the database. This will overwrite any existing tasks.
        Task data is written to the tables `tasks` and `parameters` in the schema specified at initialisation.
        """

        if not self.dependencies_validated:
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

        task_rows = ["(" + _task.db_row + ")" for _type_key, _task_key, _task in __TASKMASTER__.all_tasks()]
        if len(task_rows) > 0:
            _sql = sqlalchemy.text(f"INSERT INTO {self._schema}.tasks \nVALUES \n" + ", \n".join(task_rows) + ";")
            statements.append(_sql)

        raw_param_rows = [_task.db_parameter_rows for _type_key, _task_key, _task in __TASKMASTER__.all_tasks()]
        param_rows = []
        for _l in raw_param_rows:
            param_rows.extend(["(" + _r + ")" for _r in _l])

        if len(param_rows) > 0:
            _sql = sqlalchemy.text(f"INSERT INTO {self._schema}.parameters \nVALUES \n" + ", \n".join(param_rows) + ";")
            statements.append(_sql)

        raw_dep_rows = [_task.db_dependency_rows for _type_key, _task_key, _task in __TASKMASTER__.all_tasks()]
        dep_rows = []
        dep_arg_same_rows = []
        dep_arg_specified_rows = []
        for _tup in raw_dep_rows:
            dep_rows.extend(["(" + _r + ")" for _r in _tup[0]])
            dep_arg_same_rows.extend(["(" + _r + ")" for _r in _tup[1]])
            dep_arg_specified_rows.extend(["(" + _r + ")" for _r in _tup[2]])

        if len(dep_rows) > 0:
            _sql = sqlalchemy.text(
                f"INSERT INTO {self._schema}.dependencies \nVALUES \n" + ", \n".join(dep_rows) + ";"
            )
            statements.append(_sql)
        if len(dep_arg_same_rows) > 0:
            _sql = sqlalchemy.text(
                f"INSERT INTO {self._schema}.dependency_args_same \nVALUES \n" + ", \n".join(dep_arg_same_rows) + ";"
            )
            statements.append(_sql)
        if len(dep_arg_specified_rows) > 0:
            _sql = sqlalchemy.text(
                f"INSERT INTO {self._schema}.dependency_args_specified \nVALUES \n"
                + ", \n".join(dep_arg_specified_rows) + ";"
            )
            statements.append(_sql)

        with self._engine.connect() as conn:
            for _sql in statements:
                conn.execute(_sql)
            conn.commit()

        self.logger.info("Written tasks to database")

    def deploy(self, reset: bool = False):

        if not self.dependencies_validated:
            self._validate_dependencies()

        deploy(self._engine, self._schema, reset=reset)
        if reset:
            self.logger.info("Reset JobMaster database")
        self.logger.info("Deployed JobMaster database")
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
            message: str = None,
            fill_args: bool = False
            ) -> Job:

        _task = __TASKMASTER__[(type_key, task_key)]
        if not _task:
            raise ValueError(f"Task {type_key}.{task_key} not recognised")

        return Job(
            cmd_id=self._cmd_id,
            db_engine=self._engine,
            db_schema=self._schema,
            task=_task,
            priority=priority,
            status=status,
            arguments=arguments,
            job_id=job_id,
            created_at=created_at,
            collect_id=collect_id,
            message=message,
            logger=self.logger,
            fill_args=fill_args
        )

    def jobs_in_queue(self) -> list[Job]:
        """
        Simply returns all jobs in the queue, ordered by "updated_at" time.

        :return:
        """
        with self._engine.connect() as conn:
            _result = conn.execute(
                sqlalchemy.text(
                    f"SELECT * "
                    f"FROM {self._schema}.current_jobs() "
                    f"ORDER BY updated_at_out DESC "
                )
            )
            conn.commit()

        results = _result.all()

        if len(results) == 0:
            self.logger.info("No jobs in queue")
            return []

        self.logger.info(f"{len(results)} jobs in queue")
        _out = [
            self.job(
                job_id=_r.job_id_out,
                type_key=_r.type_key_out,
                task_key=_r.task_key_out,
                status=_r.status_out,
                priority=_r.priority_out,
                created_at=_r.created_at_out,
                arguments=_r.arguments_out
            )
            for _r in results
        ]

        return _out

    def available_process_units(self) -> int:
        with self._engine.connect() as conn:
            _processes_result = conn.execute(
                sqlalchemy.text(
                    f"""
                    WITH running_jobs AS (
                      SELECT type_key_out, task_key_out 
                      FROM {self._schema}.current_jobs() 
                      WHERE status_out = 2 
                      AND system_node_name_out = '{__SYSTEM_NODE_NAME__}' 
                    ) 
                    SELECT SUM(tsk.process_units) AS total_process_units 
                    FROM running_jobs 
                    LEFT JOIN {self._schema}.tasks tsk 
                    ON running_jobs.type_key_out = tsk.type_key 
                    AND running_jobs.task_key_out = tsk.task_key; 
                """
                )
            )
            conn.commit()

        _ = _processes_result.all()
        _process_units_in_use = None
        if len(_) > 0:
            _process_units_in_use = _[0].total_process_units
        if _process_units_in_use is None:
            _process_units_in_use = 0
        return self._system_process_units - _process_units_in_use

    def queue_pop(self) -> Job | None:
        """
        Pop the top job from the queue and return it as a Job object.
        """
        if not self.dependencies_validated:
            self._validate_dependencies()

        # Taking out the lines below because the check on process units
        # is now within the main sql query.

        # available_process_units = self.available_process_units()
        # if available_process_units <= 0:
        #     self.logger.info("No available process units")
        #     return None

        collect_id: str = str(uuid.uuid4())
        with self._engine.connect() as conn:
            # first we lock the collect_id table, this prevents other instances trying to run the queue_pop() method
            conn.execute(
                sqlalchemy.text(
                    f"LOCK TABLE {self._schema}.collect_ids IN ACCESS EXCLUSIVE MODE; "
                    f"INSERT INTO {self._schema}.collect_ids (collect_id) VALUES ('{collect_id}');"
                )
            )
            _result = conn.execute(
                sqlalchemy.text(
                    f"""
                    WITH waiting_and_running_jobs AS (
                        SELECT    job_id_out AS job_id,
                                  type_key_out AS type_key, 
                                  task_key_out AS task_key, 
                                  status_out AS status, 
                                  priority_out AS priority, 
                                  created_at_out AS created_at, 
                                  arguments_out AS arguments,
                                  system_node_name_out AS system_node_name 
                        FROM {self._schema}.current_jobs() 
                        WHERE status_out in (1, 2) 
                    ), waiting_jobs AS (
                        SELECT   job_id, type_key, task_key, status, priority, created_at, arguments
                        FROM     waiting_and_running_jobs
                        WHERE    status = 1
                    ), jobs_running_on_system AS (
                        SELECT   type_key, task_key
                        FROM     waiting_and_running_jobs
                        WHERE    status = 2
                        AND      system_node_name = '{__SYSTEM_NODE_NAME__}' 
                    ), p_units AS (
                        SELECT  SUM(tsk.process_units) AS total_process_units 
                        FROM    jobs_running_on_system jsys
                        LEFT JOIN {self._schema}.tasks tsk 
                        ON  jsys.type_key = tsk.type_key 
                        AND jsys.task_key = tsk.task_key
                    )
                    SELECT  wj.job_id,
                            wj.type_key, 
                            wj.task_key, 
                            wj.status, 
                            wj.priority, 
                            wj.created_at, 
                            wj.arguments,
                            tsk.process_units 
                    FROM waiting_jobs wj 
                    LEFT JOIN {self._schema}.tasks tsk 
                    ON  wj.type_key = tsk.type_key 
                    AND wj.task_key = tsk.task_key 
                    WHERE tsk.process_units <= {self._system_process_units} - COALESCE((SELECT total_process_units FROM p_units), 0)
                    ORDER BY priority DESC, created_at ASC 
                    LIMIT 1; 
                    """
                )
            )
            # Create a new job from the result
            results = _result.all()
            if len(results) == 0:
                self.logger.info("Queue is empty. No jobs to run.")
                new_job = None
            else:
                top_job_out = results[0]
                self.logger.info(f"Found a job to run: {top_job_out.job_id}")
                new_job = self.job(
                    job_id=top_job_out.job_id,
                    collect_id=collect_id,
                    type_key=top_job_out.type_key,
                    task_key=top_job_out.task_key,
                    status=top_job_out.status,
                    priority=top_job_out.priority,
                    created_at=top_job_out.created_at,
                    arguments=top_job_out.arguments
                )
                # Execute the update status for this job within this same transaction,
                # while the collect_ids table is still locked.
                sql = new_job.update(status=2, message="Popped from queue", _execute=False)
                conn.execute(sql)
            conn.commit()
        return new_job

    def update(self, jobs: Job | list[Job], status: int, message: str = None, _message_level: str = 'debug'):
        """
        Performs the same function as Job.update(), but for potentially multiple jobs.
        If the status of a job is 0 (local), the arguments will be written to the database,
        otherwise only the job status will be updated.

        :param jobs: a Job or list of Jobs
        :param status: The status to update to - this will be the same for all if updating multiple jobs
        :param message: A message to write to the database - this will be the same for all if updating multiple jobs
        :param _message_level: The logging level to use for the message
        :return:
        """

        if isinstance(jobs, Job):
            jobs = [jobs]

        queue_rows = []
        arg_rows = []
        for _j in jobs:
            _old_status = _j.status
            _j.update(status=status, message=message, _execute=False, _message_level=_message_level)
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
        """
        For a given job, check if it can be executed.
        If the job contains any 'ALL' arguments, we create a bunch of new jobs with all combinations of options.
        If the job has dependencies, we create new jobs to satisfy them.
        If neither of the above, we execute the job.

        :param job: a Job object
        :return: the updated Job object
        """

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
            self.update(new_jobs, status=1, message=f"Batch-spawned from job {job.job_id}")
            # Update this job to "completed"
            job.update(status=3, message=f"Created {len(new_jobs)} new jobs with all options")

        if job.status != 2:
            return job

        # then check dependencies
        new_job_specs = job.required_dependency_jobs()
        if len(new_job_specs) > 0:
            self.logger.debug(f"Creating {len(new_job_specs)} new jobs to satisfy dependencies")
            # create jobs to satisfy dependencies with higher priority
            new_jobs = [
                self.job(
                    type_key=_nj['type_key'],
                    task_key=_nj['task_key'],
                    priority=job.priority + 1,
                    arguments=_nj['arguments'],
                    fill_args=True
                )
                for _nj in new_job_specs
            ]
            # write jobs to database
            self.update(new_jobs, status=1, message=f"Created to satisfy dependencies of job {job.job_id}")
            # Update this job back to "waiting"
            job.update(status=1, message=f"Back in queue. Created {len(new_jobs)} new jobs to satisfy dependencies")

        if job.status != 2:
            self.logger.debug(f"Job {job.job_id} not ready to execute")
            return job

        # execute job
        job.safe_execute()
        return job

    def run(self) -> list[Job]:
        """
        Keep popping jobs from the queue and executing them until
        the queue is empty.

        :return:
        """

        if not self.dependencies_validated:
            self._validate_dependencies()
        jobs = []
        while True:
            job = self.queue_pop()
            if job is None:
                break
            _j = self.execute(job)
            jobs.append(_j)
        self.logger.info(f"Ran {len(jobs)} jobs.")
        return jobs

    def __call__(self) -> list[Job]:
        return self.run()



