__version__ = "0.1.0"
__author__ = 'Joshua Prettyman'
__credits__ = 'Blink SEO'

import functools
import datetime
import os
import uuid

import sqlalchemy

from .deploy import deploy
from .classes import Task, Dependency, SameParameter, Job
from . import utils

same = SameParameter()
__SYSTEM_NODE_NAME__ = os.uname().nodename.split('.')[0]
__SYSTEM_PID__ = os.getpid()
__JOB_MASTER_TASKS__ = dict()


def _get_task_type(type_key: str):
    if _job_type_dict := __JOB_MASTER_TASKS__.get(type_key):
        return _job_type_dict
    return dict()


def _get_task(type_key: str, job_key: str) -> Task | None:
    return __JOB_MASTER_TASKS__.get(type_key, dict()).get(job_key, None)


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


class JobMaster:
    def __init__(self,
                 db_engine: sqlalchemy.engine.base.Engine,
                 schema: str = 'jobmaster',
                 cmd_id: str = None,
                 system_process_units: int = 100_000,
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
        self._system_process_units = system_process_units

        self._dependencies_validated: bool = False

        if _validate_dependencies:
            self._validate_dependencies()

    @property
    def cmd_id(self):
        return self._cmd_id

    @property
    def system_process_units(self):
        return self._system_process_units

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

    def print_tasks(self):
        for _type_key, _task_key, _task in self.all_tasks():
            print(_task)
            print('')

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

    def deploy(self, reset: bool = False):

        if not self._dependencies_validated:
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

        _task = self.get((type_key, task_key))
        if not _task:
            raise ValueError(f"Task {type_key}.{task_key} not recognised")

        return Job(
            cmd_id=self._cmd_id,
            system_node_name=__SYSTEM_NODE_NAME__,
            system_pid=__SYSTEM_PID__,
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
                    f"WITH running_jobs AS ("
                    f"  SELECT type_key_out, task_key_out "
                    f"  FROM {self._schema}.current_jobs() "
                    f"  WHERE status_out = 2 "
                    f"  AND system_node_name_out = '{__SYSTEM_NODE_NAME__}' "
                    f") "
                    f"SELECT SUM(tsk.process_units) AS total_process_units "
                    f"FROM running_jobs "
                    f"LEFT JOIN {self._schema}.tasks tsk "
                    f"ON running_jobs.type_key_out = tsk.type_key "
                    f"AND running_jobs.task_key_out = tsk.task_key; "
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
        if not self._dependencies_validated:
            self._validate_dependencies()

        available_process_units = self.available_process_units()
        if available_process_units <= 0:
            self.logger.info("No available process units")
            return None

        collect_id: str = str(uuid.uuid4())
        with self._engine.connect() as conn:
            conn.execute(
                sqlalchemy.text(
                    f"LOCK TABLE {self._schema}.collect_ids IN ACCESS EXCLUSIVE MODE; "
                    f"INSERT INTO {self._schema}.collect_ids (collect_id) VALUES ('{collect_id}');"
                )
            )

            _result = conn.execute(
                sqlalchemy.text(
                    f"""
                    WITH waiting_jobs AS (
                        SELECT    job_id_out,
                                  type_key_out, 
                                  task_key_out, 
                                  status_out, 
                                  priority_out, 
                                  created_at_out, 
                                  arguments_out 
                        FROM {self._schema}.current_jobs() 
                        WHERE status_out = 1 
                    ) 
                    SELECT  wj.job_id_out,
                            wj.type_key_out, 
                            wj.task_key_out, 
                            wj.status_out, 
                            wj.priority_out, 
                            wj.created_at_out, 
                            wj.arguments_out,
                            tsk.process_units 
                    FROM waiting_jobs wj 
                    LEFT JOIN {self._schema}.tasks tsk 
                    ON wj.type_key_out = tsk.type_key 
                    AND wj.task_key_out = tsk.task_key 
                    WHERE tsk.process_units <= {available_process_units}
                    ORDER BY priority_out DESC, created_at_out ASC 
                    LIMIT 1; 
                    """
                )
            )
            results = _result.all()
            if len(results) == 0:
                self.logger.info("Queue is empty. No jobs to run.")
                new_job = None
            else:
                top_job_out = results[0]
                self.logger.info(f"Found a job to run: {top_job_out.job_id_out}")
                new_job = self.job(
                    job_id=top_job_out.job_id_out,
                    collect_id=collect_id,
                    type_key=top_job_out.type_key_out,
                    task_key=top_job_out.task_key_out,
                    status=top_job_out.status_out,
                    priority=top_job_out.priority_out,
                    created_at=top_job_out.created_at_out,
                    arguments=top_job_out.arguments_out
                )
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

        if not self._dependencies_validated:
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
