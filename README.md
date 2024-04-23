# JobMaster

JobMaster is a simple job scheduling library for Python. 
It works with any PostgreSQL database, and is designed to be simple to use and easy to integrate into your existing codebase.

## Installation

Not yet publised on pip.

## Usage

Any function in your project can become a JobMaster Task by using the `@task` decorator.
For example, you may have a function like this:
```python
# jmtests/awesome_things/things1.py

from jobmaster import task

@task
def foo(file_path: str, number: [1, 2, 3]):
    """
    Write a number to a file.

    :param file_path: the file to write to
    :param number: the number to write
    """
    with open(file_path, 'w') as f:
        f.write(str(number))
```
Think of "tasks" as functions and "jobs" as instances of those functions with specific arguments.

Once you have properly configured JobMaster, this task will be registered with 
- `type_key='things1'` (the name of the module) 
- `task_key='foo'` (the name of the function)

You can add a job to the queue by calling the procedure
```postgresql
call jobmaster.insert_job('things1', 'foo', 10, '{"file_path": "/tmp/sum.txt", "number": 2}'::json)
```
from wherever you have a connection to your database: from a different python script, from a web application, etc..
`jobmaster.insert_job` takes 4 arguments:
1. The type key of the task
2. The task key of the task
3. The priority of the job
4. The arguments to pass to the task, json formatted

Somewhere in your python project, you will have a script that pops jobs from the queue and executes them:

```python
# jmtests/__init__.py
import sqlalchemy

db_engine = sqlalchemy.create_engine("postgresql+pg8000://", ...)
```

```python
# jmtests/utils/jmutils.py

from jobmaster import JobMaster
from .. import db_engine
from '<any module with tasks>' import *

jobmaster = JobMaster(db_engine=db_engine, _validate_dependencies=True)

# Run jobs from the queue until the queue is empty
if __name__ == '__main__':
    jobs = jobmaster.run()
```
This could be run in a loop, or in a cron job, or in a web server, etc..

Tasks can depend on other tasks, and JobMaster will automatically run them in the correct order.

## Simple example

In one file you might have:
```python
# module nice_tasks.py

from jobmaster import task, Dependency, same

@task
def foo(file_path: str, number: int):
    with open(file_path, 'w') as f:
        f.write(str(number))
    

@task(dependencies=Dependency(foo, 6, file_path=same))
def bar(file_path: str, number: int, letter: ['A', 'B', 'C'] = 'A'):
    with open(file_path, 'r') as f:
        _n = int(f.read())
        
    with open(file_path, 'w') as f:
        f.write(f"{number + _n}{letter}")

@task(
    process_limit=3,
    dependencies=[
        Dependency(foo, 2, number=10, file_path=same), 
        Dependency(bar, 2, file_path=same)
    ]
)
def baz(file_path: str):
    with open(file_path, 'r') as f:
        s = f.read()
```

And in another file you might have:
```python
# module main.py

import sqlalchemy
from jobmaster import JobMaster

# Create a SQLAlchemy engine for your PostgreSQL database
my_database_engine = sqlalchemy.create_engine("postgresql+pg8000://", ...)

# Create a JobMaster instance
jobmaster = JobMaster(db_engine=my_database_engine)
```

## Setup

The first time you run your code, you'll need to create the necessary tables in your database. You can do this by using the `JobMaster.deploy()` method:
```python
# module deploy_jobmaster.py

from .main import jobmaster

# Deploy the necessary tables
jobmaster.deploy()
```

```bash
python3 deploy_jobmaster.py
```

This will create a schema in your database called `jobmaster` and create the necessary tables and functions for JobMaster to work.
If you already have a schema called `jobmaster` in your database, you can specify a different schema name when creating the `JobMaster` instance:
```python
jobmaster = JobMaster(db_engine=my_database_engine, schema="job_mistress")
```

If you change any of the task definitions, you must update the database by running `jobmaster.deploy()` again. Running `jobmaster.deploy(_reset=True)` will drop the schema and all tables then recreate them from scratch, losing any jobs you had in your queue.

## Tasks

We have already introduced the `@task` decorator. 
This can be used without arguments as in
```python
# module my_tasks.py

from jobmaster import task

@task
def foo(a: int, b: int):
    # do something
```
This will register the function `foo` as a task with type key `'my_tasks'` and task key `'foo'`.
JobMaster will register the parameters `a` and `b`, and their types (both `int`) are infered from the function signature, it is therefore important to use python type hints.

#### Optional parameters

If a parameter is optional, you can specify a default value for it:
```python
@task
def foo(a: int = 1, b: int = 2):
    # do something
```

#### "select-from" parameters

If a parameter has a number of possible options, this should be specified in the type hint:
```python
@task
def foo(a: int, b: [1, 2, 3]):
    # do something
```
the argument values `1`, `2`, and `3` are the only valid values for `b`.

#### "write-all" parameters

For such parameters, it may be desirable to insert a job to the queue which performs the task for all possible values of the parameter.
This can be achieved by specifying the relevant parameters in the `write_all` argument of the `@task` decorator:
```python
@task(write_all=['b'])
def foo(a: int, b: [1, 2, 3]):
    # do something
```
then calling
```postgresql
call jobmaster.insert_job('my_tasks', 'foo', 10, '{"a": 1, "b": "ALL"}'::json)
```
(`'ALL'` in upper-case). When this job is executed, instead of actually executing the task `foo`, JobMaster will insert new a job for each possible value of `b` into the queue.

#### Custom type keys

The type key is the name of the module by default, but you can specify a different type key by passing it as an argument to the decorator:
```python
@task(type_key='cool_tasks', write_all=['b'])
def foo(a: int, b: [1, 2, 3]):
    # do something
```

#### Dependencies

Tasks can depend on other tasks. This is specified by passing `Dependency` object (or a list of `Dependency` objects) to the `dependencies` argument of the `@task` decorator:
```python
from jobmaster import task, Dependency, same

@task(type_key='cool_tasks', write_all=['b'])
def foo(a: int, b: [1, 2, 3]):
    # do something

@task(
    type_key='cool_tasks', 
    write_all=['b'],
    dependencies=Dependency(foo, 2, a=1, b=same)
)
def bar(a: int, b: [1, 2, 3], c: str):
    # do something
```
The `Dependency` object takes the task function, a time (in hours), and the arguments to pass to the task.
In this example, the task `bar` depends on the task `foo` with `a=1` and `b` the same as the `b` of the job for `bar`.
When a job with task-type `"bar"` is popped from the queue, JobMaster will first check if there is a job for task-type `"foo"` with `a=1` and `b` the same as the `b` of the job for `bar`, which has been **completed in the past 2 hours**.
If there isn't, it will insert a job for task-type `"foo"` with `a=1` and `b` the same as the `b` of the job for `bar` into the queue, with a higher priority than the job for `bar`, then re-insert the job for `bar` into the queue.

#### Process units

JobMaster can limit jobs using process units. 

When a JobMaster object is initialised, the number of process units available on the system is passed as an argument:
```python
jobmaster = JobMaster(db_engine=my_database_engine, system_process_units=1_000)
```
The default value is 100,000 if this argument is not specified.

You can then specify the number of process units a task requires by passing an integer to the `process_units` argument of the `@task` decorator. If this argument is not specified, the task will require 1 process unit by default.

You can potentially think of process units as Megabytes of RAM. So if you want to restrict JobMaster to using 1GB, set `system_process_units=1_000` and set `process_units` for each task according to how much RAM you expect it to use in MB.

When JobMaster attempts to pop a job from the queue, it checks the queue for all "running" jobs on the same system and sums their process units. This is subtracted from the system process units to get the available process units.
Then, when checking the queue for "waiting" jobs to pop, there is a `WHERE process_units <= available_process_units` clause.

Example:
```python
from jobmaster import task, Dependency, same

@task(process_units=10, write_all=['b'])
def foo(a: int, b: [1, 2, 3]):
    # do something

@task(
    type_key='cool_tasks', 
    write_all=['b'],
    dependencies=Dependency(foo, 2, a=1, b=same),
    process_units=20
)
def bar(a: int, b: [1, 2, 3], c: str):
    # do something
```
