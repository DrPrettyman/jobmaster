# JobMaster

JobMaster is a simple job scheduling library for Python. 
It works with any PostgreSQL database, and is designed to be simple to use and easy to integrate into your existing codebase.

Any function in your project can become a JobMaster Task by using the `@task` decorator.
For example, you may have a function like this:
```python
# module my_tasks.py

from jobmaster import task

@task
def task1(file_path: str, a: int, b: int):
    with open(file_path, 'w') as f:
        f.write(str(a + b))
```
Think of "tasks" as functions and "jobs" as instances of those functions with specific arguments.

Once you have properly configured JobMaster, this task will be registered with "type key" `my_tasks` (the name of the module) and "task key" `task1` (the name of the function). 

You can add a job to the queue by calling the procedure
```postgresql
call jobmaster.insert_job('my_tasks', 'task1', 10, '{"file_path": "/tmp/sum.txt", "a": 1, "b": 2}'::json)
```
from wherever you have a connection to your database: from a different python script, from a web application, etc..
`jobmaster.insert_job` takes 4 arguments:
1. The type key of the task
2. The task key of the task
3. The priority of the job
4. The arguments to pass to the task, json formatted

Somewhere in your python project, you will have a script that pops jobs from the queue and executes them:

```python
# module run.py

import sqlalchemy
from jobmaster import JobMaster

# Create a SQLAlchemy engine for your PostgreSQL database
my_database_engine = sqlalchemy.create_engine("postgresql+pg8000://", ...)

# Create a JobMaster instance
jobmaster = JobMaster(db_engine=my_database_engine)

# Run the next job in the queue
job = jobmaster.pop_job()
if job:
    job.execute()
```
This could be run in a loop, or in a cron job, or in a web server, etc..

Tasks can depend on other tasks, and JobMaster will automatically run them in the correct order.

### Simple example

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

### Setup

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

If you change any of the task definitions, you can update the database schema by running `jobmaster.deploy()` again. Running `jobmaster.deploy(_reset=True)` will drop all tables and recreate them from scratch, losing any jobs you had in your queue.