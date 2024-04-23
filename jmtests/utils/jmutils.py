from jobmaster import JobMaster

from ..awesome_things import *
from .. import db_engine


jobmaster = JobMaster(db_engine, logger=print, _validate_dependencies=True)


def main():
    jobmaster.deploy()


def print_tasks():
    for _type_key, _task_key, _task in jobmaster.all_tasks():
        print(_task)
