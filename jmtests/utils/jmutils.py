from ..awesome_things import *
from .. import jobmaster


def main():
    jobmaster.deploy()


def print_tasks():
    for _type_key, _task_key, _task in jobmaster.all_tasks():
        print(_type_key, _task_key)
