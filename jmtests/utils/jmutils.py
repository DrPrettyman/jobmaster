from jobmaster import JobMaster
from .. import db_engine

# We want to import every single function with the @task decorator to make sure they are registered.
# This could be at any point in the code higher up than when we invoke any methods of a JobMaster instance,
# it's safe and convenient just to do it here.
from ..awesome_things import *

jobmaster = JobMaster(db_engine, logger=print, _validate_dependencies=True)
