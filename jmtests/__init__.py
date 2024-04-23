import os
import sys
import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes

# Cloud SQL instance configuration
# Initializes a connection pool for a Cloud SQL instance of Postgres.
# Uses the Cloud SQL Python Connector package.

connector = Connector(ip_type=IPTypes.PUBLIC)


def get_conn():
    _conn = connector.connect(
        instance_connection_string=os.environ['jm_db_instance'],
        driver="pg8000",
        user=os.environ['jm_db_user'],
        password=os.environ['jm_db_password'],
        db=os.environ['jm_db_name'],
        ip_type=IPTypes.PUBLIC
    )
    return _conn


# The Cloud SQL Python Connector can be used with SQLAlchemy using the 'creator' argument to 'create_engine'
db_engine = sqlalchemy.create_engine("postgresql+pg8000://", creator=get_conn)
