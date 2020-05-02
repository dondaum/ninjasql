import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from tests import db

DBPATH = os.path.dirname(db.__file__)


def get_engine():
    """
    return a database connection
    """
    dbname = "ninjasql_test.db"
    url = os.path.join(DBPATH, dbname)
    engine = create_engine('sqlite:///' + url, echo=True)
    Base = declarative_base()
    Base.metadata.create_all(engine)
    return engine
