from sqlalchemy import Table
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Executable, ClauseElement
import os


class CreateView(Executable, ClauseElement):
    def __init__(self, name, select):
        self.name = name
        self.select = select

@compiles(CreateView)
def visit_create_view(element, compiler, **kw):
    return "CREATE VIEW %s AS %s" % (
         element.name,
         compiler.process(element.select, literal_binds=True)
         )

# test data
from sqlalchemy import MetaData, Column, Integer
from sqlalchemy.engine import create_engine
from sqlalchemy.schema import CreateTable
dbname = "ninjasql_test.db"
DBPATH = "/Users/sebastiandaum/Developement/python_pipeline/ninjasql/tests/db"
url = os.path.join(DBPATH, dbname)
engine = create_engine('sqlite:///' + url, echo=True)
metadata = MetaData(engine)
t = Table('t',
          metadata,
          Column('id', Integer),
          Column('number', Integer))


# create view
createview = CreateView('ASd', t.select().where(t.c.id>5))
print(createview)

