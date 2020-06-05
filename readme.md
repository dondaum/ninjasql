![ninjasql package](https://github.com/dondaum/ninjasql/workflows/ninjasql%20package/badge.svg?branch=master)

# Ninjasql introduction
This small python package shows how far you can automate the extraction and generation
of a SQL DDL with the Python packages pandas and sqlalchemy and some custom Python
code in between. This package is not finished and should only be used as playing or
testing project. 

That has some reason. The most importent is, that it was not clear for me in the start
where to go with this package. The focus on this project was more the overall 
development process and not finishing a specfic and defined set of functionality. 

Unfortunately that is reflected in the code base. It is not focused on pure SQL DDL
extraction but furhter on creating a complete SQL DDL template for a staging, a
historization and SQL DML statements for implemention and scd2 database historization.

## How to use?
### Create a ini file
Create a ini file somewhere on your machine. You have to define the options
- Staging
- PersistentStaging
with the needed names for the schema and the table prefix.

```
[Staging]
schema_name=STAGING
table_prefix_name=STG

[PersistentStaging]
schema_name=PERS_STAGING
table_prefix_name=PER_STG
```
Copy the path of the ini file. You need to pass the path as an argument in the main class 
``` FileInspector ```. 

### Create a sqlalchemy engine with the database of your choice
pandas need a sqlalchemy engine object in order to generate the DDL sql for you. You can
use any database that is supported by sqlalchemy such as:
- Postgres
- MySQL
- MSSQL
- Sqlite
- Redshift
- Snowflake

Create the engine and pass the path as an argument in the main class ``` FileInspector ```. 

```python

    from sqlalchemy import create_engine
    dbname = "ninjasql_test.db"
    DBPATH = "/Users/JohnDoe/Developement/python_pipeline/ninjasql/tests/db"
    url = os.path.join(DBPATH, dbname)
    engine = create_engine('sqlite:///' + url, echo=True)

```

### FileInspector
In order to create a instance of the main class you have to specify
- ini path
- file path 
- file type
- file seperator for csv files
- sqlalchemy engine

```python

    c = FileInspector(
        cfg_path="/Users/JohnDoe/Developement/python_pipeline/ninjasql/test.ini",
        file="/Users/JohnDoe/Desktop/people.csv",
        type="csv",
        seperator=',',
        con=engine)

```
### Create staging DDL
Create SQL DDL as utf8 files in the specified folder.

```python

    c.save_staging_ddl(
        path="/Users/sebastiandaum/Desktop/",
        table_name="people"
    )

```

### Create history DDL
Create SQL DDL as utf8 files in the specified folder.

```python

    c.save_history_ddl(
        path="/Users/sebastiandaum/Desktop/",
        table_name="people"
    )

```

### Create a complete staging, history and scd2 blueprint 
Create SQL DDL as utf8 files in the specified folder and the 
needed scd2 DMLs for doing a database scd2 historization.

Currently to strategies are supported:
- database_table
- jinja

For **database_table** an additional database table gets created that
has all needed metadata column which should be used to organize and
run the batch load for the scd2 historization. All created DMLs for
the DDL will query this main table to get the needed date parameter.

```python

class TableLoad(Base):
    __tablename__ = 'tableloads'

    name = Column(String, primary_key=True)
    BatchDate = Column(DateTime)
    ValidToDate = Column(DateTime)
    OffsetValidToDate = Column(DateTime)
    ValidFromDate = Column(DateTime)

```

For **jinja** all created DMLs have a jinja parameter field for the needed
date parameters. Use this strategy if you want to organize your load more
with an workflow tool such as Luigi or Apache Airflow. Then just insert
the needed parameter with Jinja while running the DML statements.

In order to create a complete ELT blueprint just run this class method:

```python

    c.create_file_elt_blueprint(
         path=FILEPATH,
         table_name="PEOPLE",
         logical_pk=['Nam'],
         load_strategy='database_table'
     )

```