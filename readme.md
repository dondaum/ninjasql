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
    path="/Users/JohnDoe/Desktop/",
    table_name="people"
)

```

### Create history DDL
Create SQL DDL as utf8 files in the specified folder.

```python

c.save_history_ddl(
    path="/Users/JohnDoe/Desktop/",
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
     load_strategy='database_table')

```

### Extracted SQL


All datasources will create a new folder. Each folder has subfolders for DML and DDLs e.g.:
- TABLELOAD
    - DDL
        - ...sql
    - DML
        - ...sql
- PEOPLE
    - DDL
        - ...sql
    - DML
        - ...sql

#### database_table strategy

Main tableload table DDL for handling the batch processing.

```
-- JOBTABLE_TABLELOAD.sql

CREATE TABLE tableloads (
	name VARCHAR NOT NULL, 
	"BatchDate" DATETIME, 
	"ValidToDate" DATETIME, 
	"OffsetValidToDate" DATETIME, 
	"ValidFromDate" DATETIME, 
	PRIMARY KEY (name)
)
```

DML to create a insert for the history table.

```
-- PERS_STAGING_PER_STG_PEOPLE_INSERT_TABLELOAD.sql
INSERT INTO tableloads (name, "BatchDate", "ValidToDate", "OffsetValidToDate", "ValidFromDate") VALUES ('PERS_STAGING.PER_STG_PEOPLE', '2020-02-01 00:00:00.000000', '9999-12-31 00:00:00.000000', '2020-01-30 00:00:00.000000', '2020-01-31 00:00:00.000000')
```


Staging DDL

```
CREATE TABLE "STAGING.STG_PEOPLE" (
	id BIGINT, 
	forenames TEXT, 
	surname TEXT, 
	title TEXT, 
	address1 TEXT, 
	address2 TEXT, 
	town TEXT, 
	county TEXT, 
	country TEXT, 
	postcode TEXT, 
	subscribed BIGINT, 
	gender TEXT, 
	dob TEXT
)
```

History DDL

```
CREATE TABLE "PERS_STAGING.PER_STG_PEOPLE" (
	id BIGINT, 
	forenames TEXT, 
	surname TEXT, 
	title TEXT, 
	address1 TEXT, 
	address2 TEXT, 
	town TEXT, 
	county TEXT, 
	country TEXT, 
	postcode TEXT, 
	subscribed BIGINT, 
	gender TEXT, 
	dob TEXT, 
	"UPDATED_AT" DATETIME, 
	"BATCH_RUN_AT" DATETIME, 
	"VALID_FROM_DATE" DATETIME, 
	"VALID_TO_DATE" DATETIME
)


```

**SCD2 DMLs with table_load strategy**

```
INSERT INTO "PERS_STAGING.PER_STG_PEOPLE" (id, forenames, surname, title, address1, address2, town, county, country, postcode, subscribed, gender, dob, "UPDATED_AT", "BATCH_RUN_AT", "VALID_FROM_DATE", "VALID_TO_DATE") SELECT "STAGING.STG_PEOPLE".id, "STAGING.STG_PEOPLE".forenames, "STAGING.STG_PEOPLE".surname, "STAGING.STG_PEOPLE".title, "STAGING.STG_PEOPLE".address1, "STAGING.STG_PEOPLE".address2, "STAGING.STG_PEOPLE".town, "STAGING.STG_PEOPLE".county, "STAGING.STG_PEOPLE".country, "STAGING.STG_PEOPLE".postcode, "STAGING.STG_PEOPLE".subscribed, "STAGING.STG_PEOPLE".gender, "STAGING.STG_PEOPLE".dob, (SELECT CURRENT_TIMESTAMP AS "UPDATED_AT") AS anon_1, (SELECT tableloads."BatchDate" 
FROM tableloads 
WHERE tableloads.name = 'PERS_STAGING.PER_STG_PEOPLE') AS anon_2, (SELECT tableloads."ValidFromDate" 
FROM tableloads 
WHERE tableloads.name = 'PERS_STAGING.PER_STG_PEOPLE') AS anon_3, (SELECT tableloads."ValidToDate" 
FROM tableloads 
WHERE tableloads.name = 'PERS_STAGING.PER_STG_PEOPLE') AS anon_4 
FROM "STAGING.STG_PEOPLE" 
WHERE NOT (EXISTS (SELECT "STAGING.STG_PEOPLE".id 
FROM "PERS_STAGING.PER_STG_PEOPLE" 
WHERE "STAGING.STG_PEOPLE".id = "PERS_STAGING.PER_STG_PEOPLE".id));

--

INSERT INTO "PERS_STAGING.PER_STG_PEOPLE" (id, forenames, surname, title, address1, address2, town, county, country, postcode, subscribed, gender, dob, "UPDATED_AT", "BATCH_RUN_AT", "VALID_FROM_DATE", "VALID_TO_DATE") SELECT "STAGING.STG_PEOPLE".id, "STAGING.STG_PEOPLE".forenames, "STAGING.STG_PEOPLE".surname, "STAGING.STG_PEOPLE".title, "STAGING.STG_PEOPLE".address1, "STAGING.STG_PEOPLE".address2, "STAGING.STG_PEOPLE".town, "STAGING.STG_PEOPLE".county, "STAGING.STG_PEOPLE".country, "STAGING.STG_PEOPLE".postcode, "STAGING.STG_PEOPLE".subscribed, "STAGING.STG_PEOPLE".gender, "STAGING.STG_PEOPLE".dob, (SELECT CURRENT_TIMESTAMP AS "UPDATED_AT") AS anon_1, (SELECT tableloads."BatchDate" 
FROM tableloads 
WHERE tableloads.name = 'PERS_STAGING.PER_STG_PEOPLE') AS anon_2, (SELECT tableloads."ValidFromDate" 
FROM tableloads 
WHERE tableloads.name = 'PERS_STAGING.PER_STG_PEOPLE') AS anon_3, (SELECT tableloads."ValidToDate" 
FROM tableloads 
WHERE tableloads.name = 'PERS_STAGING.PER_STG_PEOPLE') AS anon_4 
FROM "PERS_STAGING.PER_STG_PEOPLE" JOIN "STAGING.STG_PEOPLE" ON "STAGING.STG_PEOPLE".id = "PERS_STAGING.PER_STG_PEOPLE".id 
WHERE ("STAGING.STG_PEOPLE".id != "PERS_STAGING.PER_STG_PEOPLE".id OR "STAGING.STG_PEOPLE".forenames != "PERS_STAGING.PER_STG_PEOPLE".forenames OR "STAGING.STG_PEOPLE".surname != "PERS_STAGING.PER_STG_PEOPLE".surname OR "STAGING.STG_PEOPLE".title != "PERS_STAGING.PER_STG_PEOPLE".title OR "STAGING.STG_PEOPLE".address1 != "PERS_STAGING.PER_STG_PEOPLE".address1 OR "STAGING.STG_PEOPLE".address2 != "PERS_STAGING.PER_STG_PEOPLE".address2 OR "STAGING.STG_PEOPLE".town != "PERS_STAGING.PER_STG_PEOPLE".town OR "STAGING.STG_PEOPLE".county != "PERS_STAGING.PER_STG_PEOPLE".county OR "STAGING.STG_PEOPLE".country != "PERS_STAGING.PER_STG_PEOPLE".country OR "STAGING.STG_PEOPLE".postcode != "PERS_STAGING.PER_STG_PEOPLE".postcode OR "STAGING.STG_PEOPLE".subscribed != "PERS_STAGING.PER_STG_PEOPLE".subscribed OR "STAGING.STG_PEOPLE".gender != "PERS_STAGING.PER_STG_PEOPLE".gender OR "STAGING.STG_PEOPLE".dob != "PERS_STAGING.PER_STG_PEOPLE".dob) AND "PERS_STAGING.PER_STG_PEOPLE"."VALID_TO_DATE" = (SELECT tableloads."ValidToDate" 
FROM tableloads 
WHERE tableloads.name = 'PERS_STAGING.PER_STG_PEOPLE');

--

UPDATE "PERS_STAGING.PER_STG_PEOPLE" SET "UPDATED_AT"=CURRENT_TIMESTAMP, "VALID_TO_DATE"=(SELECT tableloads."OffsetValidToDate" 
FROM tableloads 
WHERE tableloads.name = 'PERS_STAGING.PER_STG_PEOPLE') WHERE EXISTS (SELECT "STAGING.STG_PEOPLE".id 
FROM "STAGING.STG_PEOPLE" 
WHERE "STAGING.STG_PEOPLE".id = "PERS_STAGING.PER_STG_PEOPLE".id AND ("STAGING.STG_PEOPLE".id != "PERS_STAGING.PER_STG_PEOPLE".id OR "STAGING.STG_PEOPLE".forenames != "PERS_STAGING.PER_STG_PEOPLE".forenames OR "STAGING.STG_PEOPLE".surname != "PERS_STAGING.PER_STG_PEOPLE".surname OR "STAGING.STG_PEOPLE".title != "PERS_STAGING.PER_STG_PEOPLE".title OR "STAGING.STG_PEOPLE".address1 != "PERS_STAGING.PER_STG_PEOPLE".address1 OR "STAGING.STG_PEOPLE".address2 != "PERS_STAGING.PER_STG_PEOPLE".address2 OR "STAGING.STG_PEOPLE".town != "PERS_STAGING.PER_STG_PEOPLE".town OR "STAGING.STG_PEOPLE".county != "PERS_STAGING.PER_STG_PEOPLE".county OR "STAGING.STG_PEOPLE".country != "PERS_STAGING.PER_STG_PEOPLE".country OR "STAGING.STG_PEOPLE".postcode != "PERS_STAGING.PER_STG_PEOPLE".postcode OR "STAGING.STG_PEOPLE".subscribed != "PERS_STAGING.PER_STG_PEOPLE".subscribed OR "STAGING.STG_PEOPLE".gender != "PERS_STAGING.PER_STG_PEOPLE".gender OR "STAGING.STG_PEOPLE".dob != "PERS_STAGING.PER_STG_PEOPLE".dob) AND "PERS_STAGING.PER_STG_PEOPLE"."VALID_TO_DATE" = (SELECT tableloads."ValidToDate" 
FROM tableloads 
WHERE tableloads.name = 'PERS_STAGING.PER_STG_PEOPLE') AND "PERS_STAGING.PER_STG_PEOPLE"."BATCH_RUN_AT" < (SELECT tableloads."BatchDate" 
FROM tableloads 
WHERE tableloads.name = 'PERS_STAGING.PER_STG_PEOPLE'));

--

UPDATE "PERS_STAGING.PER_STG_PEOPLE" SET "UPDATED_AT"=CURRENT_TIMESTAMP, "VALID_TO_DATE"=(SELECT tableloads."OffsetValidToDate" 
FROM tableloads 
WHERE tableloads.name = 'PERS_STAGING.PER_STG_PEOPLE') WHERE NOT (EXISTS (SELECT "STAGING.STG_PEOPLE".id 
FROM "STAGING.STG_PEOPLE" 
WHERE "STAGING.STG_PEOPLE".id = "PERS_STAGING.PER_STG_PEOPLE".id)) AND "PERS_STAGING.PER_STG_PEOPLE"."VALID_TO_DATE" = (SELECT tableloads."ValidToDate" 
FROM tableloads 
WHERE tableloads.name = 'PERS_STAGING.PER_STG_PEOPLE')


```



**SCD2 DMLs with jinja strategy**

```
INSERT INTO "PERS_STAGING.PER_STG_PEOPLE" (id, forenames, surname, title, address1, address2, town, county, country, postcode, subscribed, gender, dob, "UPDATED_AT", "BATCH_RUN_AT", "VALID_FROM_DATE", "VALID_TO_DATE") SELECT "STAGING.STG_PEOPLE".id, "STAGING.STG_PEOPLE".forenames, "STAGING.STG_PEOPLE".surname, "STAGING.STG_PEOPLE".title, "STAGING.STG_PEOPLE".address1, "STAGING.STG_PEOPLE".address2, "STAGING.STG_PEOPLE".town, "STAGING.STG_PEOPLE".county, "STAGING.STG_PEOPLE".country, "STAGING.STG_PEOPLE".postcode, "STAGING.STG_PEOPLE".subscribed, "STAGING.STG_PEOPLE".gender, "STAGING.STG_PEOPLE".dob, (SELECT CURRENT_TIMESTAMP AS "UPDATED_AT") AS anon_1, (SELECT date({{ batch_date }}) AS "BATCH_RUN_AT") AS anon_2, (SELECT date({{ validfrom_date }}) AS "VALID_FROM_DATE") AS anon_3, (SELECT date({{ validto_date }}) AS "VALID_TO_DATE") AS anon_4 
FROM "STAGING.STG_PEOPLE" 
WHERE NOT (EXISTS (SELECT "STAGING.STG_PEOPLE".id 
FROM "PERS_STAGING.PER_STG_PEOPLE" 
WHERE "STAGING.STG_PEOPLE".id = "PERS_STAGING.PER_STG_PEOPLE".id))

--

INSERT INTO "PERS_STAGING.PER_STG_PEOPLE" (id, forenames, surname, title, address1, address2, town, county, country, postcode, subscribed, gender, dob, "UPDATED_AT", "BATCH_RUN_AT", "VALID_FROM_DATE", "VALID_TO_DATE") SELECT "STAGING.STG_PEOPLE".id, "STAGING.STG_PEOPLE".forenames, "STAGING.STG_PEOPLE".surname, "STAGING.STG_PEOPLE".title, "STAGING.STG_PEOPLE".address1, "STAGING.STG_PEOPLE".address2, "STAGING.STG_PEOPLE".town, "STAGING.STG_PEOPLE".county, "STAGING.STG_PEOPLE".country, "STAGING.STG_PEOPLE".postcode, "STAGING.STG_PEOPLE".subscribed, "STAGING.STG_PEOPLE".gender, "STAGING.STG_PEOPLE".dob, (SELECT CURRENT_TIMESTAMP AS "UPDATED_AT") AS anon_1, (SELECT date({{ batch_date }}) AS "BATCH_RUN_AT") AS anon_2, (SELECT date({{ validfrom_date }}) AS "VALID_FROM_DATE") AS anon_3, (SELECT date({{ validto_date }}) AS "VALID_TO_DATE") AS anon_4 
FROM "PERS_STAGING.PER_STG_PEOPLE" JOIN "STAGING.STG_PEOPLE" ON "STAGING.STG_PEOPLE".id = "PERS_STAGING.PER_STG_PEOPLE".id 
WHERE ("STAGING.STG_PEOPLE".id != "PERS_STAGING.PER_STG_PEOPLE".id OR "STAGING.STG_PEOPLE".forenames != "PERS_STAGING.PER_STG_PEOPLE".forenames OR "STAGING.STG_PEOPLE".surname != "PERS_STAGING.PER_STG_PEOPLE".surname OR "STAGING.STG_PEOPLE".title != "PERS_STAGING.PER_STG_PEOPLE".title OR "STAGING.STG_PEOPLE".address1 != "PERS_STAGING.PER_STG_PEOPLE".address1 OR "STAGING.STG_PEOPLE".address2 != "PERS_STAGING.PER_STG_PEOPLE".address2 OR "STAGING.STG_PEOPLE".town != "PERS_STAGING.PER_STG_PEOPLE".town OR "STAGING.STG_PEOPLE".county != "PERS_STAGING.PER_STG_PEOPLE".county OR "STAGING.STG_PEOPLE".country != "PERS_STAGING.PER_STG_PEOPLE".country OR "STAGING.STG_PEOPLE".postcode != "PERS_STAGING.PER_STG_PEOPLE".postcode OR "STAGING.STG_PEOPLE".subscribed != "PERS_STAGING.PER_STG_PEOPLE".subscribed OR "STAGING.STG_PEOPLE".gender != "PERS_STAGING.PER_STG_PEOPLE".gender OR "STAGING.STG_PEOPLE".dob != "PERS_STAGING.PER_STG_PEOPLE".dob) AND "PERS_STAGING.PER_STG_PEOPLE"."VALID_TO_DATE" = date({{ validto_date }})

--

UPDATE "PERS_STAGING.PER_STG_PEOPLE" SET "UPDATED_AT"=CURRENT_TIMESTAMP, "VALID_TO_DATE"=date({{ offset_validto_date }}) WHERE EXISTS (SELECT "STAGING.STG_PEOPLE".id 
FROM "STAGING.STG_PEOPLE" 
WHERE "STAGING.STG_PEOPLE".id = "PERS_STAGING.PER_STG_PEOPLE".id AND ("STAGING.STG_PEOPLE".id != "PERS_STAGING.PER_STG_PEOPLE".id OR "STAGING.STG_PEOPLE".forenames != "PERS_STAGING.PER_STG_PEOPLE".forenames OR "STAGING.STG_PEOPLE".surname != "PERS_STAGING.PER_STG_PEOPLE".surname OR "STAGING.STG_PEOPLE".title != "PERS_STAGING.PER_STG_PEOPLE".title OR "STAGING.STG_PEOPLE".address1 != "PERS_STAGING.PER_STG_PEOPLE".address1 OR "STAGING.STG_PEOPLE".address2 != "PERS_STAGING.PER_STG_PEOPLE".address2 OR "STAGING.STG_PEOPLE".town != "PERS_STAGING.PER_STG_PEOPLE".town OR "STAGING.STG_PEOPLE".county != "PERS_STAGING.PER_STG_PEOPLE".county OR "STAGING.STG_PEOPLE".country != "PERS_STAGING.PER_STG_PEOPLE".country OR "STAGING.STG_PEOPLE".postcode != "PERS_STAGING.PER_STG_PEOPLE".postcode OR "STAGING.STG_PEOPLE".subscribed != "PERS_STAGING.PER_STG_PEOPLE".subscribed OR "STAGING.STG_PEOPLE".gender != "PERS_STAGING.PER_STG_PEOPLE".gender OR "STAGING.STG_PEOPLE".dob != "PERS_STAGING.PER_STG_PEOPLE".dob) AND "PERS_STAGING.PER_STG_PEOPLE"."VALID_TO_DATE" = date({{ validto_date }}) AND "PERS_STAGING.PER_STG_PEOPLE"."BATCH_RUN_AT" < date({{ batch_date }}))

--

UPDATE "PERS_STAGING.PER_STG_PEOPLE" SET "UPDATED_AT"=CURRENT_TIMESTAMP, "VALID_TO_DATE"=date({{ offset_validto_date }}) WHERE NOT (EXISTS (SELECT "STAGING.STG_PEOPLE".id 
FROM "STAGING.STG_PEOPLE" 
WHERE "STAGING.STG_PEOPLE".id = "PERS_STAGING.PER_STG_PEOPLE".id)) AND "PERS_STAGING.PER_STG_PEOPLE"."VALID_TO_DATE" = date({{ validto_date }})


```
