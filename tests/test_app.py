import unittest
import os
from faker import Faker
from sqlalchemy import create_engine, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import VARCHAR

from ninjasql.app import FileInspector
from ninjasql.errors import NoColumnsError, NoTableNameGivenError
from tests.helpers.file_generator import FileGenerator, FILEPATH
from tests.helpers.ini_generator import IniGenerator
from tests import config
from tests import db

DBPATH = os.path.dirname(db.__file__)
CONFIGPATH = os.path.dirname(config.__file__)


def rm_file(path):
    try:
        os.remove(path)
    except OSError:
        pass


class FileInspectorTest(unittest.TestCase):

    def test_klass(self):
        """
        test if main class is available
        """
        self.assertIsNotNone(FileInspector())

    def test_instance(self):
        """
        test if class instance can be created
        """
        c = FileInspector(
            file="Path",
            seperator=",",
            header=True,
            type="csv",
        )
        self.assertIsNotNone(c)

    def test_file_exist(self):
        """
        test if file exist or not
        """
        c = FileInspector(
            file="XXYUI",
            seperator="|",
            type="csv",
        )
        self.assertEqual(c._is_file(), False)

    def test_read_config(self):
        """
        Test if config class can read config ini and
        get results back
        """
        c = FileInspector(
            file="XXYUI",
            seperator="|",
            type="csv",
        )
        inif_name = "ninjasql.ini"
        nf_path = os.path.join(CONFIGPATH, inif_name)
        IniGenerator._save_file(
            content=IniGenerator._ini_file_content(),
            path=nf_path
        )
        c.load_config(cfg_path=nf_path)

        expected_sections = [
            "Staging",
            "PersistentStaging"
        ]

        for _ in expected_sections:
            self.assertIn(_, c.config.config.sections())


class FileInspectorCsvTest(unittest.TestCase):

    testfile = {
            'name': "data1",
            'type': "csv"
        }

    @classmethod
    def setUpClass(cls):
        cls.gen = cls._gen_file()
        cls.gen.create()
        cls._create_ini_file()

    @classmethod
    def tearDownClass(cls):
        cls.gen.rm()
        rm_file(FileInspectorCsvTest._get_inipath())

    @classmethod
    def _gen_file(cls,
                  header: bool = True,
                  sep: str = '|',
                  type: str = None,
                  name: str = None
                  ):
        ftype = type or FileInspectorCsvTest.testfile['type']
        fname = name or FileInspectorCsvTest.testfile['name']
        gen = FileGenerator(type=ftype,
                            name=fname,
                            header=header,
                            seperator=sep)
        faker = Faker()
        for n in range(100):
            gen.add_rows(
                {'Lat': faker.coordinate(center=74.0, radius=0.10),
                 'Lon': faker.coordinate(center=40.8, radius=0.10),
                 'Txt': faker.sentence(),
                 'Nam': faker.name(),
                 'Add': faker.address().replace("\n", '\\n'),
                 'Job': faker.job()})
        return gen

    def _get_engine(self):
        """
        return a database connection
        """
        dbname = "ninjasql_test.db"
        url = os.path.join(DBPATH, dbname)
        engine = create_engine('sqlite:///' + url, echo=True)
        Base = declarative_base()
        Base.metadata.create_all(engine)
        # engine.execute(f"ATTACH DATABASE '{url}' AS STAGING;")
        return engine

    def _rm(self, path):
        try:
            os.remove(path)
        except OSError:
            pass

    @staticmethod
    def _create_ini_file():
        IniGenerator._save_file(
            content=IniGenerator._ini_file_content(),
            path=FileInspectorCsvTest._get_inipath()
        )

    @staticmethod
    def _get_inipath() -> str:
        inif_name = "ninjasql.ini"
        return os.path.join(CONFIGPATH, inif_name)

    def test_show_columns(self):
        """
        test if columns
        """
        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorCsvTest.testfile['name']}."
                 f"{FileInspectorCsvTest.testfile['type']}")),
            seperator="|",
            type="csv"
        )

        exp_col = [
            "Lat",
            "Lon",
            "Txt",
            "Nam",
            "Add",
            "Job",
        ]

        self.assertEqual(sorted(exp_col), sorted(c.show_columns()))

    def test_get_dtypes(self):
        """
        test if data types gets returned as a dict
        """
        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorCsvTest.testfile['name']}."
                 f"{FileInspectorCsvTest.testfile['type']}")),
            seperator="|",
            type="csv"
        )
        dtype_key_list = c.get_dtypes().keys()

        exp_col = [
            "Lat",
            "Lon",
            "Txt",
            "Nam",
            "Add",
            "Job",
        ]

        self.assertEqual(sorted(exp_col), sorted(dtype_key_list))

    def test_get_dtypes_no_header(self):
        """
        test if dtypes without header can be returned
        """
        spec = {
            'name': "nohead",
            'type': "csv"
        }
        g = FileInspectorCsvTest._gen_file(name=spec['name'],
                                           header=False,
                                           type=spec['type'])
        g.create()

        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{spec['name']}."
                 f"{spec['type']}")),
            seperator="|",
            type="csv",
            header=None)

        with self.assertRaises(NoColumnsError):
            c.get_dtypes().keys()
        g.rm()

    def test_get_dtypes_given_header(self):
        """
        test if dtypes with custom header can be returned
        """
        spec = {
            'name': "nohead",
            'type': "csv"
        }
        cols = [
            "Lat",
            "Lon",
            "Txt",
            "Nam",
            "Add",
            "Job",
        ]
        g = FileInspectorCsvTest._gen_file(name=spec['name'],
                                           header=False,
                                           type=spec['type'])
        g.create()

        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{spec['name']}."
                 f"{spec['type']}")),
            seperator="|",
            type="csv",
            columns=cols,
            header=None)

        dtype_key_list = c.get_dtypes().keys()

        g.rm()
        self.assertEqual(sorted(cols), sorted(dtype_key_list))

    def test_change_dt_to_string(self):
        """
        test if all columns can be changed to string datatype
        """
        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorCsvTest.testfile['name']}."
                 f"{FileInspectorCsvTest.testfile['type']}")),
            seperator="|",
            type="csv"
        )
        c.col_to_str()

        dtype_key_list = c.get_dtypes().values()

        check = all(x.name == "object" for x in dtype_key_list)

        self.assertEqual(check, True)

    def test_get_sqlddl(self):
        """
        test if a ddl sql file can be created
        """
        spec = {
            'name': "table1",
            'schema': "STAGING"
        }
        connection = self._get_engine()

        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorCsvTest.testfile['name']}."
                 f"{FileInspectorCsvTest.testfile['type']}")),
            seperator="|",
            type="csv",
            con=connection
        )

        c.get_file_ddl(path=FILEPATH,
                       table_name=spec['name'],
                       schema=spec['schema'])

        nfname = f"{spec['schema']}_{spec['name']}"
        full_path = f"{os.path.join(FILEPATH, nfname)}.sql"

        self.assertEqual(os.path.exists(full_path), True)
        self._rm(full_path)

    def test_get_sqlddl_with_ini(self):
        """
        test if a ddl sql file can be created with the parameters
        of the ini file
        """
        spec = {
            'name': "table1",
            'table_prefix': "STG_",
            'schema': "STAGING"
        }
        connection = self._get_engine()

        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorCsvTest.testfile['name']}."
                 f"{FileInspectorCsvTest.testfile['type']}")),
            seperator="|",
            type="csv",
            con=connection
        )

        c.get_file_ddl(path=FILEPATH,
                       table_name=spec['name']
                       )

        nfname = f"{spec['schema']}_{spec['table_prefix']}_{spec['name']}"
        full_path = f"{os.path.join(FILEPATH, nfname)}.sql"

        self.assertEqual(os.path.exists(full_path), True)
        self._rm(full_path)

    def test_get_sqldll_specific_types(self):
        """
        test if ddl can customized with specific types
        """
        spec = {
            'name': "table2",
            'schema': "STAGING"
        }
        cust_types = {
            "Lat": VARCHAR(length=1000),
            "Lon": VARCHAR(length=1000),
            "Txt": VARCHAR(length=1000),
            "Nam": VARCHAR(length=1000),
            "Add": VARCHAR(length=1000),
            "Job": VARCHAR(length=1000),
        }
        connection = self._get_engine()

        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorCsvTest.testfile['name']}."
                 f"{FileInspectorCsvTest.testfile['type']}")),
            seperator="|",
            type="csv",
            con=connection
        )

        c.get_file_ddl(path=FILEPATH,
                       table_name=spec['name'],
                       schema=spec['schema'],
                       dtype=cust_types)

        nfname = f"{spec['schema']}_{spec['name']}"
        full_path = f"{os.path.join(FILEPATH, nfname)}.sql"

        self.assertEqual(os.path.exists(full_path), True)
        self._rm(full_path)

    def test_sql_qualify_name(self):
        """
        test if name is correctly qualified
        """
        name_com = {
            "A": {"db": "db1",
                  "schema": "schema1",
                  "table": "table1",
                  "exp": "db1.schema1.table1"},
            "B": {"db": None,
                  "schema": "schema1",
                  "table": "table1",
                  "exp": "schema1.table1"},
            "C": {"db": None,
                  "schema": None,
                  "table": "table1",
                  "exp": "table1"},
            "D": {"db": "db1",
                  "schema": None,
                  "table": "table1",
                  "exp": "db1.table1"},
            "E": {"db": "db1",
                  "schema": "schema1",
                  "table": None,
                  "exp": None}
        }

        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorCsvTest.testfile['name']}."
                 f"{FileInspectorCsvTest.testfile['type']}")),
            seperator="|",
            type="csv",
        )
        for test_case in name_com.keys():
            db = name_com[test_case]["db"]
            schema = name_com[test_case]["schema"]
            table = name_com[test_case]["table"]
            exp = name_com[test_case]["exp"]
            if not table:
                with self.assertRaises(NoTableNameGivenError):
                    c._build_name(db=db,
                                  schema=schema,
                                  table=table)
            else:
                self.assertEqual(c._build_name(
                    db=db,
                    schema=schema,
                    table=table,
                ), exp)

    def test_save_file(self):
        """
        test if file can be saved
        """
        connection = self._get_engine()
        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorCsvTest.testfile['name']}."
                 f"{FileInspectorCsvTest.testfile['type']}")),
            seperator="|",
            type="csv",
            con=connection
        )

        cont = "CREATE OR REPLACE TABLE 1"
        fname = "db1.schema1.table1"

        c._save_file(path=FILEPATH,
                     fname=fname,
                     content=cont)

        nfname = "db1_schema1_table1"
        full_path = f"{os.path.join(FILEPATH, nfname)}.sql"

        self.assertEqual(os.path.exists(full_path), True)
        self._rm(full_path)

    def test_create_db_table(self):
        """
        test if pandas dataframe can be translated in a database
        table.
        """
        spec = {
            'name': "table1",
            'schema': "STAGING",
            'type': "staging"
        }
        connection = self._get_engine()

        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorCsvTest.testfile['name']}."
                 f"{FileInspectorCsvTest.testfile['type']}")),
            seperator="|",
            type="csv",
            con=connection
        )

        c.create_db_table(table_name=spec['name'],
                          if_exists="replace",
                          type=spec['type'])

        insp = inspect(connection)
        tables = insp.get_table_names()

        self.assertIn(spec['name'], tables)

    def test_save_history_table(self):
        """
        test if a second table is created that contains all relevant
        attribute for the scd2 historization
        """
        spec = {
            'name': "table1",
            'schema': "HISTORY"
        }
        connection = self._get_engine()

        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorCsvTest.testfile['name']}."
                 f"{FileInspectorCsvTest.testfile['type']}")),
            seperator="|",
            type="csv",
            con=connection
        )

        c.get_history_ddl(path=FILEPATH,
                          table_name=spec['name'],
                          schema=spec['schema'])

        nfname = f"{spec['schema']}_{spec['name']}"
        full_path = f"{os.path.join(FILEPATH, nfname)}.sql"

        self.assertEqual(os.path.exists(full_path), True)
        self._rm(full_path)

    def test_add_scd2_attributes(self):
        """
        test if scd2 attributes can be add to the dataframe
        """
        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorCsvTest.testfile['name']}."
                 f"{FileInspectorCsvTest.testfile['type']}")),
            seperator="|",
            type="csv",
        )

        cols = [
            "ROW",
            "UPDATED_AT",
            "VALID_FROM_DATE",
            "VALID_TO_DATE",
        ]

        c._add_scd2_attributes()

        dtype_key_list = c._his_data.dtypes.to_dict().keys()

        for col in cols:
            self.assertIn(col, dtype_key_list)

    def test_create_db_history_table(self):
        """
        test if pandas dataframe can be translated in a database
        table.
        """
        spec = {
            'name': "table2",
            'schema': "HISTORY",
            'type': "history"
        }
        connection = self._get_engine()

        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorCsvTest.testfile['name']}."
                 f"{FileInspectorCsvTest.testfile['type']}")),
            seperator="|",
            type="csv",
            con=connection
        )

        c._add_scd2_attributes()

        c.create_db_table(table_name=spec['name'],
                          if_exists="replace",
                          type=spec['type'])

        insp = inspect(connection)
        tables = insp.get_table_names()

        self.assertIn(spec['name'], tables)


class FileInspectorJsonTest(unittest.TestCase):

    testfile = {
            'name': "data2",
            'type': "json"
        }

    @classmethod
    def setUpClass(cls):
        cls.gen = FileGenerator(type=FileInspectorJsonTest.testfile['type'],
                                name=FileInspectorJsonTest.testfile['name'],
                                header=True,
                                seperator='|',
                                orient="split")
        faker = Faker()
        for n in range(100):
            cls.gen.add_rows(
                {'Lat': faker.coordinate(center=74.0, radius=0.10),
                 'Lon': faker.coordinate(center=40.8, radius=0.10),
                 'Txt': faker.sentence(),
                 'Nam': faker.name(),
                 'Add': faker.address(),
                 'Job': faker.job(),
                 'CreatedAt': faker.date_time()})
        cls.gen.create()

    @classmethod
    def tearDownClass(cls):
        cls.gen.rm()

    def test_show_columns(self):
        """
        test if columns
        """
        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorJsonTest.testfile['name']}."
                 f"{FileInspectorJsonTest.testfile['type']}")),
            type="json",
            orient="split"
        )

        exp_col = [
            "Lat",
            "Lon",
            "Txt",
            "Nam",
            "Add",
            "Job",
            "CreatedAt"
        ]

        self.assertEqual(sorted(exp_col), sorted(c.show_columns()))

    def test_get_dtypes(self):
        """
        test if columns
        """
        c = FileInspector(
            file=os.path.join(
                FILEPATH,
                (f"{FileInspectorJsonTest.testfile['name']}."
                 f"{FileInspectorJsonTest.testfile['type']}")),
            type="json",
            orient="split"
        )

        dtype_key_list = c.get_dtypes().keys()

        exp_col = [
            "Lat",
            "Lon",
            "Txt",
            "Nam",
            "Add",
            "Job",
            "CreatedAt"
        ]

        self.assertEqual(sorted(exp_col), sorted(dtype_key_list))


if __name__ == "__main__":
    unittest.main()
