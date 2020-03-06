import unittest
import os
from faker import Faker
from ninjasql.app import NinjaSql
from ninjasql.errors import NoColumnsError
from tests.helpers.file_generator import FileGenerator, FILEPATH


class NinjaSqlTest(unittest.TestCase):

    def test_klass(self):
        """
        test if main class is available
        """
        self.assertIsNotNone(NinjaSql())

    def test_instance(self):
        """
        test if class instance can be created
        """
        c = NinjaSql(
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
        c = NinjaSql(
            file="XXYUI",
            seperator="|",
            type="csv",
        )
        self.assertEqual(c._is_file(), False)


class NinjaSqlCsvTest(unittest.TestCase):

    testfile = {
            'name': "data1",
            'type': "csv"
        }

    @classmethod
    def setUpClass(cls):
        cls.gen = cls._gen_file()
        cls.gen.create()

    @classmethod
    def tearDownClass(cls):
        cls.gen.rm()

    @classmethod
    def _gen_file(cls,
                  header: bool = True,
                  sep: str = '|',
                  type: str = None,
                  name: str = None
                  ):
        ftype = type or NinjaSqlCsvTest.testfile['type']
        fname = name or NinjaSqlCsvTest.testfile['name']
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

    def test_show_columns(self):
        """
        test if columns
        """
        c = NinjaSql(
            file=os.path.join(
                FILEPATH,
                (f"{NinjaSqlCsvTest.testfile['name']}."
                 f"{NinjaSqlCsvTest.testfile['type']}")),
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
        c = NinjaSql(
            file=os.path.join(
                FILEPATH,
                (f"{NinjaSqlCsvTest.testfile['name']}."
                 f"{NinjaSqlCsvTest.testfile['type']}")),
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
        g = NinjaSqlCsvTest._gen_file(name=spec['name'],
                                      header=False,
                                      type=spec['type'])
        g.create()

        c = NinjaSql(
            file=os.path.join(
                FILEPATH,
                (f"{spec['name']}."
                 f"{spec['type']}")),
            seperator="|",
            type="csv",
            header=None)

        g.rm()

        with self.assertRaises(NoColumnsError):
            c.get_dtypes().keys()

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
        g = NinjaSqlCsvTest._gen_file(name=spec['name'],
                                      header=False,
                                      type=spec['type'])
        g.create()

        c = NinjaSql(
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


class NinjaSqlJsonTest(unittest.TestCase):

    testfile = {
            'name': "data2",
            'type': "json"
        }

    @classmethod
    def setUpClass(cls):
        cls.gen = FileGenerator(type=NinjaSqlJsonTest.testfile['type'],
                                name=NinjaSqlJsonTest.testfile['name'],
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
        c = NinjaSql(
            file=os.path.join(
                FILEPATH,
                (f"{NinjaSqlJsonTest.testfile['name']}."
                 f"{NinjaSqlJsonTest.testfile['type']}")),
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
        c = NinjaSql(
            file=os.path.join(
                FILEPATH,
                (f"{NinjaSqlJsonTest.testfile['name']}."
                 f"{NinjaSqlJsonTest.testfile['type']}")),
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
