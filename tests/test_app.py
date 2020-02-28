import unittest
import os
from faker import Faker
from ninjasql.app import NinjaSql
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

    def test_show_columns(self):
        """
        test if columns
        """
        testfile = {
            'name': "data1",
            'type': "csv"
        }
        gen = FileGenerator(type=testfile['type'],
                            name=testfile['name'],
                            header=True,
                            seperator='|')
        faker = Faker()
        for n in range(100):
            gen.add_rows(
                {'Lat': faker.coordinate(center=74.0, radius=0.10),
                 'Lon': faker.coordinate(center=40.8, radius=0.10),
                 'Txt': faker.sentence(),
                 'Nam': faker.name(),
                 'Add': faker.address(),
                 'Job': faker.job()})
        gen.create()

        c = NinjaSql(
            file=os.path.join(
                FILEPATH, f"{testfile['name']}.{testfile['type']}"),
            seperator="|",
            type="csv",
        )

        print(c.show_columns())
        exp_col = [
            "Lat",
            "Lon",
            "Txt",
            "Nam",
            "Add",
            "Job",
        ]

        # self.assertIsNotNone(c.show_columns())
        self.assertEqual(sorted(exp_col), sorted(c.show_columns()))


if __name__ == "__main__":
    unittest.main()
