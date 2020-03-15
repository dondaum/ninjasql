import unittest

from ninjasql.dep.table_dependency import TableDep


class ConfigSettingTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        pass

    @classmethod
    def tearDownClass(cls):
        pass

    def test_check_klass(self):
        """
        check if class exist
        """
        self.assertIsNotNone(TableDep())

    def test_add_table(self):
        first = "table1"
        second = "table2"
        g = TableDep()
        g.addTable(first, second)
        self.assertEqual(g.show_edges(), (first, second))

    def test_find_path(self):
        first = "table1"
        second = "table2"
        third = "table3"
        fourth = "table4"
        g = TableDep()
        g.addTable(first, second)
        g.addTable(first, third)
        g.addTable(first, fourth)
        # g.addTable(fourth, second)
        self.assertEqual(g.find_path(first, fourth), "a")
