import unittest

from ninjasql.dep.table_dependency import TableDep


class TableDependencyTest(unittest.TestCase):

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
        self.assertEqual(g.show_edges(), [(first, second)])

    def test_find_path(self):
        first = "table1"
        second = "table2"
        third = "table3"
        fourth = "table4"
        exp_path = [first, third, second, fourth]
        g = TableDep()
        g.addTable(second, first)
        g.addTable(third, first)
        g.addTable(fourth, second)
        self.assertEqual(g.find_path(), exp_path)

    def test_find_batch_path(self):
        exp_path = [['table1'], ['table2', 'table3', 'table5'],
                    ['table6', 'table4']]
        g = TableDep()
        g.addTable(name="table2", depends_on="table1")
        g.addTable(name="table3", depends_on="table1")
        g.addTable(name="table5", depends_on="table1")
        g.addTable(name="table4", depends_on="table1")
        g.addTable(name="table4", depends_on="table3")
        g.addTable(name="table6", depends_on="table2")
        self.assertEqual(g.find_batch_path(), exp_path)
