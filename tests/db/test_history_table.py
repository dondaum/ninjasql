import unittest

from ninjasql.db import history_table


class HistoryTableTest(unittest.TestCase):

    def test_if_klass_exist(self):
        """
        Test if main class exist
        """
        self.assertIsNotNone(history_table.HistoryTable)
