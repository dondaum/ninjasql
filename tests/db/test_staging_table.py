import unittest

from ninjasql.db import staging_table


class StagingTableTest(unittest.TestCase):

    def test_if_klass_exist(self):
        """
        Test if main class exist
        """
        self.assertIsNotNone(staging_table.StagingTable)
