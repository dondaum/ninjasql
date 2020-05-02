import unittest

from ninjasql.db.sqa_table_loads import get_sqa_tableload
from tests.db.db_helper import get_engine


class SqaTableLoads(unittest.TestCase):
    def test_ddl_extraction(self):
        """
        Test if batch helper table ddl can be extracted
        """
        ddl = get_sqa_tableload(con=get_engine())
        self.assertTrue("CREATE TABLE tableloads" in ddl)
