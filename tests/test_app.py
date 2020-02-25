import unittest
from ninjasql.app import NinjaSql


class NinjaSqlTest(unittest.TestCase):

    def test_klass(self):
        """
        test if main class is available
        """
        self.assertIsNotNone(NinjaSql())

if __name__ == "__main__":
    unittest.main()