import unittest


def _load_tests():
    """Test suite for tests"""
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('.')
    return test_suite


if __name__ == "__main__":
    result = unittest.TextTestRunner(verbosity=2).run(_load_tests())

    if result.wasSuccessful():
        exit(0)
    else:
        exit(1)
