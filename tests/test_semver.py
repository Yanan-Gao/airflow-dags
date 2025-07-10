from src.ttd.semver import SemverVersion
import unittest


class SemverTest(unittest.TestCase):

    def test_greater_than(self):
        v1 = SemverVersion(1, 0, 0)
        v2 = SemverVersion(0, 9, 0)
        v3 = SemverVersion(1, 0, 0)

        self.assertTrue(v1 > v2)
        self.assertFalse(v1 > v3)
        self.assertFalse(v2 > v1)

    def test_greater_than_equal(self):
        v1 = SemverVersion(1, 0, 0)
        v2 = SemverVersion(0, 9, 0)
        v3 = SemverVersion(1, 0, 0)

        self.assertTrue(v1 >= v2)
        self.assertTrue(v1 >= v3)
        self.assertFalse(v2 >= v1)

    def test_less_than(self):
        v1 = SemverVersion(1, 0, 0)
        v2 = SemverVersion(0, 9, 0)
        v3 = SemverVersion(1, 0, 0)

        self.assertTrue(v2 < v1)
        self.assertFalse(v1 > v3)
        self.assertFalse(v1 < v2)

    def test_less_than_equal(self):
        v1 = SemverVersion(1, 0, 0)
        v2 = SemverVersion(0, 9, 0)
        v3 = SemverVersion(1, 0, 0)

        self.assertTrue(v2 <= v1)
        self.assertTrue(v1 <= v3)
        self.assertFalse(v1 <= v2)
