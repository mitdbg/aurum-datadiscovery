import unittest
from api.apiutils import DRS
from ddapi import API


class TestDDApiPrimitives(unittest.TestCase):

    api = API(None)

    def test_keyword_search(self):
        return


class TestDDApiCombiner(unittest.TestCase):

    api = API(None)

    def test_intersection(self):
        a = DRS([1, 2, 3])
        b = DRS([3, 4, 5])
        res = self.api.intersection(a, b)
        self.assertTrue(3 in res)
        self.assertTrue(2 not in res)
        self.assertTrue(1 not in res)
        self.assertTrue(5 not in res)

    def test_union(self):
        a = DRS([1, 2, 3])
        b = DRS([3, 4, 5])
        res = self.api.union(a, b)
        self.assertTrue(3 in res)
        self.assertTrue(1 in res)
        self.assertTrue(2 in res)
        self.assertTrue(4 in res)
        self.assertTrue(5 in res)

    def test_difference(self):
        a = DRS([1, 2, 3, 4])
        b = DRS([3, 4, 5])
        res = self.api.difference(a, b)
        self.assertTrue(2 in res)
        self.assertTrue(1 in res)
        self.assertTrue(3 not in res)
        self.assertTrue(4 not in res)
        self.assertTrue(5 not in res)

if __name__ == "__main__":
    unittest.main()
