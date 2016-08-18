import unittest
from api.apiutils import DRS
from ddapi import API


"""
class TestDDApiPrimitives(unittest.TestCase):

    api = API(None)

    def test_keyword_search(self):
        return
"""

class TestDDApiCombiner(unittest.TestCase):

    api = API(None)

    """
    Seed API
    """

    def test_drs_from_raw_field(self):
        return

    def test_drs_from_hit(self):
        return

    def test_drs_from_table(self):
        return

    def test_drs_from_table_hit(self):
        return

    """
    Primitive API
    """

    def test_keyword_search(self):
        return

    def test_keywords_search(self):
        return

    def test_schema_name_search(self):
        return

    def test_schema_names_search(self):
        return

    def test_entity_search(self):
        return

    def test_schema_neighbors(self):
        return

    def test_schema_neighbors_of(self):
        return

    def test_similar_schema_name_to_field(self):
        return

    def test_similar_schema_name_to_table(self):
        return

    def test_similar_schema_name_to(self):
        return

    def test_similar_content_to_field(self):
        return

    def test_similar_content_to(self):
        return

    def test_pkfk_field(self):
        return

    def test_pkfk_table(self):
        return

    def test_pkfk_of(self):
        return

    """
    Combiner API
    """

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


    """
    TC primitive API
    """

    def test_paths_between(self):
        return

    def test_paths(self):
        return



if __name__ == "__main__":
    unittest.main()
