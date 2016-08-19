import unittest
from api.apiutils import DRS
from ddapi import API
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network


class TestDDApiCombiner(unittest.TestCase):

    # create store handler
    store_client = StoreHandler()
    # read graph
    path = 'test/network.pickle'
    network = deserialize_network(path)
    api = API(network)
    api.init_store()

    """
    Seed API
    """

    def test_drs_from_raw_field(self):
        print(self._testMethodName)

        field = ('Iap_subject_person.csv', 'Person Mit Affiliation')
        res = self.api.drs_from_raw_field(field)

        for el in res:
            print(str(el))

    def test_drs_from_hit(self):
        print(self._testMethodName)

        field = ('Iap_subject_person.csv', 'Person Mit Affiliation')
        res = self.api.drs_from_raw_field(field)

        els = [x for x in res]
        el = els[0]

        res = self.api.drs_from_hit(el)

        for el in res:
            print(str(el))

    def test_drs_from_table(self):
        print(self._testMethodName)

        table = 'Iap_subject_person.csv'
        res = self.api.drs_from_table(table)

        for el in res:
            print(el)

    def test_drs_from_table_hit(self):
        print(self._testMethodName)

        field = ('Iap_subject_person.csv', 'Person Mit Affiliation')
        res = self.api.drs_from_raw_field(field)

        els = [x for x in res]
        el = els[0]

        res = self.api.drs_from_table_hit(el)

        for el in res:
            print(str(el))

    """
    Primitive API
    """

    def test_keyword_search(self):
        print(self._testMethodName)

        res = self.api.keyword_search("Madden", max_results=10)

        for el in res:
            print(str(el))

    def test_keywords_search(self):
        print(self._testMethodName)

        res = self.api.keywords_search(["Madden", "Stonebraker", "Liskov"])

        for el in res:
            print(str(el))

    def test_schema_name_search(self):
        print(self._testMethodName)

        res = self.api.schema_name_search("Name", max_results=10)

        for el in res:
            print(str(el))

    def test_schema_names_search(self):
        print(self._testMethodName)

        res = self.api.schema_names_search(["Name", "Last Name", "Employee"])

        for el in res:
            print(str(el))

    def test_entity_search(self):
        print(self._testMethodName)
        print("Future Work...")
        return

    def test_schema_neighbors(self):
        print(self._testMethodName)

        field = ('Iap_subject_person.csv', 'Person Mit Affiliation')
        res = self.api.schema_neighbors(field)

        for el in res:
            print(str(el))

    def test_schema_neighbors_of(self):
        print(self._testMethodName)

        field = ('Iap_subject_person.csv', 'Person Mit Affiliation')
        res = self.api.schema_neighbors(field)

        res = self.api.schema_neighbors_of(res)

        for el in res:
            print(str(el))

    def test_similar_schema_name_to_field(self):
        print(self._testMethodName)

        field = ('Buildings.csv', 'Building Name')
        res = self.api.similar_schema_name_to_field(field)

        print("RES size: " + str(res.size()))
        for el in res:
            print(str(el))

    def test_similar_schema_name_to_table(self):
        print(self._testMethodName)

        table = 'Buildings.csv'
        res = self.api.similar_schema_name_to_table(table)

        print("RES size: " + str(res.size()))
        for el in res:
            print(str(el))

    def test_similar_schema_name_to(self):
        print(self._testMethodName)

        return

    def test_similar_content_to_field(self):
        print(self._testMethodName)

        return

    def test_similar_content_to(self):
        print(self._testMethodName)

        return

    def test_pkfk_field(self):
        print(self._testMethodName)

        return

    def test_pkfk_table(self):
        print(self._testMethodName)

        return

    def test_pkfk_of(self):
        print(self._testMethodName)

        return

    """
    Combiner API
    """

    def test_intersection(self):
        print(self._testMethodName)

        return
        #a = DRS([1, 2, 3])
        #b = DRS([3, 4, 5])
        #res = self.api.intersection(a, b)
        #self.assertTrue(3 in res)
        #self.assertTrue(2 not in res)
        #self.assertTrue(1 not in res)
        #self.assertTrue(5 not in res)

    def test_union(self):
        print(self._testMethodName)

        return
        #a = DRS([1, 2, 3])
        #b = DRS([3, 4, 5])
        #res = self.api.union(a, b)
        #self.assertTrue(3 in res)
        #self.assertTrue(1 in res)
        #self.assertTrue(2 in res)
        #self.assertTrue(4 in res)
        #self.assertTrue(5 in res)

    def test_difference(self):
        print(self._testMethodName)

        return
        #a = DRS([1, 2, 3, 4])
        #b = DRS([3, 4, 5])
        #res = self.api.difference(a, b)
        #self.assertTrue(2 in res)
        #self.assertTrue(1 in res)
        #self.assertTrue(3 not in res)
        #self.assertTrue(4 not in res)
        #self.assertTrue(5 not in res)


    """
    TC primitive API
    """

    def test_paths_between(self):
        print(self._testMethodName)

        return

    def test_paths(self):
        print(self._testMethodName)

        return


if __name__ == "__main__":
    unittest.main()
