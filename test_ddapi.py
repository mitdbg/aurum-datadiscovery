import unittest
from api.apiutils import Relation
from ddapi import API
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network


class TestDDApiCombiner(unittest.TestCase):

    # create store handler
    store_client = StoreHandler()
    # read graph
    path = 'test/mitdwh.pickle'
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

    def test_ids_functions(self):
        print(self._testMethodName)

        field = ('Buildings.csv', 'Building Key')
        drs1 = self.api.drs_from_raw_field(field)

        field = ('Building Key', 'Buildings.csv')
        drs2 = self.api.drs_from_raw_field(field)

        for el in drs1:
            print(str(el))
        for el in drs2:
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

        field = ('Buildings.csv', 'Building Key')
        res = self.api.similar_schema_name_to_field(field)

        res = self.api.similar_schema_name_to(res)

        print("RES size: " + str(res.size()))
        for el in res:
            print(str(el))

    def test_similar_content_to_field(self):
        print(self._testMethodName)

        field = ('Buildings.csv', 'Building Name')
        res = self.api.similar_content_to_field(field)

        print("RES size: " + str(res.size()))
        for el in res:
            print(str(el))

    def test_similar_content_to_table(self):
        print(self._testMethodName)

        table = 'Buildings.csv'
        res = self.api.similar_content_to_table(table)

        print("RES size: " + str(res.size()))
        for el in res:
            print(str(el))

    def test_similar_content_to(self):
        print(self._testMethodName)

        field = ('Buildings.csv', 'Building Name')
        res = self.api.similar_content_to_field(field)

        res = self.api.similar_content_to(res)

        print("RES size: " + str(res.size()))
        for el in res:
            print(str(el))

    def test_pkfk_field(self):
        print(self._testMethodName)

        field = ('Buildings.csv', 'Building Name')
        res = self.api.pkfk_field(field)

        print("RES size: " + str(res.size()))
        for el in res:
            print(str(el))

    def test_pkfk_table(self):
        print(self._testMethodName)

        table = 'Buildings.csv'
        res = self.api.pkfk_table(table)

        print("RES size: " + str(res.size()))
        for el in res:
            print(str(el))

    def test_pkfk_of(self):
        print(self._testMethodName)

        field = ('Buildings.csv', 'Building Name')
        res = self.api.pkfk_field(field)

        res = self.api.pkfk_of(res)

        print("RES size: " + str(res.size()))
        for el in res:
            print(str(el))

    """
    Combiner API
    """

    def test_intersection(self):
        print(self._testMethodName)

        res1 = self.api.keyword_search("Madden", max_results=10)
        res2 = self.api.keyword_search("Stonebraker", max_results=10)

        res = res1.intersection(res2)

        for el in res:
            print(str(el))

    def test_union(self):
        print(self._testMethodName)

        res1 = self.api.keyword_search("Madden", max_results=10)
        res2 = self.api.schema_name_search("Stonebraker", max_results=10)

        res = res1.union(res2)

        for el in res:
            print(str(el))

    def test_difference(self):
        print(self._testMethodName)

        res1 = self.api.keyword_search("Madden", max_results=10)
        res2 = self.api.keyword_search("Stonebraker", max_results=10)

        res = res1.set_difference(res2)

        for el in res:
            print(str(el))

    """
    TC primitive API
    """

    def test_paths_between_field_mode(self):
        print(self._testMethodName)

        field1 = ('All_olap2_uentity_desc_uses.csv', 'Entity Owner')
        field2 = ('All_olap_entity_desc_uses.csv', 'Entity Owner')

        drs1 = self.api.drs_from_raw_field(field1)
        drs2 = self.api.drs_from_raw_field(field2)

        res = self.api.paths_between(drs1, drs2, Relation.PKFK)

        for el in res:
            print(str(el))

    def test_paths_between_table_mode(self):
        print(self._testMethodName)

        field1 = ('All_olap2_uentity_desc_uses.csv', 'Entity Owner')
        field2 = ('All_olap_entity_desc_uses.csv', 'Entity Owner')

        drs1 = self.api.drs_from_raw_field(field1)
        drs2 = self.api.drs_from_raw_field(field2)

        drs1.set_table_mode()
        drs2.set_table_mode()

        res = self.api.paths_between(drs1, drs2, Relation.PKFK)

        for el in res:
            print(str(el))

    def test_paths(self):
        print(self._testMethodName)

        return

    """
    Analytical API
    """

    def test_enumerate_all_pkfk(self):
        self.api.enumerate_all_pkfk()


if __name__ == "__main__":
    unittest.main()
