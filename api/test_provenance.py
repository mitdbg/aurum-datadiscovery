import unittest
from api.apiutils import Relation
from ddapi import API
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network


class TestProvenance(unittest.TestCase):
    # create store handler
    store_client = StoreHandler()
    # read graph
    path = '../test/test4/'
    network = deserialize_network(path)
    api = API(network)
    api.init_store()

    def test_keyword_provenance(self):
        print(self._testMethodName)

        res = self.api.keyword_search("Madden", max_results=10)

        print(res.get_provenance().prov_graph().nodes())
        print(res.get_provenance().prov_graph().edges())

        el_interest = [x for x in res][0]

        info = res.why(el_interest)
        print("WHY " + str(el_interest) + "? " + str(info))

        explanation = res.how(el_interest)
        print("HOW " + str(el_interest) + "? " + str(explanation))

        self.assertTrue(True)

    def test_content_sim_provenance(self):
        print(self._testMethodName)

        table = 'Buildings.csv'
        res = self.api.similar_content_to_table(table)

        print(res.get_provenance().prov_graph().nodes())
        print(res.get_provenance().prov_graph().edges())

        el_interest = [x for x in res][0]

        info = res.why(el_interest)
        print("WHY " + str(el_interest) + "? " + str(info))

        explanation = res.how(el_interest)
        print("HOW " + str(el_interest) + "? " + str(explanation))

        self.assertTrue(True)

    def test_intersection_provenance(self):
        print(self._testMethodName)

        res1 = self.api.keyword_search("Madden", max_results=10)
        res2 = self.api.keyword_search("Stonebraker", max_results=10)

        res = res1.intersection(res2)

        print(res.get_provenance().prov_graph().nodes())
        print(res.get_provenance().prov_graph().edges())

        el_interest = [x for x in res][0]

        info = res.why(el_interest)
        print("WHY " + str(el_interest) + "? " + str(info))

        explanation = res.how(el_interest)
        print("HOW " + str(el_interest) + "? " + str(explanation))

        self.assertTrue(True)

    def test_tc_table_mode_provenance(self):
        print(self._testMethodName)

        field1 = ('dwhsmall', 'All_olap2_uentity_desc_uses.csv', 'Entity Owner')
        field2 = ('dwhsmall', 'All_olap_entity_desc_uses.csv', 'Entity Owner')

        drs1 = self.api.drs_from_raw_field(field1)
        drs2 = self.api.drs_from_raw_field(field2)

        drs1.set_table_mode()
        drs2.set_table_mode()

        res = self.api.paths_between(drs1, drs2, Relation.PKFK)

        print(res.get_provenance().prov_graph().nodes())
        print(res.get_provenance().prov_graph().edges())

        el_interest = [x for x in res][0]

        info = res.why(el_interest)
        print("WHY " + str(el_interest) + "? " + str(info))

        explanation = res.how(el_interest)
        print("HOW " + str(el_interest) + "? " + str(explanation))

        self.assertTrue(True)


if __name__ == "__main__":
    unittest.main()
