import unittest
from api.apiutils import DRS
from api.apiutils import Operation
from api.apiutils import OP
from api.apiutils import Hit
from ddapi import API
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network


class TestProvenance(unittest.TestCase):
    # create store handler
    store_client = StoreHandler()
    # read graph
    path = '../test/mitdwh.pickle'
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




if __name__ == "__main__":
    unittest.main()
