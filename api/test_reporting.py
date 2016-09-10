import unittest
from api.apiutils import Relation
from api.reporting import Report
from ddapi import API
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network


class TestReporting(unittest.TestCase):
    # create store handler
    store_client = StoreHandler()
    # read graph
    path = '../test/test4/'
    network = deserialize_network(path)
    api = API(network)
    api.init_store()

    def test_compute_statistics(self):
        r = Report(self.network)
        ncols = r.num_columns
        ntables = r.num_tables
        ncontent = r.num_content_sim_relations
        nschema = r.num_schema_sim_relations
        npkfk = r.num_pkfk_relations
        print("Num cols: " + str(ncols))
        print("Num tables: " + str(ntables))
        print("Num content sim relations: " + str(ncontent))
        print("Num schema sim relations: " + str(nschema))
        print("Num PKFK relations: " + str(npkfk))
        #topfields = r.top_connected_fields(5)
        #print("TOP FIELDS: ")
        #for t in topfields:
        #    print(t)
