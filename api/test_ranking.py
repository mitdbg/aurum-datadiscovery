import unittest
from api.apiutils import Relation
from ddapi import API
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network
from knowledgerepr.syn_network_generator import generate_network_with as GENSYN


class TestRanking(unittest.TestCase):
    # create store handler
    store_client = StoreHandler()

    # create synthetic graph
    network = GENSYN(5, 5, 20, 50, 10)

    api = API(network)
    api.init_store()

    def test_compute_ranking_scores_certainty(self):

        nodes = self.network.fields_degree(3)

        #self.network._visualize_graph()

        nids = [x for x, y in nodes]

        info = self.network.get_info_for(nids)
        hits = self.network.get_hits_from_info(info)

        drs_info = self.api.drs_from_hits(hits)

        #drs_info.visualize_provenance()

        res = self.api.similar_schema_name_to(drs_info)

        #res.visualize_provenance(labels=True)

        res = res.rank_coverage()

        res.pretty_print_columns_with_scores()

        self.assertTrue(True)

    """
    def test_compute_ranking_scores_coverage(self):
        table = 'Buildings.csv'
        res = self.api.similar_content_to_table(table)

        res = res.rank_coverage()

        res.pretty_print_columns_with_scores()
        res.print_tables_with_scores()

        self.assertTrue(True)

    def test_compute_ranking_scores_certainty_table(self):
        table = 'Buildings.csv'
        res = self.api.similar_content_to_table(table)

        res = res.rank_certainty()

        res.print_tables_with_scores()

        res.print_columns()

        self.assertTrue(True)
    """