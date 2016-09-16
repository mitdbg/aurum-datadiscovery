import unittest
from api.apiutils import Relation
from ddapi import API
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network


class TestRanking(unittest.TestCase):
    # create store handler
    store_client = StoreHandler()
    # read graph
    path = '../test/dwh/'
    network = deserialize_network(path)
    api = API(network)
    api.init_store()

    def test_compute_ranking_scores_certainty(self):
        res = self.api.keyword_search("Madden", max_results=10)

        res = res.rank_certainty()

        res.pretty_print_columns_with_scores()

        self.assertTrue(True)

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
