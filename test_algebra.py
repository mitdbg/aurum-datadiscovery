import unittest
from modelstore.elasticstore import KWType
from api.apiutils import Scope, Relation
from algebra import API
from mock import MagicMock, patch


class TestAPI(unittest.TestCase):

    def setUp(self):
        self.m_network = MagicMock()
        self.m_store_client = MagicMock()

    def test_initialization(self, *args):
        api = API(self.m_network, self.m_store_client)

        self.assertEqual(api._network, self.m_network)
        self.assertEqual(api._store_client, self.m_store_client)
        # self.assertTrue(isinstance(api._store_client, MagicMock))


class testAlgebra(unittest.TestCase):

    def setUp(self):
        self.m_network = MagicMock()
        self.m_store_client = MagicMock()
        self.api = API(self.m_network, self.m_store_client)

    def test_keyword_search_db(self):
        # not implemented
        pass

    @patch('algebra.DRS', MagicMock(return_value='return_drs'))
    def test_keyword_search_source(self, *args):
        kw = 'foo'
        scope = Scope.SOURCE
        max_results = 11
        search_keyword = self.m_store_client.search_keyword

        result = self.api.keyword_search(
            kw=kw, scope=scope, max_results=max_results)

        self.m_network.assert_not_called()
        search_keyword.assert_called_with(
            keywords=kw, elasticfieldname=KWType.KW_TABLE,
            max_results=max_results)
        self.assertEqual(result, 'return_drs')

    @patch('algebra.DRS', MagicMock(return_value='return_drs'))
    def test_keyword_search_field(self):
        kw = 'foo'
        scope = Scope.FIELD
        max_results = 11
        search_keyword = self.m_store_client.search_keyword

        result = self.api.keyword_search(
            kw=kw, scope=scope, max_results=max_results)

        self.m_network.assert_not_called()
        search_keyword.assert_called_with(
            keywords=kw, elasticfieldname=KWType.KW_SCHEMA,
            max_results=max_results)
        self.assertEqual(result, 'return_drs')

    @patch('algebra.DRS', MagicMock(return_value='return_drs'))
    def test_keyword_search_content(self):
        kw = 'foo'
        scope = Scope.CONTENT
        max_results = 11
        search_keyword = self.m_store_client.search_keyword

        result = self.api.keyword_search(
            kw=kw, scope=scope, max_results=max_results)

        self.m_network.assert_not_called()
        search_keyword.assert_called_with(
            keywords=kw, elasticfieldname=KWType.KW_TEXT,
            max_results=max_results)
        self.assertEqual(result, 'return_drs')

    # @patch('algebra.DRS', MagicMock(return_value='return_drs'))
    # @patch('algebra._hit_from_node', MagicMock(return_value='return_hit'))
    # def test_neighbor_search_pkfk_node(self):
    #     db = 'db'
    #     source = 'source_table'
    #     field = 'column'
    #     node = (db, source, field)

    #     relation = Relation.PKFK
    #     max_results = 11

    #     result = self.api.neighbor_search(
    #         node=node, relation=relation, max_results=max_results)

    #     self.m_network.assert_called_with('return_hit', Relation.PKFK)
    #     self.assertEqual(result, 'return_drs')


class TestAlgebraHelpers(unittest.TestCase):

    def setUp(self):
        self.m_network = MagicMock()
        self.m_store_client = MagicMock()
        self.api = API(self.m_network, self.m_store_client)

    @patch('algebra.Hit', MagicMock(return_value='result_hit'))
    @patch('algebra.id_from', MagicMock())
    def test_node_to_hit(self):
        node = ('foo', 'bar', 'fizz')
        result = self.api._node_to_hit(node=node)
        self.assertEqual(result, 'result_hit')





if __name__ == '__main__':
    unittest.main()