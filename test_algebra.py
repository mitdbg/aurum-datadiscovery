import unittest
from modelstore.elasticstore import KWType
from api.apiutils import Scope
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

    # @patch('algebra.StoreHandler')
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

        result = self.api.search_keyword(
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

        result = self.api.search_keyword(
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

        result = self.api.search_keyword(
            kw=kw, scope=scope, max_results=max_results)

        self.m_network.assert_not_called()
        search_keyword.assert_called_with(
            keywords=kw, elasticfieldname=KWType.KW_TEXT,
            max_results=max_results)
        self.assertEqual(result, 'return_drs')


if __name__ == '__main__':
    unittest.main()
