import unittest
from modelstore.elasticstore import KWType
from api.apiutils import Scope
from algebra import API
from mock import Mock, \
                 MagicMock, \
                 patch, \
                 mock_open


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


        # hits = store_client.search_keywords(kw, KWType.KW_TEXT, max_results)
        # drs = DRS([x for x in hits], Operation(OP.KW_LOOKUP, params=[kw]))  # materialize generator
        # return drs

    # def test_keyword_search_source(self):
    #     kw = 'foo'
    #     scope = Scope.SOURCE
    #     max_results = 11
    #     search_keywords = self.m_store_client.search_keywords

    #     self.api.search_keyword(kw=kw, scope=scope, max_results=max_results)

    #     self.m_network.assert_not_called()
    #     # import pdb; pdb.set_trace()
    #     search_keywords.assert_called_with(
    #         kw=kw, elasticfieldname=KWType.KW_TABLE, max_results=max_results)

    #     # pass

    def test_keyword_search_field(self):
        pass

    def test_keyword_search_content(self):
        pass




    # def test_isupper(self):
    #     self.assertTrue('FOO'.isupper())
    #     self.assertFalse('Foo'.isupper())

    # def test_split(self):
    #     s = 'hello world'
    #     self.assertEqual(s.split(), ['hello', 'world'])
    #     # check that s.split fails when the separator is not a string
    #     with self.assertRaises(TypeError):
    #         s.split(2)


if __name__ == '__main__':
    unittest.main()
