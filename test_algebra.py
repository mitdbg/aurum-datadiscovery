import unittest
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

        self.assertEqual(api._Algebra__network, self.m_network)
        self.assertEqual(api._Algebra__store_client, self.m_store_client)
        # self.assertTrue(isinstance(api._Algebra__store_client, MagicMock))


class testAlgebra(unittest.TestCase):

    # @patch('algebra.StoreHandler')
    def setUp(self):
        self.m_network = MagicMock()
        self.m_store_client = MagicMock()
        self.api = API(self.m_network, self.m_store_client)

    def test_keyword_search_db(self):
        kw = 'foo'
        scope = Scope.DB
        self.api.search_keyword(kw=kw, scope=scope)
        self.m_network.assert_not_called()

    def test_keyword_search_source(self):
        pass

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
