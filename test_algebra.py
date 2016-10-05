import unittest
from algebra import API
from mock import Mock, \
                 MagicMock, \
                 patch, \
                 mock_open


class TestAPI(unittest.TestCase):

    def setUp(self):
        self.mock_network = MagicMock()

    @patch('algebra.StoreHandler')
    def test_initialization(self, *args):
        api = API(self.mock_network)
        self.assertEqual(api._Algebra__network, self.mock_network)
        self.assertTrue(isinstance(api._Algebra__store_client, MagicMock))




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
