import unittest
from collections import namedtuple
from modelstore.elasticstore import KWType
from api.apiutils import Scope, Relation
from algebra import API, DRS
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

    """
    Neighbor Search
    """

    @patch('algebra.Algebra._scope_to_kw_type', MagicMock(return_value=0))
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
            keywords=kw, elasticfieldname=0,
            max_results=max_results)
        self.assertEqual(result, 'return_drs')

    @patch('algebra.Algebra._scope_to_kw_type', MagicMock(return_value=0))
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
            keywords=kw, elasticfieldname=0,
            max_results=max_results)
        self.assertEqual(result, 'return_drs')

    @patch('algebra.Algebra._scope_to_kw_type', MagicMock(return_value=0))
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
            keywords=kw, elasticfieldname=0,
            max_results=max_results)
        self.assertEqual(result, 'return_drs')

    """
    Neighbor Search
    """

    @patch('algebra.DRS', MagicMock(return_value=MagicMock()))
    def test_neighbor_search_pkfk_node(self):
        db = 'db'
        source = 'source_table'
        field = 'column'
        node = (db, source, field)

        relation = Relation.PKFK
        max_hops = 11
        # algebra.DRS.mode = 'foo'

        # mock out i_drs in neighbor_search. Make it iterable
        self.api._general_to_drs = MagicMock()
        # self.api._general_to_drs.return_value.mode = True
        # self.api._general_to_drs.return_value = iter(['foo', 'bar'])

        self.api.neighbor_search(
            general_input=node, relation=relation, max_hops=max_hops)

        self.api._general_to_drs.assert_called_with(node)


class TestAlgebraHelpers(unittest.TestCase):

    def setUp(self):
        self.m_network = MagicMock()
        self.m_store_client = MagicMock()
        self.api = API(self.m_network, self.m_store_client)

    @patch('algebra.Hit', MagicMock(return_value='result_hit'))
    def test_nid_to_node(self):
        self.api._network.get_info_for = MagicMock(
            return_value=('t', 'o', 'op', 'le'))
        nid = 123
        result = self.api._nid_to_hit(nid=nid)
        self.assertEqual(result, 'result_hit')

    @patch('algebra.Hit', MagicMock(return_value='result_hit'))
    @patch('algebra.id_from', MagicMock())
    def test_node_to_hit(self):
        node = ('foo', 'bar', 'fizz')
        result = self.api._node_to_hit(node=node)
        self.assertEqual(result, 'result_hit')

    @patch('algebra.DRS', MagicMock(return_value='result_drs'))
    def test_hit_to_drs_no_table_mode(self):
        self.api._network.get_hits_from_table = MagicMock()
        hit = 'hit'
        result = self.api._hit_to_drs(hit=hit)
        self.assertEqual(result, 'result_drs')
        self.api._network.get_hits_from_table.assert_not_called()

    @patch('algebra.DRS', MagicMock(return_value='result_drs'))
    def test_hit_to_drs_with_table_mode(self):
        self.api._network.get_hits_from_table = MagicMock()
        hit = namedtuple(
            'Hit', 'nid, db_name, source_name, field_name, score',
            verbose=False)
        hit.source_name = 'table'

        result = self.api._hit_to_drs(hit=hit, table_mode=True)
        self.assertEqual(result, 'result_drs')
        self.assertEqual(self.api._network.get_hits_from_table.called, True)

    @patch('algebra.id_from', MagicMock())
    @patch('algebra.isinstance', MagicMock(return_value=True))
    def test_general_to_drs(self):
        nid = 1
        self.api._nid_to_hit = MagicMock(return_value='n_hit')
        self.api._node_to_hit = MagicMock(return_value='t_hit')
        self.api._hit_to_drs = MagicMock(return_value='drs')

        result = self.api._general_to_drs(nid)

        self.api._nid_to_hit.assert_called_with(nid)
        self.api._node_to_hit.assert_called_with('n_hit')
        self.api._hit_to_drs.assert_called_with('t_hit')

        self.assertEqual(result, 'drs')

    @patch('algebra.isinstance', MagicMock(return_value=False))
    def test_general_to_drs_fail_case(self):
        self.assertRaises(ValueError, self.api._general_to_drs, 'bad input')

    def test_table_drs_to_field_drs(self):
        m_drs = MagicMock()
        m_drs.set_field_mode = MagicMock()

        self.api._general_to_drs = MagicMock(return_value=m_drs)
        self.api._hit_to_drs = MagicMock(return_value=True)

        self.api._general_to_field_drs('foo')

        # assert that this was called
        self.assertTrue(m_drs.set_fields_mode.called)

        # Having trouble testing anything inside of the iteration
        # there should be a test to make sure that
        # o_drs.absorb_provenance(i_drs) is called.

        # We should be able to check this, but have trouble because the i_drs
        # mock isn't flushed out as well as it should be.
        # self.m_network.assert_called_with('return_hit', Relation.PKFK)
        # self.assertEqual(result, 'return_drs')
        pass


if __name__ == '__main__':
    unittest.main()
