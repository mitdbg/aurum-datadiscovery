import unittest
from collections import namedtuple
from modelstore.elasticstore import KWType
from api.apiutils import Relation
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

    @patch('algebra.DRS', MagicMock(return_value='return_drs'))
    def test_keyword_search_source(self, *args):
        kw = 'foo'
        kw_type = 0
        max_results = 11
        search_keywords = self.m_store_client.search_keywords

        result = self.api.keyword_search(
            kw=kw, kw_type=kw_type, max_results=max_results)

        self.m_network.assert_not_called()
        search_keywords.assert_called_with(
            keywords=kw, elasticfieldname=0,
            max_hits=max_results)
        self.assertEqual(result, 'return_drs')

    @patch('algebra.DRS', MagicMock(return_value='return_drs'))
    def test_keyword_search_field(self):
        kw = 'foo'
        kw_type = 0
        max_results = 11
        search_keywords = self.m_store_client.search_keywords

        result = self.api.keyword_search(
            kw=kw, kw_type=kw_type, max_results=max_results)

        self.m_network.assert_not_called()
        search_keywords.assert_called_with(
            keywords=kw, elasticfieldname=0,
            max_hits=max_results)
        self.assertEqual(result, 'return_drs')

    @patch('algebra.DRS', MagicMock(return_value='return_drs'))
    def test_keyword_search_content(self):
        kw = 'foo'
        kw_type = 0
        max_results = 11
        search_keywords = self.m_store_client.search_keywords

        result = self.api.keyword_search(
            kw=kw, kw_type=kw_type, max_results=max_results)

        self.m_network.assert_not_called()
        search_keywords.assert_called_with(
            keywords=kw, elasticfieldname=0,
            max_hits=max_results)
        self.assertEqual(result, 'return_drs')

    def test_union(self):
        a = MagicMock()
        b = MagicMock()
        self.api._general_to_drs = MagicMock(return_value=a)

        res = self.api.union(a, b)

        self.api._general_to_drs.assert_called()
        self.assertEqual(a.union(b), res)

    def test_intersection(self):
        a = MagicMock()
        b = MagicMock()
        self.api._general_to_drs = MagicMock(return_value=a)
        self.api._assert_same_mode = MagicMock()

        res = self.api.intersection(a, b)

        self.api._general_to_drs.assert_called()
        self.assertEqual(a.intersection(b), res)
        pass

    def test_difference(self):
        a = MagicMock()
        b = MagicMock()
        self.api._general_to_drs = MagicMock(return_value=a)
        self.api._assert_same_mode = MagicMock()

        res = self.api.difference(a, b)

        self.api._general_to_drs.assert_called()
        self.assertEqual(a.set_difference(b), res)
        pass

    def test_paths(self):
        self.api._general_to_drs = MagicMock()
        pass

    @patch('algebra.DRS', MagicMock())
    def test_traverse(self):
        self.api._general_to_drs = MagicMock()
        res = self.api.traverse(a='drs', primitive='primitive')

        self.api._general_to_drs.assert_called()
        res.absorb_provenance.assert_called()

        pass

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
            return_value=[('t', 'o', 'op', 'le')])
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

    @patch('algebra.DRS', MagicMock())
    def test_hit_to_drs_with_table_mode(self):
        self.api._network.get_hits_from_table = MagicMock()
        hit = namedtuple(
            'Hit', 'nid, db_name, source_name, field_name, score',
            verbose=False)
        hit.source_name = 'table'

        self.api._hit_to_drs(hit=hit, table_mode=True)
        self.assertEqual(self.api._network.get_hits_from_table.called, True)

    @patch('algebra.id_from', MagicMock())
    @patch('algebra.isinstance', MagicMock(
        side_effect=[False, True, True, True, True, True, True]))
    @patch('algebra.DRS', MagicMock())
    def test_general_to_drs(self):
        nid = 1
        self.api._nid_to_hit = MagicMock()
        self.api._node_to_hit = MagicMock(return_value='t_hit')
        self.api._hit_to_drs = MagicMock(return_value='drs')

        result = self.api._general_to_drs(nid)

        self.api._nid_to_hit.assert_called_with(nid)
        # self.api._node_to_hit.assert_called_with('n_hit')
        # self.api._hit_to_drs.assert_called_with('n_hit')

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

    @patch('algebra.isinstance', MagicMock(return_value=True))
    def test_make_drs(self):
        general_input = ['foo', 'bar']
        self.api._general_to_drs = MagicMock(return_value=True)
        self.api.union = MagicMock(return_value=True)
        res = self.api.make_drs(general_input)

        self.assertTrue(res)


if __name__ == '__main__':
    #unittest.main()

    print("HERE")
    from main import init_system
    from api.apiutils import Relation

    api, reporting = init_system("/Users/ra-mit/development/discovery_proto/models/dwh2/")

    table = "Fac_building.csv"  # The table of interest
    table_drs = api.drs_from_table(table)  # We get the representation of that table in DRS
    similar_tables = api.similar_content_to(table_drs)

    print(str(similar_tables.__dict__()))

    hit = Hit(1, params[0], params[0], params[0], -1)

