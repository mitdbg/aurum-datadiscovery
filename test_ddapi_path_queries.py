import unittest
from api.apiutils import Relation
from ddapi import API
from modelstore.elasticstore import StoreHandler
from knowledgerepr.fieldnetwork import deserialize_network


class TestDDApiPathQueries(unittest.TestCase):

    # create store handler
    store_client = StoreHandler()
    # read graph
    path = 'models/chemical/'
    network = deserialize_network(path)
    api = API(network)
    api.init_store()

    """
    TC primitive API
    """

    def test_paths_between_field_mode(self):
        print(self._testMethodName)

        field1 = ('chembl_21', 'drug_indication', 'record_id')
        field2 = ('chembl_21', 'compound_records', 'record_id')

        drs1 = self.api.drs_from_raw_field(field1)
        drs2 = self.api.drs_from_raw_field(field2)

        res = self.api.paths_between(drs1, drs2, Relation.PKFK)

        data = [x for x in res]
        print("Total results: " + str(len(data)))
        for el in data:
            print(str(el))

    def test_paths_between_table_mode(self):
        print(self._testMethodName)

        field1 = ('chembl_21', 'drug_indication', 'record_id')
        field2 = ('chembl_21', 'compound_records', 'record_id')

        drs1 = self.api.drs_from_raw_field(field1)
        drs2 = self.api.drs_from_raw_field(field2)

        drs1.set_table_mode()
        drs2.set_table_mode()

        res = self.api.paths_between(drs1, drs2, Relation.PKFK)

        data = [x for x in res]
        print("Total results: " + str(len(data)))
        for el in data:
            print(str(el))

        print("Paths: ")
        res.visualize_provenance()
        res.debug_print()
        paths = res.paths()
        for p in paths:
            print(str(p))

    def test_paths_between_from_tables(self):
        print(self._testMethodName)

        table1_name = "drug_indication"
        table2_name = "compound_records"
        table1 = self.api.drs_from_table(table1_name)
        table2 = self.api.drs_from_table(table2_name)
        table1.set_table_mode()
        table2.set_table_mode()
        res = self.api.paths_between(table1, table2, Relation.PKFK)

        data = [x for x in res]
        print("Total results: " + str(len(data)))
        for el in data:
            print(str(el))

        print("Paths: ")
        paths = res.paths()
        for p in paths:
            print(str(p))

    def test_paths(self):
        print(self._testMethodName)

        return

    def test_traverse(self):
        print(self._testMethodName)

        field1 = ('chembl_21', 'drug_indication', 'record_id')
        drs_field = self.api.drs_from_raw_field(field1)
        res = self.api.traverse(drs_field, Relation.SCHEMA_SIM, 1)

        data = [x for x in res]
        print("Total results: " + str(len(data)))
        for el in data:
            print(str(el))

        return
