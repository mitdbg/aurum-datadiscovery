import unittest
from api.apiutils import DRS
from api.apiutils import Operation
from api.apiutils import OP
from api.apiutils import Hit
from ddapi import API


class TestApiutils(unittest.TestCase):

    api = API(None)

    def test_drs_field_iteration(self):
        print(self._testMethodName)

        h1 = Hit(0, "table_a", "a", -1)
        h2 = Hit(1, "table_a", "b", -1)
        h3 = Hit(2, "table_b", "c", -1)
        h4 = Hit(3, "table_b", "d", -1)
        drs = DRS([h1, h2, h3, h4], Operation(OP.ORIGIN))
        drs.set_fields_mode()

        for el in drs:
            print(str(el))

        self.assertTrue(True)

    def test_drs_table_iteration(self):
        print(self._testMethodName)

        h1 = Hit(0, "table_a", "a", -1)
        h2 = Hit(1, "table_a", "b", -1)
        h3 = Hit(2, "table_b", "c", -1)
        h4 = Hit(3, "table_b", "d", -1)
        drs = DRS([h1, h2, h3, h4], Operation(OP.ORIGIN))
        drs.set_table_mode()

        for el in drs:
            print(str(el))

        self.assertTrue(True)

    def test_creation_initial_provenance(self):
        print(self._testMethodName)

        h0 = Hit(10, "table_c", "v", -1)

        h1 = Hit(0, "table_a", "a", -1)
        h2 = Hit(1, "table_a", "b", -1)
        h3 = Hit(2, "table_b", "c", -1)
        h4 = Hit(3, "table_b", "d", -1)
        drs = DRS([h1, h2, h3, h4], Operation(OP.CONTENT_SIM, params=[h0]))

        prov_graph = drs.get_provenance().prov_graph()
        nodes = prov_graph.nodes()
        print("NODES")
        for n in nodes:
            print(str(n))
        print(" ")
        edges = prov_graph.edges(keys=True)
        print("EDGES")
        for e in edges:
            print(str(e))
        print(" ")

        self.assertTrue(True)

    def test_absorb_provenance(self):
        print(self._testMethodName)

        # DRS 1
        h0 = Hit(10, "table_c", "v", -1)

        h1 = Hit(0, "table_a", "a", -1)
        h2 = Hit(1, "table_a", "b", -1)
        h3 = Hit(2, "table_b", "c", -1)
        h4 = Hit(3, "table_b", "d", -1)
        drs1 = DRS([h1, h2, h3, h4], Operation(OP.CONTENT_SIM, params=[h0]))

        # DRS 2
        h5 = Hit(1, "table_a", "b", -1)

        h6 = Hit(16, "table_d", "a", -1)
        h7 = Hit(17, "table_d", "b", -1)
        drs2 = DRS([h6, h7], Operation(OP.SCHEMA_SIM, params=[h5]))

        drs = drs1.absorb_provenance(drs2)

        prov_graph = drs.get_provenance().prov_graph()
        nodes = prov_graph.nodes()
        print("NODES")
        for n in nodes:
            print(str(n))
        print(" ")
        edges = prov_graph.edges(keys=True)
        print("EDGES")
        for e in edges:
            print(str(e))
        print(" ")

        init_data = set([x for x in drs1])
        merged_data = set([x for x in drs])
        new_data = init_data - merged_data

        print("Len must be 0: " + str(len(new_data)))

        self.assertTrue(len(new_data) == 0)


if __name__ == "__main__":
    unittest.main()