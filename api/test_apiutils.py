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

        h1 = Hit(0, "dba", "table_a", "a", -1)
        h2 = Hit(1, "dba", "table_a", "b", -1)
        h3 = Hit(2, "dba", "table_b", "c", -1)
        h4 = Hit(3, "dba", "table_b", "d", -1)
        drs = DRS([h1, h2, h3, h4], Operation(OP.ORIGIN))
        drs.set_fields_mode()

        for el in drs:
            print(str(el))

        self.assertTrue(True)

    def test_drs_table_iteration(self):
        print(self._testMethodName)

        h1 = Hit(0, "dba", "table_a", "a", -1)
        h2 = Hit(1, "dba", "table_a", "b", -1)
        h3 = Hit(2, "dba", "table_b", "c", -1)
        h4 = Hit(3, "dba", "table_b", "d", -1)
        drs = DRS([h1, h2, h3, h4], Operation(OP.ORIGIN))
        drs.set_table_mode()

        for el in drs:
            print(str(el))

        self.assertTrue(True)

    def test_creation_initial_provenance(self):
        print(self._testMethodName)

        h0 = Hit(10, "dba", "table_c", "v", -1)

        h1 = Hit(0, "dba", "table_a", "a", -1)
        h2 = Hit(1, "dba", "table_a", "b", -1)
        h3 = Hit(2, "dba", "table_b", "c", -1)
        h4 = Hit(3, "dba", "table_b", "d", -1)
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
        h0 = Hit(10, "dba", "table_c", "v", -1)

        h1 = Hit(0, "dba", "table_a", "a", -1)
        h2 = Hit(1, "dba", "table_a", "b", -1)
        h3 = Hit(2, "dba", "table_b", "c", -1)
        h4 = Hit(3, "dba", "table_b", "d", -1)
        drs1 = DRS([h1, h2, h3, h4], Operation(OP.CONTENT_SIM, params=[h0]))

        # DRS 2
        h5 = Hit(1, "dba", "table_a", "b", -1)

        h6 = Hit(16, "dba", "table_d", "a", -1)
        h7 = Hit(17, "dba", "table_d", "b", -1)
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

    def test_absorb(self):
        print(self._testMethodName)

        # DRS 1
        h0 = Hit(10, "dba", "table_c", "v", -1)

        h1 = Hit(0, "dba", "table_a", "a", -1)
        h2 = Hit(1, "dba", "table_a", "b", -1)
        h3 = Hit(2, "dba", "table_b", "c", -1)
        h4 = Hit(3, "dba", "table_b", "d", -1)
        drs1 = DRS([h1, h2, h3, h4], Operation(OP.CONTENT_SIM, params=[h0]))

        # DRS 2
        h5 = Hit(1, "dba", "table_a", "b", -1)

        h6 = Hit(16, "dba", "table_d", "a", -1)
        h7 = Hit(17, "dba", "table_d", "b", -1)
        drs2 = DRS([h6, h7], Operation(OP.SCHEMA_SIM, params=[h5]))

        drs = drs1.absorb(drs2)

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

        drs1_data = set([x for x in drs1])
        drs2_data = set([x for x in drs2])
        merged_data = set([x for x in drs])

        lm = len(merged_data)
        lu = len(drs1_data.union(drs2_data))

        print("Len must be 0: " + str(lu - lm))

        self.assertTrue((lu - lm) == 0)

    def test_intersection(self):
        print(self._testMethodName)

        # DRS 1
        h0 = Hit(10, "dba", "table_c", "v", -1)

        h1 = Hit(0, "dba", "table_a", "a", -1)
        h2 = Hit(1, "dba", "table_a", "b", -1)
        h3 = Hit(2, "dba", "table_b", "c", -1)
        h4 = Hit(3, "dba", "table_b", "d", -1)
        drs1 = DRS([h0, h1, h2, h3, h4], Operation(OP.ORIGIN))

        # DRS 2
        h5 = Hit(1, "dba", "table_a", "b", -1)

        h6 = Hit(16, "dba", "table_d", "a", -1)
        h7 = Hit(17, "dba", "table_d", "b", -1)
        drs2 = DRS([h5, h6, h7], Operation(OP.ORIGIN))

        drs = drs1.intersection(drs2)

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

        data = [x for x in drs]
        ld = len(data)

        print("Len must be 1: " + str(ld))

        self.assertTrue(ld == 1)

    def test_union(self):
        print(self._testMethodName)

        # DRS 1
        h0 = Hit(10, "dba", "table_c", "v", -1)

        h1 = Hit(0, "dba", "table_a", "a", -1)
        h2 = Hit(1, "dba", "table_a", "b", -1)
        h3 = Hit(2, "dba", "table_b", "c", -1)
        h4 = Hit(3, "dba", "table_b", "d", -1)
        drs1 = DRS([h0, h1, h2, h3, h4], Operation(OP.ORIGIN))

        # DRS 2
        h5 = Hit(1, "dba", "table_a", "b", -1)

        h6 = Hit(16, "dba", "table_d", "a", -1)
        h7 = Hit(17, "dba", "table_d", "b", -1)
        drs2 = DRS([h5, h6, h7], Operation(OP.ORIGIN))

        drs = drs1.union(drs2)

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

        data = [x for x in drs]
        ld = len(data)

        print("Len must be 7: " + str(ld))

        self.assertTrue(ld == 7)

    def test_sdifference(self):
        print(self._testMethodName)

        # DRS 1
        h0 = Hit(10, "dba", "table_c", "v", -1)

        h1 = Hit(0, "dba", "table_a", "a", -1)
        h2 = Hit(1, "dba", "table_a", "b", -1)
        h3 = Hit(2, "dba", "table_b", "c", -1)
        h4 = Hit(3, "dba", "table_b", "d", -1)
        drs1 = DRS([h0, h1, h2, h3, h4], Operation(OP.ORIGIN))

        # DRS 2
        h5 = Hit(1, "dba", "table_a", "b", -1)

        h6 = Hit(16, "dba", "table_d", "a", -1)
        h7 = Hit(17, "dba", "table_d", "b", -1)
        drs2 = DRS([h5, h6, h7], Operation(OP.ORIGIN))

        drs = drs1.set_difference(drs2)

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

        data = [x for x in drs]
        ld = len(data)

        print("Len must be 4: " + str(ld))

        self.assertTrue(ld == 4)


if __name__ == "__main__":
    unittest.main()
