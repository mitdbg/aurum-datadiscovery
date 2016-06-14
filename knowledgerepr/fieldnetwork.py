from collections import namedtuple
from enum import Enum
import operator
import networkx as nx
import binascii

BaseHit = namedtuple('Hit', 'nid, sourcename, fieldname, score', verbose=False)


class Hit(BaseHit):
    def __hash__(self):
        hsh = int(self.nid)
        return hsh

    def __eq__(self, other):
        return self.nid == other.nid


class Relation(Enum):
    SCHEMA = 0
    SCHEMA_SIM = 1
    CONTENT_SIM = 2
    ENTITY_SIM = 3
    OVERLAP = 4
    PKFK = 5


class Node:
    __nid = None
    __source_name = None
    __field_name = None

    @staticmethod
    def compute_field_id(source_name, field_name):
        string = source_name + field_name
        nid = binascii.crc32(bytes(string, encoding="UTF-8"))
        return nid

    def __init__(self, source_name, field_name):
        self.__nid = self.compute_field_id(source_name, field_name)
        self.__source_name = source_name
        self.__field_name = field_name

    @property
    def nid(self):
        return self.__nid

    @property
    def field_name(self):
        return self.__field_name

    @property
    def source_name(self):
        return self.__source_name

    def __str__(self):
        return self.__source_name + " - " + self.__field_name

    def __hash__(self):
        return self.__nid

    def __eq__(self, y):
        if isinstance(y, int):  # cover the case when id is provided directly
            if self.__nid == y:
                return True
        elif self.__nid == y.__nid:  # cover the case of comparing two nodes
            return True
        return False


class FieldNetwork:
    # The core graph
    __G = nx.MultiGraph()

    def __init__(self, graph=None):
        if graph is None:
            self.__G = nx.MultiGraph()
        else:
            self.__G = graph

    def _get_underlying_repr(self):
        return self.__G

    def relations_between(self, node1, node2):
        return self.__G[node1][node2]

    def relation_between(self, node1, node2, relation):
        return self.__G[node1][node2][relation]

    def add_field(self, source_name, field_name):
        """
        Creates a graph node for this field and adds it to the graph
        :param source_name: of the field
        :param field_name: of the field
        :return: the newly added field node
        """
        n = Node(source_name, field_name)
        self.__G.add_node(n)
        return n

    def add_fields(self, list_of_fields):
        """
        Creates a list of graph nodes from the list of fields and adds them to the graph
        :param list_of_fields: list of (source_name, field_name) tuples
        :return: the newly added list of field nodes
        """
        nodes = []
        for sn, fn in list_of_fields:
            n = Node(sn, fn)
            nodes.append(n)
        self.__G.add_nodes_from(nodes)
        return nodes

    def add_relation(self, node_src, node_target, relation, score):
        """
        Adds or updates the score of relation for the edge between node_src and node_target
        :param node_src: the source node
        :param node_target: the target node
        :param relation: the type of relation (edge)
        :param score: the numerical value of the score
        :return:
        """
        score = {'score': score}
        self.__G.add_edge(node_src, node_target, relation, score)

    def fields_degree(self, topk):
        degree = nx.degree(self.__G)
        sorted_degree = sorted(degree.items(), key=operator.itemgetter(1))
        sorted_degree.reverse()
        topk_nodes = sorted_degree[:topk]
        return topk_nodes

    def neighbors(self, field, relation):
        sn, cn = field
        nid = Node.compute_field_id(sn, cn)
        neighbours = self.__G[nid]
        for k, v in neighbours.items():
            if relation in v:
                score = v[relation]['score']
                yield Hit(k.nid, k.source_name, k.field_name, score)
        return []

def serialize_network(network, path):
    G = network._get_underlying_repr()
    nx.write_gpickle(G, path)


def deserialize_network(path):
    G = nx.read_gpickle(path)
    network = FieldNetwork(G)
    return network

def test():
    sn = "sourcename"
    fn = "fieldname"
    import time
    id = ""
    strng = sn + fn
    s = time.time()
    for i in range(1000000):
        id = hash(strng)
    e = time.time()
    print("builtin hash: " + str(e - s))
    print(str(id))
    s = time.time()
    for i in range(1000000):
        id = Node.compute_field_id(sn, fn)
    e = time.time()
    print(str(id))
    print("custom hash: " + str(e-s))
    import binascii
    s = time.time()

    for i in range(1000000):
        id = binascii.crc32(bytes(strng, encoding="UTF-8"))
    e = time.time()
    print(str(id))
    print("binascii hash: " + str(e - s))

if __name__ == "__main__":
    test()
    exit()
    print("Field Network")
    node1 = Node("source1", "field1")
    node2 = Node("source1", "field2")
    node3 = Node("source1", "field1")

    assert node1 != node2
    assert node2 != node3
    n1 = hash(node1)
    n2 = hash(node2)
    n3 = hash(node3)
    assert node1 == node3
    print("node equality: OK")

    FN = FieldNetwork()

    n1 = FN.add_field("source1", "field1")
    n2 = FN.add_field("source1", "field2")
    n3 = FN.add_field("source2", "field1")
    FN.add_relation(n1, n2, Relation.SCHEMA, 1)
    FN.add_relation(n1, n2, Relation.CONTENT_SIM, 0.33)
    FN.add_relation(n1, n3, Relation.OVERLAP, 0.97)

    print(str(FN.relations_between(n1, n2)))
    print("SCHEMA: " + str(FN.relation_between(n1, n2, Relation.SCHEMA)))
    print("CONTENT_SIM: " + str(FN.relation_between(n1, n2, Relation.CONTENT_SIM)))
    print("graph access through node: OK")

    print("ALL GOOD")
