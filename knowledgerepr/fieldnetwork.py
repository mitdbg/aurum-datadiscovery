from enum import Enum
import networkx as nx


class Relation(Enum):
    SCHEMA = 0
    SCHEMA_SIM = 1
    CONTENT_SIM = 2
    ENTITY_SIM = 3
    OVERLAP = 4
    PKFK = 5


def compute_field_id(source_name, field_name):
    def java_hash_code(string):
        str_len = len(string)
        h = 0  # will keep the hash
        for char in string:
            for i in range(str_len):
                h = 31 * h + ord(char)
        return h

    string = source_name + field_name
    nid = java_hash_code(string)
    return nid


class Node:
    __nid = None
    __source_name = None
    __field_name = None

    def __init__(self, source_name, field_name):
        self.__nid = compute_field_id(source_name, field_name)
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
        if self.__nid == y.__nid:
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


def serialize_network(network, path):
    G = network.G
    nx.write_gpickle(G, path)


def deserialize_network(path):
    G = nx.read_gpickle(path)
    network = FieldNetwork(G)
    return network


if __name__ == "__main__":
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
