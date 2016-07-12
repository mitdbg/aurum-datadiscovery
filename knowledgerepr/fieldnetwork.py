from collections import namedtuple
from enum import Enum
import operator
import networkx as nx
import binascii

BaseHit = namedtuple('Hit', 'nid, source_name, field_name, score', verbose=False)


def compute_field_id(source_name, field_name):
    string = source_name + field_name
    nid = binascii.crc32(bytes(string, encoding="UTF-8"))
    return nid


def build_hit(sn, fn):
    nid = compute_field_id(sn, fn)
    return Hit(nid, sn, fn, -1)


class Hit(BaseHit):
    def __hash__(self):
        hsh = int(self.nid)
        return hsh

    def __eq__(self, other):
        if isinstance(other, int):  # cover the case when id is provided directly
            if self.nid == other:
                return True
        elif isinstance(other, Hit):  # cover the case of comparing a Node with a Hit
            if self.nid == other.nid:
                return True
        elif self.nid == other.nid:  # cover the case of comparing two nodes
            return True
        return False


class Relation(Enum):
    SCHEMA = 0
    SCHEMA_SIM = 1
    CONTENT_SIM = 2
    ENTITY_SIM = 3
    PKFK = 5


class FieldNetwork:
    # The core graph
    __G = nx.MultiGraph()

    def __init__(self, graph=None):
        if graph is None:
            self.__G = nx.MultiGraph()
        else:
            self.__G = graph

    def get_cardinality_of(self, node):
        c = self.__G[node]
        card = 0  # no cardinality key is like cardinality = 0
        if 'cardinality' in c:  # FIXME: why cardinality may not be present?
            card = c['cardinality']
        return card

    def _get_underlying_repr(self):
        return self.__G

    def enumerate_fields(self):
        for n in self.__G.nodes():
            yield n

    def relations_between(self, node1, node2):
        return self.__G[node1][node2]

    def relation_between(self, node1, node2, relation):
        return self.__G[node1][node2][relation]

    def add_field(self, source_name, field_name, cardinality=None):
        """
        Creates a graph node for this field and adds it to the graph
        :param source_name: of the field
        :param field_name: of the field
        :return: the newly added field node
        """
        nid = compute_field_id(source_name, field_name)
        n = Hit(nid, source_name, field_name, -1)
        self.__G.add_node(n)
        if cardinality is not None:
            self.__G[n]['cardinality'] = cardinality
        return n

    def add_fields(self, list_of_fields):
        """
        Creates a list of graph nodes from the list of fields and adds them to the graph
        :param list_of_fields: list of (source_name, field_name) tuples
        :return: the newly added list of field nodes
        """
        nodes = []
        for nid, sn, fn in list_of_fields:
            n = Hit(nid, sn, fn, -1)
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

    def enumerate_pkfk(self):
        for n in self.__G.nodes():
            neighbors = self.neighbors((n.source_name, n.field_name), Relation.PKFK)
            for nid, sn, fn, score in neighbors:
                print(str(n.source_name) + "-" + str(n.field_name) + " <-> " + str(sn) + "-" + str(fn))

    def neighbors(self, field, relation):
        sn, cn = field
        nid = compute_field_id(sn, cn)
        neighbours = self.__G[nid]
        for k, v in neighbours.items():
            if str(k) == 'cardinality':
                continue  # skipping node attributes
            if relation in v:
                score = v[relation]['score']
                yield Hit(k.nid, k.source_name, k.field_name, score)
        return []

    def _bidirectional_pred_succ(self, source, target, relation):
        """Bidirectional shortest path helper.
           :returns (pred,succ,w) where
           :param pred is a dictionary of predecessors from w to the source, and
           :param succ is a dictionary of successors from w to the target.
        """
        # does BFS from both source and target and meets in the middle
        if target == source:
            return ({target: None}, {source: None}, source)

        # we always have an undirected graph
        Gpred = self.neighbors
        Gsucc = self.neighbors

        # predecesssor and successors in search
        pred = {source: None}
        succ = {target: None}

        # initialize fringes, start with forward
        forward_fringe = [source]
        reverse_fringe = [target]

        while forward_fringe and reverse_fringe:
            if len(forward_fringe) <= len(reverse_fringe):
                this_level = forward_fringe
                forward_fringe = []
                for v in this_level:
                    n = (v.source_name, v.field_name)
                    for w in Gsucc(n, relation):
                        if w not in pred:
                            forward_fringe.append(w)
                            pred[w] = v
                        if w in succ: return pred, succ, w  # found path
            else:
                this_level = reverse_fringe
                reverse_fringe = []
                for v in this_level:
                    n = (v.source_name, v.field_name)
                    for w in Gpred(n, relation):
                        if w not in succ:
                            succ[w] = v
                            reverse_fringe.append(w)
                        if w in pred: return pred, succ, w  # found path

        return None

    def bidirectional_shortest_path(self, source, target, relation):
        """
        Return a list of nodes in a shortest path between source and target.
        :param source is one extreme of the potential path
        :param target is the other extreme of the potential path
        :return path is a list of Node/Hit
        Note: This code is based on the networkx implementation
        """
        # call helper to do the real work
        results = self._bidirectional_pred_succ(source, target, relation)
        if results == None:  # check for None result
            return []
        pred, succ, w = results

        # build path from pred+w+succ
        path = []
        # from source to w
        while w is not None:
            path.append(w)
            w = pred[w]
        path.reverse()
        # from w to target
        w = succ[path[-1]]
        while w is not None:
            path.append(w)
            w = succ[w]
        return path

    def find_path(self, source, target, relation):
        (sn, fn) = source
        source = Hit(nid=0, source_name=sn, field_name=fn, score=0)
        (sn, fn) = target
        target = Hit(nid=0, source_name=sn, field_name=fn, score=0)
        path = self.bidirectional_shortest_path(source, target, relation)
        return path


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
        id = compute_field_id(sn, fn)
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
    #test()
    print("Field Network")
    node1 = build_hit("source1", "field1")
    node2 = build_hit("source1", "field2")
    node3 = build_hit("source1", "field1")

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
