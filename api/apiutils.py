from collections import namedtuple
from enum import Enum
import binascii
import networkx as nx

global_origin_id = 0

BaseHit = namedtuple(
    'Hit', 'nid, source_name, field_name, score', verbose=False)


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


def compute_field_id(source_name, field_name):
    string = source_name + field_name
    nid = binascii.crc32(bytes(string, encoding="UTF-8"))
    return nid


class Relation(Enum):
    SCHEMA = 0
    SCHEMA_SIM = 1
    CONTENT_SIM = 2
    ENTITY_SIM = 3
    PKFK = 5


class OP(Enum):
    NONE = 0  # for initial creation of DRS
    ORIGIN = 1
    KW_LOOKUP = 2
    SCHNAME_LOOKUP = 3
    SCHEMA_SIM = 4
    TABLE = 5
    CONTENT_SIM = 6
    PKFK = 7
    ENTITY_SIM = 8
    ENTITY_LOOKUP = 9


class Operation:

    def __init__(self, op: OP, params=None):
        self._op = op
        self._params = params

    @property
    def op(self):
        return self._op

    @property
    def params(self):
        return self._params


class DRSMode(Enum):
    FIELDS = 0
    TABLE = 1


class Provenance:
    """
    Nodes are Hit (only). Origin nodes are given a special Hit object too.
    """

    def __init__(self, data, operation):
        self._p_graph = nx.MultiDiGraph()
        op = operation.op
        params = operation.params
        self.populate_provenance(data, op, params)

    def prov_graph(self):
        return self._p_graph

    def swap_p_graph(self, new):
        self._p_graph = new

    def populate_provenance(self, data, op, params):
        if op == OP.NONE:
            # This is a carrier DRS, skip
            return
        elif op == OP.ORIGIN:
            for element in data:
                self._p_graph.add_node(element)
        # We check operations that come with parameters
        elif op == OP.SCHNAME_LOOKUP or op == OP.ENTITY_LOOKUP or op == OP.KW_LOOKUP:
            global global_origin_id
            hit = Hit(global_origin_id, params[0], params[0], -1)
            global_origin_id += 1
            self._p_graph.add_node(hit)
            for element in data:  # now we connect the new node to data with the op
                self._p_graph.add_node(element)
                self._p_graph.add_edge(hit, element, op)
        else:  # This all come with a Hit parameter
            hit = params[0]  # get the hit that comes with the op otherwise
            self._p_graph.add_node(hit)  # we add the param
            for element in data:  # now we connect the new node to data with the op
                self._p_graph.add_node(element)
                self._p_graph.add_edge(hit, element, op)

    def get_leafs_and_heads(self):
        # Compute leafs and heads
        # FIXME: cache this to avoid graph traversal every time
        leafs = []
        heads = []
        for node in self._p_graph.nodes():
            pre = self._p_graph.predecessors(node)
            suc = self._p_graph.successors(node)
            if len(pre) == 0:
                leafs.append(node)
            if len(suc) == 0:
                heads.append(node)
        return leafs, heads

    def compute_paths_from_origin_to(self, a: Hit, leafs=None, heads=None):
        if leafs is None and heads is None:
            leafs, heads = self.get_leafs_and_heads()
        all_paths = []
        for l in leafs:
            paths = nx.all_simple_paths(self._p_graph, l, a)
            all_paths.extend(paths)
        return all_paths

    def compute_all_paths(self, leafs=None, heads=None):
        if leafs is None and heads is None:
            leafs, heads = self.get_leafs_and_heads()
        all_paths = []
        for h in heads:
            paths = self.compute_paths_with(h)
            all_paths.extend(paths)
        return all_paths

    def compute_paths_with(self, a: Hit, leafs=None, heads=None):
        """
        Given a node, a, in the provenance graph, return all paths that contain it.
        :param a:
        :return:
        """
        # FIXME: refactor with compute_paths_from_origin and all that
        if leafs is None and heads is None:
            leafs, heads = self.get_leafs_and_heads()
        all_paths = []
        if a in leafs:
            for h in heads:
                paths = nx.all_simple_paths(self._p_graph, a, h)
                all_paths.extend(paths)
        elif a in heads:
            for l in leafs:
                paths = nx.all_simple_paths(self._p_graph, l, a)
                all_paths.extend(paths)
        else:
            upstreams = []
            for l in leafs:
                paths = nx.all_simple_paths(self._p_graph, l, a)
                upstreams.extend(paths)
            downstreams = []
            for h in heads:
                paths = nx.all_simple_paths(self._p_graph, a, h)
                downstreams.extend(paths)

            if len(downstreams) > len(upstreams):
                for d in downstreams:
                    for u in upstreams:
                        all_paths.append(u + d)
            else:
                for u in upstreams:
                    for d in downstreams:
                        all_paths.append(u + d)
        return all_paths

    def explain_path(self, p: [Hit]):
        """
        Given a path in the provenance graph, traverse it, checking the edges that connect nodes and forming
        a story that explains how p is a result.
        :param p:
        :return:
        """
        def get_name_from_hit(h: Hit):
            name = h.source_name + ":" + h.field_name
            return name

        def get_string_from_edge_info(edge_info):
            string = ""
            for k, v in edge_info.items():
                string = string + str(k) + " ,"
            return string

        explanation = ""

        slice_range = lambda a: a + 1  # pairs
        for idx in range(len(p)):
            if (idx + 1) < len(p):
                pair = p[idx::slice_range(idx)]
                src, trg = pair
                explanation = explanation + get_name_from_hit(src) + " -> "
                edge_info = self._p_graph[src][trg]
                explanation = explanation + get_string_from_edge_info(edge_info) + " -> " \
                    + get_name_from_hit(trg) + '\n'
        return explanation


class DRS:

    def __init__(self, data, operation):
        self._data = data
        self._provenance = Provenance(data, operation)
        self._table_view = []
        self._idx = 0
        self._idx_table = 0
        self._mode = DRSMode.FIELDS

    def __iter__(self):
        return self

    def __next__(self):
        # Iterating fields mode
        if self._mode == DRSMode.FIELDS:
            if self._idx < len(self._data):
                self._idx += 1
                return self._data[self._idx - 1]
            else:
                self._idx = 0
                raise StopIteration
        # Iterating in table mode
        elif self._mode == DRSMode.TABLE:
            #  Lazy initialization of table view
            if len(self._table_view) == 0:
                table_set = set()
                for h in self._data:
                    t = h.source_name
                    table_set.add(t)
                self._table_view = list(table_set)
            if self._idx_table < len(self._table_view):
                self._idx_table += 1
                return self._table_view[self._idx_table - 1]
            else:
                self._idx_table = 0
                raise StopIteration

    @property
    def data(self):
        return self._data

    def set_data(self, data):
        self._data = list(data)
        self._table_view = []
        self._idx = 0
        self._idx_table = 0
        self._mode = DRSMode.FIELDS
        return self

    @property
    def mode(self):
        return self._mode

    def get_provenance(self):
        return self._provenance

    def size(self):
        return len(self.data)

    def absorb_provenance(self, drs):
        """
        Merge provenance of the input parameter into self, *not* the data.
        :param drs:
        :return:
        """
        # Get prov graph of merging
        prov_graph_of_merging = drs.get_provenance().prov_graph()
        # Compose into my prov graph
        merge = nx.compose(self._provenance.prov_graph(),
                           prov_graph_of_merging)
        # Rewrite my prov graph to the new merged one and return
        self._provenance.swap_p_graph(merge)
        return self

    def absorb(self, drs):
        """
        Merge the input parameter DRS into self, by extending provenance appropriately and appending data
        :param drs:
        :return:
        """
        # Set union merge data
        merging_data = set(drs.data)
        my_data = set(self.data)
        new_data = merging_data.union(my_data)
        self.set_data(list(new_data))
        # Merge provenance
        self.absorb_provenance(drs)
        return self

    def intersection(self, drs):
        merging_data = set(drs.data)
        my_data = set(self.data)
        new_data = merging_data.intersection(my_data)
        self.set_data(list(new_data))
        # Merge provenance
        # FIXME: perhaps we need to do some garbage collection of the prov graph at some point
        # FIXME: or alternatively perform a more fine-grained merging
        self.absorb_provenance(drs)
        return self

    def union(self, drs):
        merging_data = set(drs.data)
        my_data = set(self.data)
        new_data = merging_data.union(my_data)
        self.set_data(list(new_data))
        # Merge provenance
        # FIXME: perhaps we need to do some garbage collection of the prov
        # graph at some point
        self.absorb_provenance(drs)
        return self

    def set_difference(self, drs):
        merging_data = set(drs.data)
        my_data = set(self.data)
        new_data = my_data - merging_data
        self.set_data(list(new_data))
        # Merge provenance
        # FIXME: perhaps we need to do some garbage collection of the prov
        # graph at some point
        self.absorb_provenance(drs)
        return self

    def set_fields_mode(self):
        self._mode = DRSMode.FIELDS

    def set_table_mode(self):
        self._mode = DRSMode.TABLE

    def paths(self):
        """
        Returns all paths contained in the provenance graph
        :return:
        """
        paths = self._provenance.compute_all_paths()
        return paths

    def path(self, a: Hit):
        """
        Return all paths that contain a
        :param a:
        :return:
        """
        paths = self._provenance.compute_paths_with(a)
        return paths

    def why_id(self, a: int) -> [Hit]:
        """
        Given the id of a Hit, explain what were the initial results that lead to this result appearing here
        :param a:
        :return:
        """
        hit = None
        for x in self._data:
            if x.nid == a:
                hit = x
        return self.why(hit)

    def why(self, a: Hit) -> [Hit]:
        """
        Given a result, explain what were the initial results that lead to this result appearing here
        :param a:
        :return:
        """
        # Make sure a is in data
        if a not in self.data:
            print("The result does not exist")
            return

        # Calculate paths from a to leafs, in reverse order and return the
        # leafs.
        paths = self._provenance.compute_paths_from_origin_to(a)
        origins = []
        for p in paths:
            origins.append(p[0])
        return origins

    def how_id(self, a: int) -> [Hit]:
        """
        Given the id of a Hit, explain what were the initial results that lead to this result appearing here
        :param a:
        :return:
        """
        hit = None
        for x in self._data:
            if x.nid == a:
                hit = x
        return self.how(hit)

    def how(self, a: Hit) -> [str]:
        """
        Given a result, explain how this result ended up forming part of the output
        :param a:
        :return:
        """
        # Make sure a is in data
        if a not in self.data:
            print("The result does not exist")
            return

        # Calculate paths from a to leafs, in reverse order and return the
        # leafs.
        paths = self._provenance.compute_paths_from_origin_to(a)
        explanations = []
        for p in paths:
            explanation = self._provenance.explain_path(p)
            explanations.append(explanation)
        return explanations

    """
    Convenience functions
    """

    def print_tables(self):
        mode = self.mode  # save state
        self.set_table_mode()
        for x in self:
            print(x)
        self._mode = mode  # recover state

    def print_columns(self):
        mode = self.mode  # save state
        self.set_fields_mode()
        for x in self:
            print(x)
        self._mode = mode  # recover state

    def pretty_print_columns(self):
        mode = self.mode  # save state
        self.set_fields_mode()
        for x in self:
            string = "SOURCE: " + x.source_name + "\t\t\t FIELD " + x.field_name
            print(string)
        self._mode = mode  # recover state


if __name__ == "__main__":

    test = DRS([1, 2, 3])

    for el in test:
        print(str(el))

    print(test.mode)
    test.set_table_mode()
    print(test.mode)
