from collections import namedtuple
from enum import Enum
import binascii
import networkx as nx

global_origin_id = 0

BaseHit = namedtuple('Hit', 'nid, source_name, field_name, score', verbose=False)


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
        else:  # This all come with a Hit parameter
            hit = params[0]  # get the hit that comes with the op otherwise
            self._p_graph.add_node(hit)  # we add the param
            for element in data:  # now we connect the new node to data with the op
                self._p_graph.add_node(element)
                self._p_graph.add_edge(hit, element, op)


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
        self._data = data
        self._table_view = []
        self._idx = 0
        self._idx_table = 0
        self._mode = DRSMode.FIELDS

    @property
    def mode(self):
        return self._mode

    def get_provenance(self):
        return self._provenance

    def size(self):
        return len(self.data)

    def invert_provenance(self):
        return

    def absorb_provenance(self, drs):
        """
        Merge provenance of the input parameter into self, *not* the data.
        :param drs:
        :return:
        """
        # Get prov graph of merging
        prov_graph_of_merging = drs.get_provenance().prov_graph()
        # Compose into my prov graph
        merge = nx.compose(self._provenance.prov_graph(), prov_graph_of_merging)
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
        # FIXME: perhaps we need to do some garbage collection of the prov graph at some point
        self.absorb_provenance(drs)
        return self

    def set_difference(self, drs):
        merging_data = set(drs.data)
        my_data = set(self.data)
        new_data = my_data - merging_data
        self.set_data(list(new_data))
        # Merge provenance
        # FIXME: perhaps we need to do some garbage collection of the prov graph at some point
        self.absorb_provenance(drs)
        return self

    def set_fields_mode(self):
        self._mode = DRSMode.FIELDS

    def set_table_mode(self):
        self._mode = DRSMode.TABLE

    def paths(self):
        return

    def path(self, a: str):
        return

    def why(self, a: str):
        return

    def how(self, a: str):
        return


if __name__ == "__main__":

    test = DRS([1, 2, 3])

    for el in test:
        print(str(el))

    print(test.mode)
    test.set_table_mode()
    print(test.mode)
