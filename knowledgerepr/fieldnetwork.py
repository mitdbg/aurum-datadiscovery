import matplotlib.pyplot as plt
import operator
import networkx as nx
import os
from collections import defaultdict
from api.apiutils import DRS
from api.apiutils import Operation
from api.apiutils import OP
from api.apiutils import Hit
from api.apiutils import Relation
from api.apiutils import compute_field_id


def build_hit(sn, fn):
    nid = compute_field_id(sn, fn)
    return Hit(nid, sn, fn, -1)


class FieldNetwork:
    # The core graph
    __G = nx.MultiGraph()
    __id_names = dict()
    __source_ids = defaultdict(list)

    def __init__(self, graph=None, id_names=None, source_ids=None):
        if graph is None:
            self.__G = nx.MultiGraph()
        else:
            self.__G = graph
            self.__id_names = id_names
            self.__source_ids = source_ids

    def graph_order(self):
        return len(self.__id_names.keys())

    def get_number_tables(self):
        return len(self.__source_ids.keys())

    def iterate_ids(self):
        for k, _ in self.__id_names.items():
            yield k

    def iterate_ids_text(self):
        for k, v in self.__id_names.items():
            (db_name, source_name, field_name, data_type) = v
            if data_type == 'T':
                yield k

    def iterate_values(self) -> (str, str, str, str):
        for _, v in self.__id_names.items():
            yield v

    def get_fields_of_source(self, source) -> [int]:
        return self.__source_ids[source]

    def get_info_for(self, nids):
        info = []
        for nid in nids:
            db_name, source_name, field_name, data_type = self.__id_names[nid]
            info.append((nid, db_name, source_name, field_name))
        return info

    def get_hits_from_table(self, table) -> [Hit]:
        nids = self.get_fields_of_source(table)
        info = self.get_info_for(nids)
        hits = [Hit(nid, db_name, s_name, f_name, 0) for nid, db_name, s_name, f_name in info]
        return hits

    def get_cardinality_of(self, node_id):
        c = self.__G.node[node_id]
        card = c['cardinality']
        if card is None:
            return 0  # no cardinality is like card 0
        return card

    def _get_underlying_repr_graph(self):
        return self.__G

    def _get_underlying_repr_id_to_field_info(self):
        return self.__id_names

    def _get_underlying_repr_table_to_ids(self):
        return self.__source_ids

    def _visualize_graph(self):
        nx.draw(self.__G)
        plt.show()

    def init_meta_schema(self, fields: (int, str, str, str, int, int, str)):
        """
        Creates a dictionary of id -> (dbname, sourcename, fieldname)
        and one of:
        sourcename -> id
        Then it also initializes the graph with all the nodes, e.g., ids and the cardinality
        for these, if any.
        :param fields:
        :return:
        """
        print("Building schema relation...")
        for (nid, db_name, sn_name, fn_name, total_values, unique_values, data_type) in fields:
            self.__id_names[nid] = (db_name, sn_name, fn_name, data_type)
            self.__source_ids[sn_name].append(nid)
            cardinality_ratio = None
            if float(total_values) > 0:
                cardinality_ratio = float(unique_values) / float(total_values)
            self.add_field(nid, cardinality_ratio)
        print("Building schema relation...OK")

    def add_field(self, nid, cardinality=None):
        """
        Creates a graph node for this field and adds it to the graph
        :param nid: the id of the node (a hash of dbname, sourcename and fieldname
        :param cardinality: the cardinality of the values of the node, if any
        :return: the newly added field node
        """
        self.__G.add_node(nid, cardinality=cardinality)
        return nid

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

    def enumerate_relation(self, relation):
        for nid in self.iterate_ids():
            db_name, source_name, field_name, data_type = self.__id_names[nid]
            hit = Hit(nid, db_name, source_name, field_name, 0)
            neighbors = self.neighbors_id(hit, relation)
            for n2 in neighbors:
                string = str(hit) + " - " + str(n2)
                yield string

    def print_relations(self, relation):
        total_relationships = 0
        if relation == Relation.CONTENT_SIM:
            for x in self.enumerate_relation(Relation.CONTENT_SIM):
                total_relationships += 1
                print(x)
        if relation == Relation.SCHEMA_SIM:
            for x in self.enumerate_relation(Relation.SCHEMA):
                print(x)
        if relation == Relation.PKFK:
            for x in self.enumerate_relation(Relation.PKFK):
                print(x)
        print("Total " + str(relation) +
              " relations: " + str(total_relationships))

    def get_op_from_relation(self, relation):
        if relation == Relation.CONTENT_SIM:
            return OP.CONTENT_SIM
        if relation == Relation.ENTITY_SIM:
            return OP.ENTITY_SIM
        if relation == Relation.PKFK:
            return OP.PKFK
        if relation == Relation.SCHEMA:
            return OP.TABLE
        if relation == Relation.SCHEMA_SIM:
            return OP.SCHEMA_SIM

    def neighbors_id(self, hit: Hit, relation: Relation) -> DRS:
        if isinstance(hit, Hit):
            nid = hit.nid
        if isinstance(hit, str):
            nid = hit
        data = []
        neighbours = self.__G[nid]
        for k, v in neighbours.items():
            if str(k) == 'cardinality':  # FIXME: with the new way of setting attributes this should not be necessary
                continue  # skipping node attributes
            if relation in v:
                score = v[relation]['score']
                (db_name, source_name, field_name, data_type) = self.__id_names[k]
                data.append(Hit(k, db_name, source_name, field_name, score))
        op = self.get_op_from_relation(relation)
        o_drs = DRS(data, Operation(op, params=[hit]))
        return o_drs

    def _bidirectional_pred_succ(self, source, target, relation):
        """
        Bidirectional shortest path helper.
        :returns (pred,succ,w) where
        :param pred is a dictionary of predecessors from w to the source, and
        :param succ is a dictionary of successors from w to the target.
        """
        o_drs = DRS([], Operation(OP.NONE))  # Works as a carrier of provenance
        # does BFS from both source and target and meets in the middle
        if target == source:
            return {target: None}, {source: None}, source, o_drs

        # we always have an undirected graph
        Gpred = self.neighbors_id
        Gsucc = self.neighbors_id

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
                    successors = Gsucc(v, relation)
                    o_drs = o_drs.absorb(successors)  # Keep provenance
                    for w in successors:
                        if w not in pred:
                            forward_fringe.append(w)
                            pred[w] = v
                        if w in succ:
                            return pred, succ, w, o_drs  # found path
            else:
                this_level = reverse_fringe
                reverse_fringe = []
                for v in this_level:
                    predecessors = Gpred(v, relation)
                    o_drs = o_drs.absorb(predecessors)
                    for w in predecessors:
                        if w not in succ:
                            succ[w] = v
                            reverse_fringe.append(w)
                        if w in pred:
                            return pred, succ, w, o_drs  # found path
        return None

    def _bidirectional_pred_succ_with_table_hops(self, source, target, relation, api):
        """
        Bidirectional shortest path with table hops, i.e. two-relation exploration
        :returns (pred,succ,w) where
        :param pred is a dictionary of predecessors from w to the source, and
        :param succ is a dictionary of successors from w to the target.
        """
        def neighbors_with_table_hop(hit, rel) -> DRS:
            o_drs = DRS([], Operation(OP.NONE))

            hits = self.get_hits_from_table(hit.source_name)
            table_neighbors_drs = DRS([x for x in hits], Operation(OP.TABLE, params=[hit]))

            o_drs = o_drs.absorb_provenance(table_neighbors_drs)
            neighbors_with_table = set()
            for n in table_neighbors_drs:
                neighbors_of_n = self.neighbors_id(n, rel)
                o_drs = o_drs.absorb_provenance(neighbors_of_n)
                for match in neighbors_of_n:
                    neighbors_with_table.add(match)
            # just assign data here
            o_drs = o_drs.set_data(neighbors_with_table)
            return o_drs

        o_drs = DRS([], Operation(OP.NONE))  # Carrier of provenance

        # does BFS from both source and target and meets in the middle
        src_drs = api.drs_from_table(source)
        o_drs = o_drs.absorb(src_drs)
        trg_drs = api.drs_from_table(target)
        o_drs = o_drs.absorb(trg_drs)
        src_drs.set_table_mode()
        if target in src_drs:  # source and target are in the same table
            return {x: None for x in trg_drs}, {x: None for x in src_drs}, [x for x in src_drs], o_drs
        src_drs.set_fields_mode()
        trg_drs.set_fields_mode()

        # we always have an undirected graph
        Gpred = neighbors_with_table_hop
        Gsucc = neighbors_with_table_hop

        # predecessor and successors in search
        # pred = {source: None}
        # succ = {target: None}
        pred = {x: None for x in src_drs}
        succ = {x: None for x in trg_drs}

        # initialize fringes, start with forward
        forward_fringe = [x for x in src_drs]
        reverse_fringe = [x for x in trg_drs]

        while forward_fringe and reverse_fringe:
            if len(forward_fringe) <= len(reverse_fringe):
                this_level = forward_fringe
                forward_fringe = []
                for v in this_level:
                    successors = Gsucc(v, relation)
                    o_drs = o_drs.absorb(successors)
                    for w in successors:
                        if w not in pred:
                            forward_fringe.append(w)
                            pred[w] = v
                        if w in succ:
                            return pred, succ, w, o_drs  # found path
            else:
                this_level = reverse_fringe
                reverse_fringe = []
                for v in this_level:
                    predecessors = Gpred(v, relation)
                    o_drs = o_drs.absorb(predecessors)
                    for w in predecessors:
                        if w not in succ:
                            succ[w] = v
                            reverse_fringe.append(w)
                        if w in pred:
                            return pred, succ, w, o_drs  # found path
        return None

    def find_path_hit(self, source, target, relation):
        # source and target are Hit
        results = self._bidirectional_pred_succ(source, target, relation)
        if results is None:  # check for None result
            return DRS([], Operation(OP.NONE))
        pred, succ, w, o_drs = results

        return o_drs

    def find_path_table(self, source: str, target: str, relation, api):

        # source and target are strings with the table name
        results = self._bidirectional_pred_succ_with_table_hops(source, target, relation, api)
        if results is None:  # check for None result
            return DRS([], Operation(OP.NONE))
        pred, succ, w, o_drs = results

        return o_drs


def serialize_network(network, path):
    """
    Serialize the meta schema index
    :param network:
    :param path:
    :return:
    """
    G = network._get_underlying_repr_graph()
    id_to_field_info = network._get_underlying_repr_id_to_field_info()
    table_to_ids = network._get_underlying_repr_table_to_ids()

    # Make sure we create directory if this does not exist
    path = path + '/'  # force separator
    os.makedirs(os.path.dirname(path), exist_ok=True)

    nx.write_gpickle(G, path + "graph.pickle")
    nx.write_gpickle(id_to_field_info, path + "id_info.pickle")
    nx.write_gpickle(table_to_ids, path + "table_ids.pickle")


def deserialize_network(path):
    G = nx.read_gpickle(path + "graph.pickle")
    id_to_info = nx.read_gpickle(path + "id_info.pickle")
    table_to_ids = nx.read_gpickle(path + "table_ids.pickle")
    network = FieldNetwork(G, id_to_info, table_to_ids)
    return network


if __name__ == "__main__":
    print("Field Network")
