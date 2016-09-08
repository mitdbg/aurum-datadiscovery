import networkx as nx
from collections import defaultdict
from knowledgerepr.fieldnetwork import FieldNetwork
from api.apiutils import Relation


class Report:

    def __init__(self, network: FieldNetwork):
        self.__network = network
        self.__num_tables = 0
        self.__num_columns = 0
        self.__num_schema_sim_relations = 0
        self.__num_content_sim_relations = 0
        self.__num_pkfk_relations = 0
        self.compute_all_statistics()

    @property
    def num_tables(self):
        return self.__num_tables

    @property
    def num_columns(self):
        return self.__num_columns

    @property
    def num_schema_sim_relations(self):
        return self.__num_schema_sim_relations

    @property
    def num_content_sim_relations(self):
        return self.__num_content_sim_relations

    @property
    def num_pkfk_relations(self):
        return self.__num_pkfk_relations

    def get_number_tables(self, nodes):
        tables = defaultdict(list)
        # Separate fields per table
        for (nid, source_name, field_name, score) in nodes:
            tables[source_name].append(field_name)
        return len(tables.items())

    def top_connected_fields(self, topk):
        return self.__network.fields_degree(topk)

    def compute_all_statistics(self):
        graph = self.__network._get_underlying_repr()
        self.__num_columns = graph.order()
        nodes = graph.nodes()
        self.__num_tables = self.get_number_tables(nodes)
        # FIXME: Failing due to cardinality being attached as float to graph nodes ??
        # relations = graph.edges(keys=True)
        content_sim_relations_gen = self.__network.enumerate_relation(
            Relation.CONTENT_SIM)
        # FIXME: counting twice (both directions), so /2. Once edges works, we
        # can modify it
        total_content_sim_relations = len(
            [x for x in content_sim_relations_gen]) / 2
        self.__num_content_sim_relations = total_content_sim_relations

        schema_sim_relations_gen = self.__network.enumerate_relation(
            Relation.SCHEMA_SIM)
        total_schema_sim_relations = len(
            [x for x in schema_sim_relations_gen]) / 2
        self.__num_schema_sim_relations = total_schema_sim_relations

        pkfk_relations_gen = self.__network.enumerate_relation(Relation.PKFK)
        total_pkfk_relations = len([x for x in pkfk_relations_gen]) / 2
        self.__num_pkfk_relations = total_pkfk_relations

        return self

    def print_content_sim_relations(self):
        self.__network.print_relations(Relation.CONTENT_SIM)

    def print_schema_sim_relations(self):
        self.__network.print_relations(Relation.SCHEMA_SIM)

    def print_pkfk_relations(self):
        self.__network.print_relations(Relation.PKFK)
