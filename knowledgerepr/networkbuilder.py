import time

from dataanalysis import dataanalysis as da

from knowledgerepr.fieldnetwork import FieldNetwork
from knowledgerepr.fieldnetwork import Relation
from nearpy import Engine
from nearpy.hashes import RandomBinaryProjections
from nearpy.distances import CosineDistance

rbp = RandomBinaryProjections('rbp', 30)


def create_sim_graph_text(network, text_engine, fields, tfidf, relation):
    # FIXME: bottleneck. parallelize this
    rowidx = 0
    for (sn, fn) in fields:
        node1 = network.add_field(sn, fn)
        sparse_row = tfidf.getrow(rowidx)
        rowidx += 1
        dense_row = sparse_row.todense()
        array = dense_row.A[0]
        N = text_engine.neighbours(array)
        for n in N:
            (data, label, value) = n
            tokens = label.split('%&%&%')
            node2 = network.add_field(tokens[0], tokens[1])
            network.add_relation(node1, node2, relation, value)


def index_in_text_engine(fields, tfidf):
    num_features = tfidf.shape[1]
    print("tfidf shape: " + str(tfidf.shape))
    text_engine = Engine(num_features,
                         lshashes=[rbp],
                         distance=CosineDistance())

    st = time.time()
    rowidx = 0
    for (sn, fn) in fields:
        key = str(sn) + "%&%&%" + str(fn)
        sparse_row = tfidf.getrow(rowidx)
        rowidx += 1
        dense_row = sparse_row.todense()
        array = dense_row.A[0]
        text_engine.store_vector(array, key)
    et = time.time()
    print("total store text: " + str((et - st)))
    return text_engine


def build_schema_relation(network, fields):
    for (sn_outer, fn_outer) in fields:
        n_outer = network.add_field(sn_outer, fn_outer)
        for(sn, fn) in fields:
            if sn_outer == sn and fn_outer != fn:
                assert isinstance(network, FieldNetwork)
                n_inner = network.add_field(sn, fn)
                network.add_relation(n_outer, n_inner, Relation.SCHEMA)


def build_schema_sim_relation(network, fields):
    docs = []
    for (sn, fn) in fields:
        docs.append(fn)

    tfidf = da.get_tfidf_docs(docs)
    text_engine = index_in_text_engine(fields, tfidf)
    create_sim_graph_text(network, text_engine, fields, tfidf, Relation.SCHEMA_SIM)


def build_entity_sim_relation(network, fields, entities):
    docs = []
    for e in entities:
        docs.append(' '.join(e))

    tfidf = da.get_tfidf_docs(docs)
    text_engine = index_in_text_engine(fields, tfidf)
    create_sim_graph_text(network, text_engine, fields, tfidf, Relation.ENTITY_SIM)


def build_content_sim_relation():
    print('todo')


def build_overlap_relation():
    print("todo")


def build_pkfk_relation():
    print('todo')


if __name__ == "__main__":
    print("TODO")
