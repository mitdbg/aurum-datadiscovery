import time

from dataanalysis import dataanalysis as da

from knowledgerepr.fieldnetwork import FieldNetwork
from knowledgerepr.fieldnetwork import Relation
from nearpy import Engine
from nearpy.hashes import RandomBinaryProjections
from nearpy.distances import CosineDistance

from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
import numpy as np

from collections import defaultdict

rbp = RandomBinaryProjections('default', 25)


def create_sim_graph_text(network, text_engine, fields, tfidf, relation):
    # FIXME: bottleneck. parallelize this
    rowidx = 0
    for (nid, sn, fn) in fields:
        node1 = network.add_field(sn, fn)
        sparse_row = tfidf.getrow(rowidx)
        rowidx += 1
        dense_row = sparse_row.todense()
        array = dense_row.A[0]
        N = text_engine.neighbours(array)
        print(str(sn) + str(fn) + " simto: ")
        if len(N) > 1:
            print(" ")
            for (data, label, value) in N:
                print(str(label) + " value: " + str(value))
        if len(N) > 1:
            for n in N:
                (data, label, value) = n
                tokens = label.split('%&%&%')
                node2 = network.add_field(tokens[0], tokens[1])
                if node1 is not node2:
                    network.add_relation(node1, node2, relation, value)
        print("")


def index_in_text_engine(fields, tfidf):
    num_features = tfidf.shape[1]
    print("tfidf shape: " + str(tfidf.shape))
    text_engine = Engine(num_features,
                         lshashes=[rbp],
                         distance=CosineDistance())

    st = time.time()
    rowidx = 0
    for (nid, sn, fn) in fields:
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
    for (nid, sn_outer, fn_outer) in fields:
        n_outer = network.add_field(sn_outer, fn_outer)
        for(nid, sn, fn) in fields:
            if sn_outer == sn and fn_outer != fn:
                assert isinstance(network, FieldNetwork)
                n_inner = network.add_field(sn, fn)
                network.add_relation(n_outer, n_inner, Relation.SCHEMA, 1)


def build_schema_sim_relation(network, fields):
    docs = []
    for (nid, sn, fn) in fields:
        docs.append(fn)

    tfidf = da.get_tfidf_docs(docs)
    text_engine = index_in_text_engine(fields, tfidf)
    create_sim_graph_text(network, text_engine, fields, tfidf, Relation.SCHEMA_SIM)


def build_entity_sim_relation(network, fields, entities):
    docs = []
    for e in entities:
        docs.append(e)

    tfidf = da.get_tfidf_docs(docs)
    text_engine = index_in_text_engine(fields, tfidf)
    create_sim_graph_text(network, text_engine, fields, tfidf, Relation.ENTITY_SIM)


def build_content_sim_relation_text(network, fields, signatures):
    docs = []
    for e in signatures:
        docs.append(' '.join(e))

    tfidf = da.get_tfidf_docs(docs)  # this may become redundant if we exploit the store characteristics
    text_engine = index_in_text_engine(fields, tfidf)
    create_sim_graph_text(network, text_engine, fields, tfidf, Relation.CONTENT_SIM)


def build_content_sim_relation_num(network, fields, features):
    #X = StandardScaler().fit_transform(features)
    X = np.asarray(features)
    db = DBSCAN(eps=0.3, min_samples=2).fit(X)
    labels = db.labels_
    #print(str(labels))
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    print("Total num clusters found: " + str(n_clusters))
    # group indices by label
    clusters = defaultdict(list)
    for i in range(len(labels)):
        clusters[labels[i]].append(i)
    # create relations
    for k, v in clusters.items():
        #print("K: " + str(k))
        #print("V: " + str(v))
        if k == -1:
            continue
        for el1 in v:
            for el2 in v:
                if el1 != el2:
                    nid1, sn1, fn1 = fields[el1]
                    nid2, sn2, fn2 = fields[el2]
                    n1 = network.add_field(sn1, fn1)
                    n2 = network.add_field(sn2, fn2)
                    #print(str(n1) +" issimto "+ str(n2))
                    network.add_relation(n1, n2, Relation.CONTENT_SIM, 1)


def build_overlap_relation():
    print("todo")


def build_pkfk_relation():
    print('todo')


if __name__ == "__main__":
    print("TODO")

    # test
    from scipy import spatial
    import numpy

    d = [1, 2, 3]
    d2 = [3, 4, 5]

    result = 1 - spatial.distance.cosine(d, d2)
    print("result: " + str(result))
    result2 = 1.0 - numpy.dot(d, d2)
    print("result2: " + str(result))
