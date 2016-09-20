import time

from dataanalysis import dataanalysis as da

from knowledgerepr.fieldnetwork import FieldNetwork
from knowledgerepr.fieldnetwork import Relation
from nearpy import Engine
from nearpy.hashes import RandomBinaryProjections
from nearpy.hashes import RandomDiscretizedProjections
from nearpy.distances import CosineDistance
from sklearn.decomposition import TruncatedSVD

from sklearn.cluster import DBSCAN
import numpy as np

from collections import defaultdict

rbp = RandomBinaryProjections('default', 30)


def create_sim_graph_text(nid_gen, network, text_engine, tfidf, relation, tfidf_is_dense=False):
    st = time.time()
    row_idx = 0
    for nid in nid_gen:
        if tfidf_is_dense:
            dense_row = tfidf[row_idx]
            array = dense_row
        else:
            sparse_row = tfidf.getrow(row_idx)
            dense_row = sparse_row.todense()
            array = dense_row.A[0]
        row_idx += 1
        N = text_engine.neighbours(array)
        if len(N) > 1:
            for n in N:
                (data, key, value) = n
                if nid != key:
                    print("tsim: {0} <-> {1}".format(nid, key))
                    network.add_relation(nid, key, relation, value)
    et = time.time()
    print("Create graph schema: {0}".format(str(et - st)))


def index_in_text_engine(nid_gen, tfidf, lsh_projections, tfidf_is_dense=False):
    num_features = tfidf.shape[1]
    print("TF-IDF shape: " + str(tfidf.shape))
    text_engine = Engine(num_features,
                         lshashes=[lsh_projections],
                         distance=CosineDistance())

    st = time.time()
    row_idx = 0
    for key in nid_gen:
        if tfidf_is_dense:
            dense_row = tfidf[row_idx]
            array = dense_row
        else:
            sparse_row = tfidf.getrow(row_idx)
            dense_row = sparse_row.todense()
            array = dense_row.A[0]
        row_idx += 1
        text_engine.store_vector(array, key)
    et = time.time()
    print("Total index text: " + str((et - st)))
    return text_engine


def lsa_dimensionality_reduction(tfidf):
    svd = TruncatedSVD(n_components=1000, random_state=42)
    svd.fit(tfidf)
    new_tfidf_vectors = svd.transform(tfidf)
    return new_tfidf_vectors


def build_schema_sim_relation(network):
    st = time.time()
    docs = []
    for (_, _, field_name, _) in network.iterate_values():
        docs.append(field_name)

    tfidf = da.get_tfidf_docs(docs)
    et = time.time()
    print("Time to create docs and TF-IDF: ")
    print("Create docs and TF-IDF: {0}".format(str(et - st)))
    nid_gen = network.iterate_ids()
    text_engine = index_in_text_engine(nid_gen, tfidf, rbp)  # rbp the global variable
    nid_gen = network.iterate_ids()
    create_sim_graph_text(nid_gen, network, text_engine, tfidf, Relation.SCHEMA_SIM)


def build_schema_sim_relation_lsa(network, fields):
    docs = []
    for (nid, sn, fn, _, _) in fields:
        docs.append(fn)

    tfidf = da.get_tfidf_docs(docs)

    print("tfidf shape before LSA: " + str(tfidf.shape))
    tfidf = lsa_dimensionality_reduction(tfidf)
    print("tfidf shape after LSA: " + str(tfidf.shape))

    text_engine = index_in_text_engine(
        fields, tfidf, rbp, tfidf_is_dense=True)  # rbp the global variable
    create_sim_graph_text(network, text_engine, fields,
                          tfidf, Relation.SCHEMA_SIM, tfidf_is_dense=True)


def build_entity_sim_relation(network, fields, entities):
    docs = []
    for e in entities:
        if e != "":  # Append only non-empty documents
            docs.append(e)
    print(str(docs))

    if len(docs) > 0:  # If documents are empty, then skip this step; not entity similarity will be found
        tfidf = da.get_tfidf_docs(docs)
        text_engine = index_in_text_engine(
            fields, tfidf, rbp)  # rbp the global variable
        create_sim_graph_text(network, text_engine, fields,
                              tfidf, Relation.ENTITY_SIM)


def build_content_sim_relation_text_lsa(network, signatures):

    def get_nid_gen(signatures):
        for nid, sig in signatures:
            yield nid

    docs = []
    for nid, e in signatures:
        docs.append(' '.join(e))

    # this may become redundant if we exploit the store characteristics
    tfidf = da.get_tfidf_docs(docs)

    print("TF-IDF shape before LSA: " + str(tfidf.shape))
    st = time.time()
    tfidf = lsa_dimensionality_reduction(tfidf)
    et = time.time()
    print("TF-IDF shape after LSA: " + str(tfidf.shape))
    print("Time to compute LSA: {0}".format(str(et - st)))
    lsh_projections = RandomBinaryProjections('default', 10000)
    #lsh_projections = RandomDiscretizedProjections('rnddiscretized', 1000, 2)
    nid_gen = get_nid_gen(signatures)  # to preserve the order nid -> signature
    text_engine = index_in_text_engine(nid_gen, tfidf, lsh_projections, tfidf_is_dense=True)
    nid_gen = get_nid_gen(signatures)  # to preserve the order nid -> signature
    create_sim_graph_text(nid_gen, network, text_engine, tfidf, Relation.CONTENT_SIM, tfidf_is_dense=True)


def build_content_sim_relation_text(network, fields, signatures):

    def get_nid_gen(signatures):
        for nid, sig in signatures:
            yield nid

    docs = []
    for nid, e in signatures:
        docs.append(' '.join(e))

    # this may become redundant if we exploit the store characteristics
    tfidf = da.get_tfidf_docs(docs)
    # rbp = RandomBinaryProjections('default', 1000)
    lsh_projections = RandomDiscretizedProjections('rnddiscretized', 1000, 2)
    nid_gen = get_nid_gen(signatures)
    text_engine = index_in_text_engine(nid_gen, tfidf, lsh_projections)
    nid_gen = get_nid_gen(signatures)
    create_sim_graph_text(nid_gen, network, text_engine, tfidf, Relation.CONTENT_SIM)


def build_content_sim_relation_num(network, id_sig):

    def get_sig_gen(id_sig):
        for nid, sig in id_sig:
            yield sig

    features_gen = get_sig_gen(id_sig)
    fields = [x[0] for x in id_sig]

    X = np.asarray([x for x in features_gen])
    db = DBSCAN(eps=0.3, min_samples=2).fit(X)
    labels = db.labels_
    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    print("Total num clusters found: " + str(n_clusters))
    # group indices by label
    clusters = defaultdict(list)
    for i in range(len(labels)):
        clusters[labels[i]].append(i)
    # create relations
    for k, v in clusters.items():
        if k == -1:
            continue
        for el1 in v:
            for el2 in v:
                if el1 != el2:
                    nid1 = fields[el1]
                    nid2 = fields[el2]
                    network.add_relation(nid1, nid2, Relation.CONTENT_SIM, 1)


def build_pkfk_relation(network):
    total_pkfk_relations = 0
    for n in network.iterate_ids():
        n_card = network.get_cardinality_of(n)
        if n_card > 0.5:  # Early check if this is a candidate
            neighborhood = network.neighbors_id(n, Relation.CONTENT_SIM)
            for ne in neighborhood:
                if ne is not n:
                    ne_card = network.get_cardinality_of(ne.nid)
                    if n_card > ne_card:
                        highest_card = n_card
                    else:
                        highest_card = ne_card
                    network.add_relation(n, ne.nid, Relation.PKFK, highest_card)
                    total_pkfk_relations += 1
                    #print(str(n) + " -> " + str(ne))
    print("Total number PKFK: {0}".format(str(total_pkfk_relations)))


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
