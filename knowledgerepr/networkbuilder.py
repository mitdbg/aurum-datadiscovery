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


def create_sim_graph_text(network, text_engine, fields, tfidf, relation, tfidf_is_dense=False):
    # FIXME: bottleneck. parallelize this
    rowidx = 0
    for (nid, sn, fn) in fields:
        node1 = network.add_field(sn, fn)
        if tfidf_is_dense:
            dense_row = tfidf[rowidx]
            array = dense_row
        else:
            sparse_row = tfidf.getrow(rowidx)
            dense_row = sparse_row.todense()
            array = dense_row.A[0]
        rowidx += 1
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
                if node1.nid != node2.nid:
                    network.add_relation(node1, node2, relation, value)
        print("")


def index_in_text_engine(fields, tfidf, lsh_projections, tfidf_is_dense=False):
    num_features = tfidf.shape[1]
    print("tfidf shape: " + str(tfidf.shape))
    text_engine = Engine(num_features,
                         lshashes=[lsh_projections],
                         distance=CosineDistance())

    st = time.time()
    rowidx = 0
    for (nid, sn, fn) in fields:
        key = str(sn) + "%&%&%" + str(fn)
        if tfidf_is_dense:
            dense_row = tfidf[rowidx]
            array = dense_row
        else:
            sparse_row = tfidf.getrow(rowidx)
            dense_row = sparse_row.todense()
            array = dense_row.A[0]
        rowidx += 1
        text_engine.store_vector(array, key)
    et = time.time()
    print("total store text: " + str((et - st)))
    return text_engine


def lsa_dimensionality_reduction(tfidf):
    svd = TruncatedSVD(n_components=1000, random_state=42)
    svd.fit(tfidf)
    new_tfidf_vectors = svd.transform(tfidf)
    return new_tfidf_vectors


def build_schema_relation(network, fields):
    print("Building schema relation...")
    print("Putting fields in buckets...")
    tables = defaultdict(list)
    # Separate fields per table
    for (nid, sn_outer, fn_outer, tvals_outer, uvals_outer) in fields:
        if float(tvals_outer) > 0:
            card_outer = float(uvals_outer) / float(tvals_outer)
        # append tuple with (field_name, cardinality)
        tables[sn_outer].append((fn_outer, card_outer))
    print("Putting fields in buckets...OK")

    print("Filling schema relations for all tables...")
    total_tables = len(tables.keys())
    curr_table = 0
    # Connect fields of same table
    for table, table_fields in tables.items():
        curr_table += 1
        if curr_table % 500 == 0:
            print(str(curr_table) + "/" + str(total_tables))
        for f_out in table_fields:
            field_out, card_out = f_out
            for f_in in table_fields:
                field_in, card_in = f_in
                n_outer = network.add_field(table, field_out, card_out)
                n_inner = network.add_field(table, field_in, card_in)
                network.add_relation(n_outer, n_inner, Relation.SCHEMA, 1)
    print("Filling schema relations for all tables...OK")


def _build_schema_relation(network, fields):
    """
    tvals = total values
    uvals = unique values
    :param network:
    :param fields:
    :return:
    """
    total_fields = len(fields)
    curr_field = 0
    for (nid, sn_outer, fn_outer, tvals_outer, uvals_outer) in fields:
        if curr_field % 500 == 0:
            msg = str(curr_field) + "/" + str(total_fields)
            print(msg, end='')
            print('\r' * len(msg), end='')
            print(str(curr_field) + "/" + str(total_fields), end="")
        curr_field += 1
        card_outer = 0
        if float(tvals_outer) > 0:
            card_outer = float(uvals_outer) / float(tvals_outer)
        n_outer = network.add_field(sn_outer, fn_outer, card_outer)
        for(nid, sn, fn, tvals, uvals) in fields:
            if sn_outer == sn and fn_outer != fn:
                assert isinstance(network, FieldNetwork)
                card = 0
                if float(tvals) > 0:
                    card = float(uvals) / float(tvals)
                n_inner = network.add_field(sn, fn, card)
                network.add_relation(n_outer, n_inner, Relation.SCHEMA, 1)


def build_schema_sim_relation(network, fields):
    docs = []
    for (nid, sn, fn) in fields:
        docs.append(fn)

    tfidf = da.get_tfidf_docs(docs)
    text_engine = index_in_text_engine(
        fields, tfidf, rbp)  # rbp the global variable
    create_sim_graph_text(network, text_engine, fields,
                          tfidf, Relation.SCHEMA_SIM)


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


def build_content_sim_relation_text_lsa(network, fields, signatures):
    docs = []
    for e in signatures:
        docs.append(' '.join(e))

    # this may become redundant if we exploit the store characteristics
    tfidf = da.get_tfidf_docs(docs)

    print("tfidf shape before LSA: " + str(tfidf.shape))
    tfidf = lsa_dimensionality_reduction(tfidf)
    print("tfidf shape after LSA: " + str(tfidf.shape))
    # rbp = RandomBinaryProjections('default', 1000)
    lsh_projections = RandomDiscretizedProjections('rnddiscretized', 1000, 2)
    text_engine = index_in_text_engine(
        fields, tfidf, lsh_projections, tfidf_is_dense=True)
    create_sim_graph_text(network, text_engine, fields,
                          tfidf, Relation.CONTENT_SIM, tfidf_is_dense=True)


def build_content_sim_relation_text(network, fields, signatures):
    docs = []
    for e in signatures:
        docs.append(' '.join(e))

    # this may become redundant if we exploit the store characteristics
    tfidf = da.get_tfidf_docs(docs)
    # rbp = RandomBinaryProjections('default', 1000)
    lsh_projections = RandomDiscretizedProjections('rnddiscretized', 1000, 2)
    text_engine = index_in_text_engine(fields, tfidf, lsh_projections)
    create_sim_graph_text(network, text_engine, fields,
                          tfidf, Relation.CONTENT_SIM)


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


def build_pkfk_relation(network):
    seen = set()
    for n in network.enumerate_fields():
        seen.add(n)
        n_card = network.get_cardinality_of(n)
        neighborhood = network.neighbors(
            (n.source_name, n.field_name), Relation.CONTENT_SIM)
        for ne in neighborhood:
            if ne not in seen and ne is not n:
                ne_card = network.get_cardinality_of(ne)
                if n_card > 0.5 or ne_card > 0.5:
                    if n_card > ne_card:
                        highest_card = n_card
                    else:
                        highest_card = ne_card
                    network.add_relation(n, ne, Relation.PKFK, highest_card)
                    print(str(n) + " -> " + str(ne))


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
