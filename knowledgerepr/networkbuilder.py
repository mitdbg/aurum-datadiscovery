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


def build_content_sim_relation_num_overlap_distr(network, id_sig):

    overlap = 0.7

    fields = []
    domains = []
    stats = []
    for c_k, (c_median, c_iqr, c_min_v, c_max_v) in id_sig:
        fields.append(c_k)
        domain = (c_median + c_iqr) - (c_median - c_iqr)
        domains.append(domain)
        extreme_left = c_median - c_iqr
        extreme_right = c_median + c_iqr
        stats.append((extreme_left, extreme_right))

    fields = [x for (y, x) in sorted(zip(domains, fields), reverse=True)]  # order fields according to domains
    stats = [x for (y, x) in sorted(zip(domains, stats), reverse=True)]    # order stats  according to domains too

    domains = sorted(domains, reverse=True)  # order domains

    candidate_entries = []
    for i in range(len(domains)):
        candidate_entries.append((domains[i], stats[i][0], stats[i][1]))


    entries = dict()
    entries[candidate_entries[0]] = []

    for candidate in candidate_entries:
        candidate_domain, candidate_x_left, candidate_x_right = candidate
        for entry in entries.keys():
            ref_domain, ref_x_left, ref_x_right = entry
            if  candidate_domain / ref_domain <= overlap:
                continue  # early stop, not even the entire domain would overlap the necessary amount
            else:
                if candidate_x_left > ref_x_left and candidate_x_right < ref_x_right:
                    if candidate_domain / ref_domain >= overlap:
                        True
                if candidate_x_left > ref_x_left:
                    total_overlap = ref_x_right - candidate_x_left
                    if total_overlap / candidate_domain >= overlap:
                        True




    for entry in entries:
        for (ref_domain, ref_extreme_left, ref_extreme_right) in candidate:
            continue





def build_content_sim_relation_num_overlap_distr_old(network, id_sig):

    # populate vectors
    entries_initial = dict()
    total = 0
    for c_k, (c_median, c_iqr, c_min_v, c_max_v) in id_sig:
        total += 1
        appended = False
        for k, v in entries_initial.items():
            (key, median, iqr, min_v, max_v) = k
            c_extreme_left = c_median - c_iqr
            c_extreme_right = c_median + c_iqr
            ref_extreme_left = median - iqr
            ref_extreme_right = median + iqr

            if c_median < ref_extreme_left or c_median > ref_extreme_right:  ## median has to fall in the range
                continue

            if c_extreme_left < 0 or c_extreme_right < 0 or ref_extreme_right < 0 or ref_extreme_left < 0:
                continue
            """
            check = False
            if c_median > median:
                if (c_median - median) > c_median + iqr:
                    check = True
            if c_median <= median:
                if (median - c_median) > c_median + iqr:
                    check = True
            if not check:
                continue
            """

            if c_extreme_left >= ref_extreme_left\
                and c_extreme_left <= ref_extreme_right\
                or c_extreme_right >= ref_extreme_left\
                and c_extreme_right <= ref_extreme_right:
                #if c_median - c_iqr >= median - iqr or c_median + c_iqr <= median + iqr:
                    entries_initial[k].append((c_k, c_median, c_iqr, c_min_v, c_max_v))
                    appended = True
        if not appended:
            entries_initial[(c_k, c_median, c_iqr, c_min_v, c_max_v)] = [(c_k, c_median, c_iqr, c_min_v, c_max_v)]  # I include myself for convenience

    print("Total entries: " + str(len(entries_initial.keys())))

    for k, v in entries_initial.items():
        (nid, median, iqr, min_v, max_v) = k
        info = network.get_info_for([nid])
        (nid, db_name, source_name, field_name) = info[0]
        print("SUBSUBMED BY: " + source_name + " - " + field_name + " median: " + str(median))
        print("")
        for (nid, median, iqr, min_v, max_v) in v:
            info = network.get_info_for([nid])
            (nid, db_name, source_name, field_name) = info[0]
            print(source_name + " - " + field_name + " median: " + str(median) + " iqr: " + str(iqr))
        print("")



def build_content_sim_relation_num_double_clustering(network, id_sig):

    fields = []
    median_vector = []
    iqr_vector = []

    # populate vectors
    total = 0
    for k, (c_median, c_iqr, c_min_v, c_max_v) in id_sig:
        total += 1
        fields.append(k)
        median_vector.append(c_median)
        iqr_vector.append(c_iqr)

    print("Total samples: " + str(total))

    #median_vector = median_vector.reshape(-1, 1)
    #iqr_vector = iqr_vector.reshape(-1, 1)

    x_median = np.asarray(median_vector)
    x_iqr = np.asarray(iqr_vector)
    x_median = x_median.reshape(-1, 1)
    x_iqr = x_iqr.reshape(-1, 1)

    db_median = DBSCAN(eps=0.1, min_samples=3).fit(x_median)
    db_iqr = DBSCAN(eps=0.1, min_samples=3).fit(x_iqr)
    labels_median = db_median.labels_
    labels_iqr = db_iqr.labels_
    n_clusters_median = len(set(labels_median)) - (1 if -1 in labels_median else 0)
    n_clusters_iqr = len(set(labels_iqr)) - (1 if -1 in labels_iqr else 0)
    print("Num clusters median: " + str(n_clusters_median))
    print("Num clusters iqr: " + str(n_clusters_iqr))

    clusters_median = defaultdict(list)
    for i in range(len(labels_median)):
        clusters_median[labels_median[i]].append(i)

    clusters_iqr = defaultdict(list)
    for i in range(len(labels_iqr)):
        clusters_iqr[labels_iqr[i]].append(i)

    print("Clusters median")
    print("")
    for k, v in clusters_median.items():
        if k == -1:
            continue
        print("cluster: " + str(k))
        for el in v:
            nid = fields[el]
            info = network.get_info_for([nid])
            (nid, db_name, source_name, field_name) = info[0]
            print(source_name + " - " + field_name + " median: " + str(median_vector[el]))
        print("")
        print("")

    print("Clusters IQR")
    print("")
    for k, v in clusters_iqr.items():
        if k == -1:
            continue
        print("cluster: " + str(k))
        for el in v:
            nid = fields[el]
            info = network.get_info_for([nid])
            (nid, db_name, source_name, field_name) = info[0]
            print(source_name + " - " + field_name + " iqr: " + str(iqr_vector[el]))
        print("")
        print("")

    """
    for k, (c_median, c_iqr, c_min_v, c_max_v) in id_sig:
        info = network.get_info_for([k])
        (nid, db_name, source_name, field_name) = info[0]
        print("{0}-{1} has: {2}-{3}-{4}, iqr: {5}".format(source_name, field_name, c_min_v, c_median, c_max_v, c_iqr))
        # Check whether the new entry fits in one of the existing entries
        for k, v in entries.items():
            (min, median_low, median, median_upper, max) = k
            if c_min_v > min or c_max_v <= max:
                continue
    """


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
