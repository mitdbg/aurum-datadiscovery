from knowledgerepr import fieldnetwork
from ontomatch import glove_api
from nltk.corpus import stopwords
import numpy as np
import pickle
import itertools
import operator


np.seterr(all='raise')

def store_signatures(signatures, path):
    f = open(path + '/semantic_vectors.pkl', 'wb')
    pickle.dump(signatures, f)
    f.close()


def load_signatures(path):
    f = open(path + '/semantic_vectors.pkl', 'rb')
    semantic_vectors = pickle.load(f)
    f.close()
    return semantic_vectors


def read_table_columns(path_to_serialized_model):
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    source_ids = network._get_underlying_repr_table_to_ids()
    col_info = network._get_underlying_repr_id_to_field_info()
    cols = []
    for k, v in source_ids.items():
        for el in v:
            (db_name, sn_name, fn_name, data_type) = col_info[el]
            cols.append(fn_name)
            print(str(fn_name))
        yield (k, cols)
        cols.clear()


def generate_table_vectors(path_to_serialized_model):
    table_vectors = dict()

    for table_name, cols in read_table_columns(path_to_serialized_model):
        semantic_vectors = []
        for c in cols:
            tokens = c.split(' ')
            for token in tokens:
                if token not in stopwords.words('english'):
                    vec = glove_api.get_embedding_for_word(token.lower())
                    if vec is not None:
                        semantic_vectors.append(vec)
        print("Table: " + str(table_name) + " has: " + str(len(semantic_vectors)))
        table_vectors[table_name] = semantic_vectors
    return table_vectors


def compute_sv_coherency(sv):
    semantic_sim_array = []
    for a, b in itertools.combinations(sv, 2):
        sem_sim = glove_api.semantic_distance(a, b)
        semantic_sim_array.append(sem_sim)
    coh = 0
    if len(semantic_sim_array) > 1:  # if not empty slice
        coh = np.mean(semantic_sim_array)
    return coh


def compute_semantic_similarity_cross_average(sv1, sv2):
    global_sim = []
    for v1 in sv1:
        local_sim = []
        for v2 in sv2:
            sem_sim = glove_api.semantic_distance(v1, v2)
            local_sim.append(sem_sim)
        ls = 0
        if len(local_sim) > 1:
            ls = np.mean(local_sim)
        elif len(local_sim) == 1:
            ls = local_sim[0]
        global_sim.append(ls)
    gs = 0
    if len(global_sim) > 1:
        gs = np.mean(global_sim)
    elif len(global_sim) == 1:
        gs = global_sim[0]
    return gs


def compute_semantic_similarity_max_average(sv1, sv2):
    global_sim = []
    for v1 in sv1:
        local_sim = []
        for v2 in sv2:
            sem_sim = glove_api.semantic_distance(v1, v2)
            local_sim.append(sem_sim)
        if len(local_sim) > 0:
            ls = max(local_sim)
        else:
            continue
        global_sim.append(ls)
    gs = 0
    if len(global_sim) > 1:
        gs = np.mean(global_sim)
    elif len(global_sim) == 1:
        gs = global_sim[0]
    return gs


def compute_semantic_similarity_min_average(sv1, sv2):
    global_sim = []
    for v1 in sv1:
        local_sim = []
        for v2 in sv2:
            sem_sim = glove_api.semantic_distance(v1, v2)
            local_sim.append(sem_sim)
        if len(local_sim) > 0:
            ls = min(local_sim)
        else:
            continue
        global_sim.append(ls)
    gs = 0
    if len(global_sim) > 1:
        gs = np.mean(global_sim)
    elif len(global_sim) == 1:
        gs = global_sim[0]
    return gs


def compute_semantic_similarity_table(table, semantic_vectors):
    sv1 = semantic_vectors[table]

    results = dict()

    for k, v in semantic_vectors.items():
        if sv1 != k:
            avg_sim = compute_semantic_similarity_cross_average(sv1, v)
            max_sim = compute_semantic_similarity_max_average(sv1, v)
            min_sim = compute_semantic_similarity_min_average(sv1, v)
            results[k] = (avg_sim, max_sim, min_sim)
    return results


if __name__ == "__main__":

    path_to_serialized_model = "../models/dwh3/"

    """
    # Load glove model
    print("Loading glove model...")
    glove_api.load_model("../glove/glove.6B.100d.txt")
    print("Loading glove model...OK")

    table_vectors = generate_table_vectors(path_to_serialized_model)

    print("Storing semantic vectors...")
    store_signatures(table_vectors, ".")
    print("Storing semantic vectors...OK")
    """

    semantic_vectors = load_signatures(".")

    tables_coh = []

    for t, vecs in semantic_vectors.items():
        coh = compute_sv_coherency(vecs)
        tables_coh.append((coh, t))

    tables_coh = sorted(tables_coh, reverse=True)

    #for coh, t in tables_coh:
    #    print(str(t) + " -> " + str(coh))

    res = compute_semantic_similarity_table("Hr_org_unit.csv", semantic_vectors)

    only_cross_average = []
    only_max_average = []
    only_min_average = []

    for k, v in res.items():
        print(str(k) + " - " + str(v))
        only_cross_average.append((v[0], k))
        only_max_average.append((v[1], k))
        only_min_average.append((v[2], k))

    oca = sorted(only_cross_average, reverse=True)
    omx = sorted(only_max_average, reverse=True)
    omi = sorted(only_min_average, reverse=True)

    for i in range(len(oca)):
        print(oca[i])



