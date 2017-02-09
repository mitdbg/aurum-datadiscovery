from knowledgerepr import fieldnetwork
from ontomatch import glove_api
from nltk.corpus import stopwords
import numpy as np
import pickle
import itertools
import operator


def store_signatures(signatures, path):
    f = open(path + '/semantic_vectors.pkl', 'wb')
    pickle.dump(signatures, f)
    f.close()


def load_signatures(path):
    f = open(path + '/semantic_vectors.pkl', 'rb')
    semantic_vectors = pickle.load(f)
    f.close()
    return semantic_vectors


def read_table_columns(path_to_serialized_model, network=False):
    # If the network is not provided, then we use the path to deserialize from disk
    if not network:
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


def generate_table_vectors(path_to_serialized_model, network=False):
    table_vectors = dict()

    for table_name, cols in read_table_columns(path_to_serialized_model, network=network):
        semantic_vectors = []
        for c in cols:
            c = c.replace('_', ' ')
            tokens = c.split(' ')
            for token in tokens:
                token = token.lower()
                if token not in stopwords.words('english'):
                    vec = glove_api.get_embedding_for_word(token)
                    if vec is not None:
                        semantic_vectors.append(vec)
        print("Table: " + str(table_name) + " has: " + str(len(semantic_vectors)))
        table_vectors[table_name] = semantic_vectors
    return table_vectors


def get_semantic_vectors_for(tokens):
    s_vectors = []
    for t in tokens:
        vec = glove_api.get_embedding_for_word(t)
        s_vectors.append(vec)
    return s_vectors


def compute_internal_cohesion(sv):
    semantic_sim_array = []
    for a, b in itertools.combinations(sv, 2):
        sem_sim = glove_api.semantic_distance(a, b)
        semantic_sim_array.append(sem_sim)
    coh = 0
    if len(semantic_sim_array) > 1:  # if not empty slice
        coh = np.mean(semantic_sim_array)
    return coh


def compute_internal_cohesion_elementwise(x, sv):
    semantic_sim_array = []
    for el in sv:
        sem_sim = glove_api.semantic_distance(x, el)
        semantic_sim_array.append(sem_sim)
    coh = 0
    if len(semantic_sim_array) > 1:
        coh = np.mean(semantic_sim_array)
    return coh


def compute_sem_distance_with(x, sv):
    semantic_sim_array = []
    for el in sv:
        sem_sim = glove_api.semantic_distance(x, el)
        semantic_sim_array.append(sem_sim)
    ssim = 0
    if len(semantic_sim_array) > 1:
        ssim = np.mean(semantic_sim_array)
    return ssim


def compute_semantic_similarity(sv1, sv2):
    products = 0
    accum = 0
    for x in sv1:
        products += 1
        internal_cohesion = compute_internal_cohesion_elementwise(x, sv1)
        distance = compute_sem_distance_with(x, sv2)
        denominator = 2 * max(internal_cohesion, distance)
        value = internal_cohesion + distance / denominator
        accum += value
    ss = accum / products
    return ss


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


def compute_semantic_similarity_median(sv1, sv2):
    global_sim = []
    for v1 in sv1:
        local_sim = []
        for v2 in sv2:
            sem_sim = glove_api.semantic_distance(v1, v2)
            local_sim.append(sem_sim)
        ls = 0
        if len(local_sim) > 1:
            ls = np.median(local_sim)
        elif len(local_sim) == 1:
            ls = local_sim[0]
        global_sim.append(ls)
    gs = 0
    if len(global_sim) > 1:
        gs = np.median(global_sim)
    elif len(global_sim) == 1:
        gs = global_sim[0]
    return gs


def compute_semantic_similarity_table(table, semantic_vectors):
    sv1 = semantic_vectors[table]

    results = dict()

    for k, v in semantic_vectors.items():
        if sv1 != k:
            avg_sim = compute_semantic_similarity_cross_average(sv1, v)
            median_sim = compute_semantic_similarity_median(sv1, v)
            max_sim = compute_semantic_similarity_max_average(sv1, v)
            min_sim = compute_semantic_similarity_min_average(sv1, v)
            results[k] = (avg_sim, max_sim, min_sim, median_sim)
    return results


def compute_new_ss(table, semantic_vectors):
    sv1 = semantic_vectors[table]

    res = dict()

    for k, v in semantic_vectors.items():
        if sv1 != k:
            ss = compute_semantic_similarity(sv1, v)
            #print(str(k) + " -> " + str(ss))
            res[k] = ss
    return res

if __name__ == "__main__":

    path_to_serialized_model = "../models/chemical/"

    # Load glove model
    print("Loading glove model...")
    glove_api.load_model("../glove/glove.6B.100d.txt")
    print("Loading glove model...OK")

    # For the rest of operations, raise all errors
    np.seterr(all='raise')

    table_vectors = generate_table_vectors(path_to_serialized_model)

    print("Storing semantic vectors...")
    store_signatures(table_vectors, "data/chemical/")
    print("Storing semantic vectors...OK")

    semantic_vectors = load_signatures("data/chemical")

    """
    tables_coh = []

    for t, vecs in semantic_vectors.items():
        coh = compute_internal_cohesion(vecs)
        tables_coh.append((coh, t))

    tables_coh = sorted(tables_coh, reverse=True)

    for coh, t in tables_coh:
        print(str(t) + " -> " + str(coh))

    res = compute_semantic_similarity_table("Cambridge Home Page Featured Story_mfs6-yu9a.csv", semantic_vectors)

    only_cross_average = []
    only_max_average = []
    only_min_average = []
    only_median_average = []

    for k, v in res.items():
        print(str(k) + " - " + str(v))
        only_cross_average.append((v[0], k))
        only_max_average.append((v[1], k))
        only_min_average.append((v[2], k))
        only_median_average.append((v[3], k))

    oca = sorted(only_cross_average, reverse=True)
    omx = sorted(only_max_average, reverse=True)
    omi = sorted(only_min_average, reverse=True)
    oma = sorted(only_median_average, reverse=True)

    print("Average")
    for i in range(len(oca)):
        print(oca[i])

    print("")
    print("")
    print("")
    print("")

    print("Max")
    for i in range(len(oca)):
        print(oma[i])
    """

    # New metrics
    table = "assays"
    table_sim = compute_new_ss(table, semantic_vectors)

    table_sim = sorted(table_sim.items(), key=operator.itemgetter(1), reverse=True)
    for k, v in table_sim:
        print(str(k) + " -> " + str(v))







