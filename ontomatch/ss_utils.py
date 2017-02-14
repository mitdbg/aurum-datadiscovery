from knowledgerepr import fieldnetwork
from ontomatch import glove_api
from nltk.corpus import stopwords
import numpy as np
import pickle
import itertools
import operator
from ontomatch import javarandom
from dataanalysis import nlp_utils as nlp
from nltk.corpus import stopwords

# minhash variables
k = 512
mersenne_prime = 536870911
rnd_seed = 1
rnd = javarandom.Random(rnd_seed)
random_seeds = []
a = np.int64()
for i in range(k):
    randoms = [rnd.nextLong(), rnd.nextLong()]
    random_seeds.append(randoms)


def minhash(str_values):

    def java_long(number):
        return (number + 2**63) % 2**64 - 2**63

    def remainder(a, b):
        return a - (b * int(a/b))

    def hash_this(value):
        h = mersenne_prime
        length = len(value)
        for i in range(length):
            h = 31 * h + ord(value[i])
        return h

    mh = [9223372036854775807 for i in range(k)]

    for v in str_values:
        v = nlp.camelcase_to_snakecase(v)
        v = v.replace('_', ' ')
        v = v.replace('-', ' ')
        v = v.lower()
        for token in v.split(' '):
            if token not in stopwords.words('english'):
                raw_hash = hash_this(token)
                for i in range(k):
                    first_part = java_long(random_seeds[i][0] * raw_hash)
                    second_part = java_long(random_seeds[i][1])
                    nomodule = java_long(first_part + second_part)
                    h = java_long(remainder(nomodule, mersenne_prime))
                    if h < mh[i]:
                        mh[i] = h
    return mh


def extract_cohesive_groups(table_name, attrs):

    def maybe_add_new_set(groups, current):
        # Right now, just filter duplicate sets
        # TODO: should we filter subsumed sets to keep only the largest ones?
        score, current_set = current
        for score, set_attrs in groups:
            if len(current_set) == len(set_attrs) and len(current_set - set_attrs) == 0:
                return  # if repeated, then just return without adding
            len_a = len(current_set)
            len_b = len(set_attrs)
            if len_a > len_b:
                if len(set_attrs - current_set) == 0:
                    return
            else:
                if len((current_set - set_attrs)) == 0:
                    return
        groups.append(current)  # otherwise add and finish

    groups = []
    tokens = set()
    ctb = nlp.curate_string(table_name)
    tokens |= set(ctb.split(' '))
    for attr in attrs:
        cattr = nlp.curate_string(attr)
        tokens |= set(cattr.split(' '))
    tokens = [t for t in tokens if t not in stopwords.words('english') and len(t) > 1]
    for anchor in tokens:
        current = (0.5, set())  # keeps (score, []) cohesiveness score and list of attrs that honor it
        for t in tokens:
            if anchor == t:  # not interested in self-comparison
                continue
            anchor_v = glove_api.get_embedding_for_word(anchor)
            t_v = glove_api.get_embedding_for_word(t)
            if anchor_v is not None and t_v is not None:
                ss = glove_api.semantic_distance(anchor_v, t_v)
                if ss > current[0]:
                    new_set = current[1]
                    new_set.add(anchor)
                    new_set.add(t)
                    current = (ss, new_set)
        if len(current[1]) > 0:
            maybe_add_new_set(groups, current)
    return groups


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
    # for table_name, field_ids in ...
    for k, v in source_ids.items():
        db_name = None
        for el in v:
            (db_name, sn_name, fn_name, data_type) = col_info[el]
            cols.append(fn_name)
        yield (db_name, k, cols)
        cols.clear()


def generate_table_vectors(path_to_serialized_model, network=False):
    table_vectors = dict()

    for db_name, table_name, cols in read_table_columns(path_to_serialized_model, network=network):
        semantic_vectors = []
        seen_tokens = []
        for c in cols:
            c = c.replace('_', ' ')
            tokens = c.split(' ')
            for token in tokens:
                token = token.lower()
                if token not in stopwords.words('english'):
                    if token not in seen_tokens:
                        seen_tokens.append(token)
                        vec = glove_api.get_embedding_for_word(token)
                        if vec is not None:
                            semantic_vectors.append(vec)
        print("Table: " + str(table_name) + " has: " + str(len(semantic_vectors)))
        table_vectors[(db_name, table_name)] = semantic_vectors
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
        if x is not None and el is not None:
            sem_sim = glove_api.semantic_distance(x, el)
            semantic_sim_array.append(sem_sim)
    coh = 0
    if len(semantic_sim_array) > 1:
        coh = np.mean(semantic_sim_array)
    return coh


def compute_sem_distance_with(x, sv):
    semantic_sim_array = []
    for el in sv:
        if x is not None and el is not None:
            sem_sim = glove_api.semantic_distance(x, el)
            semantic_sim_array.append(sem_sim)
    ssim = 0
    if len(semantic_sim_array) > 1:
        ssim = np.mean(semantic_sim_array)
    return ssim


def compute_semantic_similarity(sv1, sv2):
    accum = []
    for a, b in itertools.product(sv1, sv2):
        if a is not None and b is not None:
            sim = glove_api.semantic_distance(a, b)
            accum.append(sim)
    sim = 0
    if len(accum) > 0:
        sim = np.mean(accum)
    return sim


def __compute_semantic_similarity(sv1, sv2):
    products = 0
    accum = 0
    for x in sv1:
        products += 1
        internal_cohesion = compute_internal_cohesion_elementwise(x, sv1)
        distance = compute_sem_distance_with(x, sv2)
        denominator = 2 * max(internal_cohesion, distance)
        if (internal_cohesion + distance) < 0:
            value = 0
        else:
            if denominator > 0:
                value = internal_cohesion + distance / denominator
            else:
                value = 0
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
        if table != k:
            if k == "molecule_hierarchy":
                a = 1
            ss = compute_semantic_similarity(sv1, v)
            #print(str(k) + " -> " + str(ss))
            res[k] = ss
    return res


def test(path_to_serialized_model):
    # Load glove model
    print("Loading language model...")
    path_to_glove_model = "../glove/glove.6B.100d.txt"
    glove_api.load_model(path_to_glove_model)
    print("Loading language model...OK")

    total_tables = 0
    avg_attrs_per_table = 0
    avg_groups_per_table = 0
    for db, t, attrs in read_table_columns(path_to_serialized_model):
        total_tables += 1
        groups = extract_cohesive_groups(t, attrs)
        avg_attrs_per_table += len(attrs)
        avg_groups_per_table += len(groups)
        print("Table: " + str(t))
        print("num groups: " + str(len(groups)))
        for score, tokens in groups:
            print("Score: " + str(score))
            print(tokens)
            print("#####")
    avg_attrs_per_table = avg_attrs_per_table / total_tables
    avg_groups_per_table = avg_groups_per_table / total_tables
    print("Avg attr per table: " + str(avg_attrs_per_table))
    print("Avg group per table: " + str(avg_groups_per_table))

if __name__ == "__main__":

    path_to_serialized_model = "../models/massdata/"

    test(path_to_serialized_model)
    exit()


    """
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
    """

    values = ["test", "test1", "torpedo", "raiz", "agua", "water"]

    print("MP: " + str(mersenne_prime))
    for el in random_seeds:
        print("SEED0: " + str(el[0]))
        print("SEED1: " + str(el[1]))

    mh = minhash(values)
    print(mh)
    exit()


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
    table = "parameter_type"
    table_sim = compute_new_ss(table, semantic_vectors)

    table_sim = sorted(table_sim.items(), key=operator.itemgetter(1), reverse=True)
    for k, v in table_sim:
        print(str(k) + " -> " + str(v))







