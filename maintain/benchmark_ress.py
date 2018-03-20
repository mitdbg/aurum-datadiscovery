import random
import numpy as np
from datasketch import MinHash


def generate_data(num_columns):
    #data = [[i for i in range(100)] for _ in range(num_columns)]
    data = [[random.randint(0, 100) for i in range(100)] for _ in range(num_columns)]
    return data


def perturb_data_uniform(data, percentage):
    data_size = len(data)
    size_of_mutation = round(percentage * data_size)
    for i in range(round(data_size/2)):
        sample_to_mutate = random.sample(range(0, 99), size_of_mutation)
        for el in sample_to_mutate:
            data[i][el] = -1  # mutate
    return data


def get_random_sample(values_set, perc):
    original_size = len(values_set)
    # if original_size == 0:
    #     return np.asarray([])
    sample_size = int(original_size * perc)
    # if sample_size == 0:  # in that case sampling yield 0
    #     return np.asarray(list(values_set))
    if sample_size == 0:
        sample_size = 1
    #values_set = list(values_set)
    #values_np = np.asarray(list(values_set))
    #values_np = np.asarray(values_set)
    sample = np.random.choice(values_set, sample_size, replace=False)
    return sample


def get_mh(values, permutations=512):
    mh = MinHash(num_perm=permutations)
    for el in values:
        mh.update(str(el).encode('utf8'))
    return mh


def col2col_ress(dataset1, dataset2, sample_perc, store_path):
    with open(store_path, 'w') as fw:

        found = 0
        correct = 0

        for i in range(len(dataset1)):  # dataset1 and 2 have the same number of columns

            val1 = dataset1[i]
            val2 = dataset2[i]

            val2 = get_random_sample(val2, sample_perc)
            #mh1 = get_mh(val1)
            mh1 = get_mh(val1)
            mh2 = get_mh(val2)
            card1 = len(set(val1))
            card2 = len(set(val2))
            if card1 == 0:
                max_js = 0
            else:
                max_js = card2 / card1
            est_js = mh1.jaccard(mh2)
            if max_js == 0:
                scaled_est_js = 0
            else:
                scaled_est_js = est_js / max_js

            magnitude_change = 1 - scaled_est_js

            if magnitude_change > 0.05:
                # print("col: " + str(i) + " changed!")
                found += 1
                if i < 50:
                    correct += 1

        if found > 0:
            precision = correct / found
        else:
            precision = 0
        recall = correct / 50
        false_positive_ratio = (found - correct) / 50
        if precision == 0 and recall == 0:
            fscore = 0
        else:
            fscore = 2 * ((precision * recall) / (precision + recall))

        return precision, recall, fscore, false_positive_ratio

        # s = str(f)+"."+str(c)+","+str(max_js)+","+str(est_js)+","+str(scaled_est_js)+'\n'
        # fw.write(s)
        # print("EST-JS: " + str(scaled_est_js))


def testbed(output_data):

    # create data
    original_data = generate_data(100)

    with open(output_data, "w") as fil:
        fil.write("perturbation_factor, sample_size, precision, recall, fscore, falsepositive_ratio" + '\n')
        for perturbation_factor, sample_size in [(0.99, 0.7), (0.9, 0.7), (0.8, 0.7), (0.7, 0.7), (0.6, 0.7),
                                                 (0.5, 0.7), (0.4, 0.7), (0.3, 0.7), (0.2, 0.7), (0.1, 0.7), (0, 0.7),
                                                 # (0.99, 0.2), (0.9, 0.2), (0.8, 0.2), (0.7, 0.2), (0.6, 0.2), (0.5, 0.2),
                                                 # (0.4, 0.2), (0.3, 0.2), (0.2, 0.2), (0.1, 0.2), (0, 0.2),
                                                 (0.99, 0.1), (0.9, 0.1), (0.8, 0.1), (0.7, 0.1), (0.6, 0.1),
                                                 (0.5, 0.1),
                                                 (0.4, 0.1), (0.3, 0.1), (0.2, 0.1), (0.1, 0.1), (0, 0.1)
                                                 ]:
            ps = []
            rs = []
            fs = []
            fps = []
            for i in range(10):
                copy = [[el for el in original_data[i]] for i in range(len(original_data))]
                perturbed = perturb_data_uniform(copy, perturbation_factor)
                # original_data = generate_data(100)

                p, r, f, fp = col2col_ress(original_data, perturbed, sample_size, "test.null")
                ps.append(p)
                rs.append(r)
                fs.append(f)
                fps.append(fs)

            p = np.average(np.asarray(ps))
            r = np.average(np.asarray(rs))
            f = np.average(np.asarray(fs))
            fp = np.average(np.asarray(fps))

            data_point = [str(perturbation_factor), str(sample_size), str(p), str(r), str(f), str(fp)]
            string_repr = ",".join(data_point) + '\n'
            fil.write(string_repr)


if __name__ == "__main__":
    print("Benchmark RESS")

    # # create data
    # original_data = generate_data(100)
    # copy = [[el for el in original_data[i]] for i in range(len(original_data))]
    # perturbed = perturb_data_uniform(copy, 0.2)
    # # original_data = generate_data(100)
    #
    # p, r, f, fp = col2col_ress(original_data, perturbed, 0.2, "test")
    # print("P: " + str(p))
    # print("R: " + str(r))
    # print("F: " + str(f))
    # print("IO: " + str(fp))

    testbed("test.null")

