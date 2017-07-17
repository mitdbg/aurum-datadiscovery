from os import listdir
import pandas as pd
from datasketch import MinHash, MinHashLSH
import numpy as np
import pickle
import time

global mh_kv
mh_kv = None


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


def compute_and_store_mh(path1, store_path, permutations):
    fnames = [f for f in listdir(path1)]
    mh_kv = dict()
    for f in fnames:
        print("File: " + str(f))

        df1 = pd.read_csv(path1 + f, sep=";", dtype=str)
        cols = df1.columns
        for c in cols:
            val1 = set(df1[c].values)
            mh1 = get_mh(val1, permutations)
            key = f + c
            mh_kv[key] = mh1
    store_file = open(store_path + str(permutations), 'wb')
    pickle.dump(mh_kv, store_file)
    return mh_kv


def deserialize_store_mh(path1):
    f = open(path1, 'rb')
    mh_kv = pickle.load(f)
    return mh_kv


def col2col_ress(path1, path2, sample_perc, store_path):
    with open(store_path, 'w') as fw:
        fnames = [f for f in listdir(path1)]
        for f in fnames:
            print("File: " + str(f))

            df1 = pd.read_csv(path1 + f, sep=";", dtype=str)
            df2 = pd.read_csv(path2 + f, sep=";", dtype=str)
            cols = df1.columns
            for c in cols:
                print("  Col: " + str(c))
                val1 = set(df1[c].values)
                val2 = df2[c].drop_duplicates()

                val2 = get_random_sample(val2, sample_perc)
                #mh1 = get_mh(val1)
                mh1 = mh_kv[f+c]
                mh2 = get_mh(val2)
                card1 = len(val1)
                card2 = len(val2)
                if card1 == 0:
                    max_js = 0
                else:
                    max_js = card2 / card1
                est_js = mh1.jaccard(mh2)
                if max_js == 0:
                    scaled_est_js = 0
                else:
                    scaled_est_js = est_js / max_js

                s = str(f)+"."+str(c)+","+str(max_js)+","+str(est_js)+","+str(scaled_est_js)+'\n'
                fw.write(s)
                print("EST-JS: " + str(scaled_est_js))


def col2col_js(path1, path2, store_path):
    with open(store_path, 'w') as fw:
        fnames = [f for f in listdir(path1)]
        for f in fnames:
            print("File: " + str(f))

            df1 = pd.read_csv(path1 + f, sep=";", dtype=str)
            df2 = pd.read_csv(path2 + f, sep=";", dtype=str)
            cols = df1.columns
            for c in cols:
                print("  Col: " + str(c))
                val1 = set(df1[c].values)
                val2 = set(df2[c].values)
                intersection = val1.intersection(val2)
                union = val1.union(val2)
                js = len(intersection) / len(union)
                print("  JS: " + str(js))
                s = str(f)+"."+str(c)+","+str(js)+'\n'
                fw.write(s)


def obtain_diffs(path1, path2):
    set1 = set([f for f in listdir(path1)])
    set2 = set([f for f in listdir(path2)])

    intersection = set1.intersection(set2)
    diff1 = set1.difference(intersection)
    diff2 = set2.difference(intersection)
    diffs = diff1.union(diff2)
    return diffs


def cols_changed_more_than(path, threshold):
    total_lines = 0
    lines_changed = 0
    with open(path, 'r') as f:
        for line in f:
            total_lines += 1
            tokens = line.split(',')
            score = float((tokens[-1]).rstrip().strip())
            if score < threshold:
                lines_changed += 1
    print("Total lines: " + str(total_lines))
    print("Change lines: " + str(lines_changed))


def measure_pr(gt_path, mh_path, threshold):
    gt = set()
    with open(gt_path, 'r') as f:
        for line in f:
            tokens = line.split(',')
            element = (tokens[0]).rstrip().strip()
            score = float((tokens[-1]).rstrip().strip())
            if score < threshold:
                gt.add(element)
    res = set()
    with open(mh_path, 'r') as f:
        for line in f:
            tokens = line.split(',')
            element = (tokens[0]).rstrip().strip()
            score = float((tokens[-1]).rstrip().strip())
            if score < threshold:
                res.add(element)
    total_gt = len(gt)
    found = 0
    for el in gt:
        if el in res:
            found += 1
    total_res = len(res)
    false_positives = 0
    for el in res:
        if el not in gt:
            false_positives += 1
    recall = found / total_gt
    precision = (total_res - false_positives) / total_res
    return precision, recall, total_gt, total_res


def experiment_data(base_path, output_path, thresholds=[0.9, 0.8, 0.7, 0.6, 0.5]):
    gt_path = base_path + "../original/js_scores.csv"
    with open(output_path, 'w') as f:
        s = "threshold,sample,total_gt,total_res,precision,recall,f1score\n"
        f.write(s)
        for th in thresholds:
            for d in ["/s100", "/s75", "/s50", "/s25", "/s10"]:
                res_path = base_path + d + "/mh_scores.csv"
                p, r, total_gt, total_res = measure_pr(gt_path, res_path, th)
                f1score = 2 * (p * r) / (p + r)
                s = str(th) + "," + str(d) + "," + str(total_gt) \
                    + "," + str(total_res) + "," + str(p) + "," + str(r) + "," + str(f1score) + '\n'
                f.write(s)


def format_results_to_dat(path, output_path):

    with open(output_path, 'w') as g:
        xpoint = 0
        cur_threshold = 1
        s = "# threshold, fscore100, fscore75, fscore50, fscore25, fscore10\n"
        g.write(s)
        with open(path, 'r') as f:
            line_counter = 0
            f_collector = []
            first = True
            for l in f:
                if first:
                    first = False
                    continue
                if line_counter == 5:
                    s = str(xpoint) + " " + str(cur_threshold) + " " + " ".join(f_collector) + '\n'
                    xpoint += 1
                    line_counter = 0
                    f_collector = []
                    #cur_threshold -= 0.1
                    g.write(s)
                line_counter += 1
                tokens = l.split(",")
                cur_threshold = float(tokens[0].rstrip().strip())
                fscore = (tokens[-1]).rstrip().strip()
                f_collector.append(fscore)
            s = str(xpoint) + " " + str(cur_threshold) + " " + " ".join(f_collector) + '\n'
            g.write(s)


def obtain_error_data(path_or, path_data, output_path):
    original = []
    with open(path_or, 'r') as f:
        for l in f:
            tokens = l.split(',')
            js = (tokens[1]).rstrip().strip()
            original.append(float(js))
    s100 = []
    with open(path_data + "s100/mh_scores.csv", 'r') as f:
        for l in f:
            tokens = l.split(',')
            est_js = (tokens[-1]).rstrip().strip()
            s100.append(float(est_js))
    s75 = []
    with open(path_data + "s75/mh_scores.csv", 'r') as f:
        for l in f:
            tokens = l.split(',')
            est_js = (tokens[-1]).rstrip().strip()
            s75.append(float(est_js))
    s50 = []
    with open(path_data + "s50/mh_scores.csv", 'r') as f:
        for l in f:
            tokens = l.split(',')
            est_js = (tokens[-1]).rstrip().strip()
            s50.append(float(est_js))
    s25 = []
    with open(path_data + "s25/mh_scores.csv", 'r') as f:
        for l in f:
            tokens = l.split(',')
            est_js = (tokens[-1]).rstrip().strip()
            s25.append(float(est_js))
    s10 = []
    with open(path_data + "s10/mh_scores.csv", 'r') as f:
        for l in f:
            tokens = l.split(',')
            est_js = (tokens[-1]).rstrip().strip()
            s10.append(float(est_js))

    error_list = []
    for idx in range(len(original)):
        js_or = original[idx]
        js_est_100 = s100[idx]
        js_est_75 = s75[idx]
        js_est_50 = s50[idx]
        js_est_25 = s25[idx]
        js_est_10 = s10[idx]
        # e100 = (js_est_100 - js_or) ** 2
        # e75 = (js_est_75 - js_or) ** 2
        # e50 = (js_est_50 - js_or) ** 2
        # e25 = (js_est_25 - js_or) ** 2
        # e10 = (js_est_10 - js_or) ** 2
        e100 = abs(js_est_100 - js_or)
        e75 = abs(js_est_75 - js_or)
        e50 = abs(js_est_50 - js_or)
        e25 = abs(js_est_25 - js_or)
        e10 = abs(js_est_10 - js_or)
        s = str(idx) + " " + str(e100) + " " + str(e75) + " " + str(e50) + " " + str(e25) + " " + str(e10) + '\n'
        error_list.append(s)
        print(s)
    with open(output_path, 'w') as f:
        for el in error_list:
            f.write(el)


if __name__ == "__main__":
    print("Utils-RESS")

    obtain_error_data("/Users/ra-mit/data/ress_exp/original/js_scores.csv",
                      "/Users/ra-mit/data/ress_exp/mh512/",
                      "/Users/ra-mit/data/ress_exp/error.dat")
    exit()
    #
    # format_results_to_dat("/Users/ra-mit/data/ress_exp/results_mh512.csv",
    #                       "/Users/ra-mit/data/ress_exp/results_mh512.dat")
    # exit()




    # thresholds = list()
    # th = 101
    # for i in range(100):
    #     th -= 1
    #     #th = th/100
    #     thresholds.append(th/100)
    # for el in thresholds:
    #     print(str(el))
    # #
    # experiment_data("/Users/ra-mit/data/ress_exp/mh512/", "/Users/ra-mit/data/ress_exp/results_mh512.csv",
    #                 thresholds=thresholds)
    # exit()


    # s = time.time()
    # col2col_js("/Users/ra-mit/data/ress_exp/original/c21/",
    #            "/Users/ra-mit/data/ress_exp/original/c22/",
    #            "/Users/ra-mit/data/ress_exp/original/js_scores.csv")
    # e = time.time()
    # print("Total time GT: " + str(e-s))
    #
    # exit()

    # p, r, total_gt, total_res = measure_pr("/Users/ra-mit/data/ress_exp/original/js_scores.csv",
    #            "/Users/ra-mit/data/ress_exp/s25/mh_scores.csv",
    #            0.5)
    # print("p: " + str(p))
    # print("r: " + str(r))
    # exit()

    # cols_changed_more_than("/Users/ra-mit/data/ress_exp/original/js_scores.csv", 0.5)
    # cols_changed_more_than("/Users/ra-mit/data/ress_exp/s100/mh_scores.csv", 0.5)
    # cols_changed_more_than("/Users/ra-mit/data/ress_exp/s75/mh_scores.csv", 0.5)
    # cols_changed_more_than("/Users/ra-mit/data/ress_exp/s50/mh_scores.csv", 0.5)
    # cols_changed_more_than("/Users/ra-mit/data/ress_exp/s25/mh_scores.csv", 0.5)
    # exit()

    global mh_kv
    mh_kv = deserialize_store_mh("/Users/ra-mit/data/ress_exp/mh_kv_512.pkl")
    # for k, v in mh_kv.items():
    #     print(str(k))
    #     print(str(v))
    # exit()

    # mh_kv = compute_and_store_mh("/Users/ra-mit/data/ress_exp/original/c21/",
    #                              "/Users/ra-mit/data/ress_exp/mh_kv128.pkl",
    #                              permutations=512)


    # print("Compute mh 100")
    # s = time.time()  # 1789(512) 1415(128)
    # col2col_ress("/Users/ra-mit/data/ress_exp/original/c21/",
    #             "/Users/ra-mit/data/ress_exp/original/c22/",
    #             1.0,
    #             "/Users/ra-mit/data/ress_exp/mh512/s100/mh_scores.csv")
    # e = time.time()
    # print("mh100: " + str(e - s))
    #

    # print("Compute mh 75")
    # s = time.time()  # 1360(512) 1100(128)
    # col2col_ress("/Users/ra-mit/data/ress_exp/original/c21test/",
    #              "/Users/ra-mit/data/ress_exp/original/c22test/",
    #              0.75,
    #              "/Users/ra-mit/data/ress_exp/mh512/s75/mh_scores.csv")
    # e = time.time()
    # print("mh75: " + str(e - s))
    #
    # exit()


    print("Compute mh 75")
    s = time.time()  # 1360(512) 1100(128)
    col2col_ress("/Users/ra-mit/data/ress_exp/original/c21/",
                 "/Users/ra-mit/data/ress_exp/original/c22/",
                 0.75,
                 "/Users/ra-mit/data/ress_exp/mh512/s75/mh_scores.csv")
    e = time.time()
    print("mh75: " + str(e - s))

    print("Compute mh 50")
    s = time.time()  # 974(512) 805(128)
    col2col_ress("/Users/ra-mit/data/ress_exp/original/c21/",
                 "/Users/ra-mit/data/ress_exp/original/c22/",
                 0.5,
                 "/Users/ra-mit/data/ress_exp/mh512/s50/mh_scores.csv")
    e = time.time()
    print("mh50: " + str(e - s))

    print("Compute mh 25")
    s = time.time()  # 646(512) 500(128)
    col2col_ress("/Users/ra-mit/data/ress_exp/original/c21/",
                 "/Users/ra-mit/data/ress_exp/original/c22/",
                 0.25,
                 "/Users/ra-mit/data/ress_exp/mh512/s25/mh_scores.csv")
    e = time.time()
    print("mh25: " + str(e - s))

    print("Compute mh 10")
    s = time.time()
    col2col_ress("/Users/ra-mit/data/ress_exp/original/c21/",
                 "/Users/ra-mit/data/ress_exp/original/c22/",
                 0.1,
                 "/Users/ra-mit/data/ress_exp/mh512/s10/mh_scores.csv")
    e = time.time()
    print("mh10: " + str(e - s))

    exit()





    # diffs = obtain_diffs("/Users/ra-mit/data/ress_exp/original/c21", "/Users/ra-mit/data/ress_exp/original/c22")
    # for el in diffs:
    #     print(str(el))
