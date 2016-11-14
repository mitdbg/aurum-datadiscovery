from knowledgerepr import syn_network_generator as syn
from api.apiutils import Relation
import numpy as np
from ddapi import API

import timeit
import time


api = None

"""
Global variables to use by the queries
"""
in_drs = None

def query2():
    # Neighbor queries
    s = time.time()
    res = api.similar_content_to(in_drs)
    e = time.time()
    return s, e

def query3():
    # Intersection queries
    res1 = api.similar_content_to(in_drs)
    res2 = api.similar_schema_name_to(in_drs)
    s = time.time()
    res = api.intersection(res1, res2)
    e = time.time()
    return s, e


def query4():
    # TC queries
    hits = [x for x in in_drs]
    h1 = api.drs_from_hit(hits[0])
    h2 = api.drs_from_hit(hits[len(hits) - 1])
    s = time.time()
    res = api.paths_between(h1, h2, Relation.CONTENT_SIM)
    e = time.time()
    return s, e


def run_all_queries(repetitions, api_obj=None, in_drs_obj=None):

    # Define global variables to use by the queries
    global api, in_drs
    api = api_obj
    in_drs = in_drs_obj

    # Query 2
    query2_times = []
    for i in range(repetitions):
        s, e = query2()
        query2_times.append((e - s))

    # Query 3
    query3_times = []
    for i in range(repetitions):
        s, e = query3()
        query3_times.append((e - s))

    # Query 4
    query4_times = []
    for i in range(repetitions):
        s = time.time()
        query4()
        e = time.time()
        query4_times.append((e - s))

    return query2_times, query3_times, query4_times


def experiment_changing_input_size(repetitions=100):

    # Create a graph

    fn = syn.generate_network_with(num_nodes=100000, num_nodes_per_table=10, num_schema_sim=90000,
                                   num_content_sim=90000, num_pkfk=90000)

    api = API(fn)

    perf_results = dict()

    # input size from 1 to 100
    for i in range(50):
        i = i + 1
        nodes = fn.fields_degree(i)
        nids = [x for x, y in nodes]
        info = fn.get_info_for(nids)
        hits = fn.get_hits_from_info(info)
        in_drs = api.drs_from_hits(hits)

        q2, q3, q4 = run_all_queries(repetitions, api_obj=api, in_drs_obj=in_drs)
        percentile_results = get_percentiles([q2, q3, q4])
        perf_results[i] = percentile_results
    return perf_results


def experiment_changing_graph_size_constant_density(repetitions=10):
    sizes = [100, 1000, 10000, 100000, 1000000]
    perf_results = dict()
    for size in sizes:
        relations = int(size)
        fn = syn.generate_network_with(num_nodes=size, num_nodes_per_table=10, num_schema_sim=relations,
                                       num_content_sim=relations, num_pkfk=relations)

        api = API(fn)

        nodes = fn.fields_degree(1)
        nids = [x for x, y in nodes]
        info = fn.get_info_for(nids)
        hits = fn.get_hits_from_info(info)
        in_drs = api.drs_from_hits(hits)

        q2, q3, q4 = run_all_queries(repetitions, api_obj=api, in_drs_obj=in_drs)
        percentile_results = get_percentiles([q2, q3, q4])
        perf_results[size] = percentile_results

    return perf_results


def experiment_changing_graph_density_constant_size(repetitions=10):
    size = 100000
    densities = [100, 1000, 10000, 100000, 1000000]
    perf_results = dict()
    for density in densities:
        fn = syn.generate_network_with(num_nodes=size, num_nodes_per_table=10, num_schema_sim=density,
                                       num_content_sim=density, num_pkfk=density)

        api = API(fn)

        nodes = fn.fields_degree(3)
        nids = [x for x, y in nodes]
        info = fn.get_info_for(nids)
        hits = fn.get_hits_from_info(info)
        in_drs = api.drs_from_hits(hits)

        q2, q3, q4 = run_all_queries(repetitions, api_obj=api, in_drs_obj=in_drs)
        percentile_results = get_percentiles([q2, q3, q4])
        perf_results[density] = percentile_results

    return perf_results


def experiment_changing_max_hops_tc_queries(repetitions=100):
    perf_results = dict()
    for i in range(10):
        i = i +1
        fn = syn.generate_network_with(num_nodes=100000, num_nodes_per_table=10, num_schema_sim=100000,
                                       num_content_sim=100000, num_pkfk=100000)

        api = API(fn)

        nodes = fn.fields_degree(1)
        nids = [x for x, y in nodes]
        info = fn.get_info_for(nids)
        hits = fn.get_hits_from_info(info)
        in_drs = api.drs_from_hits(hits)

        query_times = []
        for repet in range(repetitions):
            s = time.time()
            res = api.traverse(in_drs, Relation.SCHEMA_SIM, max_hops=i)
            e = time.time()
            query_times.append((e - s))

        percentile_results = get_percentiles([query_times])
        perf_results[i] = percentile_results
    return perf_results


def get_percentiles(list_of_lists):
    results = []
    for l in list_of_lists:
        nq = np.array(l)
        p5 = np.percentile(nq, 5)
        p50 = np.percentile(nq, 50)
        p95 = np.percentile(nq, 95)
        percentiles = (p5, p50, p95)
        results.append(percentiles)
    return results


def test():
    # Fixed graph density, differing sizes (nodes)

    fn = syn.generate_network_with(num_nodes=100, num_nodes_per_table=10, num_schema_sim=200,
                                   num_content_sim=150, num_pkfk=50)
    api = API(fn)

    nodes = fn.fields_degree(3)
    nids = [x for x, y in nodes]
    info = fn.get_info_for(nids)
    hits = fn.get_hits_from_info(info)
    in_drs = api.drs_from_hits(hits)

    q2, q3, q4 = run_all_queries(100, api_obj=api, in_drs_obj=in_drs)

    nq2 = np.array(q2)
    p5 = np.percentile(nq2, 5)
    p50 = np.percentile(nq2, 50)
    p95 = np.percentile(nq2, 95)
    print("q2: " + str(p5) + " - " + str(p50) + " - " + str(p95))

    nq3 = np.array(q3)
    p5 = np.percentile(nq3, 5)
    p50 = np.percentile(nq3, 50)
    p95 = np.percentile(nq3, 95)
    print("q3: " + str(p5) + " - " + str(p50) + " - " + str(p95))

    nq4 = np.array(q4)
    p5 = np.percentile(nq4, 5)
    p50 = np.percentile(nq4, 50)
    p95 = np.percentile(nq4, 95)
    print("q4: " + str(p5) + " - " + str(p50) + " - " + str(p95))


def write_csv(name, lines):
    with open(name, 'a') as csvfile:
        for line in lines:
            csvfile.write(line)
            csvfile.write('\n')


def write_results_to_csv_three_queries(name, results, csv=False, dat=False):
    lines = []

    from collections import OrderedDict
    od = OrderedDict(sorted(results.items()))

    header = None
    if csv:
        header = "x_axis,q2_5p,q2_median,q2_95p,q3_5p,q3_median,q3_95p,q4_5p,q4_median,q4_95p"
    elif dat:
        header = "# x_axis q2_5p q2_median q2_95p q3_5p q3_median q3_95p q4_5p q4_median q4_95p"
    lines.append(header)
    for k, v in od.items():
        (q2, q3, q4) = v
        (fivep_2, median_2, ninetyp_2) = q2
        (fivep_3, median_3, ninetyp_3) = q3
        (fivep_4, median_4, ninetyp_4) = q4
        separator = None
        if csv:
            separator = ','
        elif dat:
            separator = ' '
        string = separator.join([str(k), str(fivep_2), str(median_2), str(ninetyp_2), str(fivep_3),
                           str(median_3), str(ninetyp_3), str(fivep_4), str(median_4), str(ninetyp_4)])
        lines.append(string)

    write_csv(name, lines)


def write_results_to_csv_one_query(name, results, csv=False, dat=False):
    lines = []

    from collections import OrderedDict
    od = OrderedDict(sorted(results.items()))

    header = None
    if csv:
        header = "x_axis,q2_5p,q2_median,q2_95p,q3_5p,q3_median,q3_95p,q4_5p,q4_median,q4_95p"
    elif dat:
        header = "# x_axis q2_5p q2_median q2_95p q3_5p q3_median q3_95p q4_5p q4_median q4_95p"
    lines.append(header)
    for k, v in od.items():
        (fivep_2, median_2, ninetyp_2) = v[0]
        separator = None
        if csv:
            separator = ','
        elif dat:
            separator = ' '
        string = separator.join([str(k), str(fivep_2), str(median_2), str(ninetyp_2)])
        lines.append(string)

    write_csv(name, lines)


if __name__ == "__main__":

    changing_size_results = experiment_changing_input_size(repetitions=10)
    for k, v in changing_size_results.items():
        print(str(k) + " -> " + str(v))
    write_results_to_csv_three_queries("/Users/ra-mit/research/data-discovery/papers/dd-paper/evaluation_results/qp_performance/data/changing_input_size.dat", changing_size_results, dat=True)

    changing_graph_size_results = experiment_changing_graph_size_constant_density(repetitions=10)
    for k, v in changing_graph_size_results.items():
        print(str(k) + " -> " + str(v))
    write_results_to_csv_three_queries("/Users/ra-mit/research/data-discovery/papers/dd-paper/evaluation_results/qp_performance/data/changing_graph_size_density_constant.dat", changing_graph_size_results, dat=True)

    changing_graph_density_results = experiment_changing_graph_density_constant_size(repetitions=10)
    for k, v in changing_graph_density_results.items():
        print(str(k) + " -> " + str(v))
    write_results_to_csv_three_queries("/Users/ra-mit/research/data-discovery/papers/dd-paper/evaluation_results/qp_performance/data/changing_graph_density_fixed_graph_size.dat", changing_graph_density_results, dat=True)

    changing_hops_results = experiment_changing_max_hops_tc_queries(repetitions=10)
    for k, v in changing_hops_results.items():
        print(str(k) + " -> " + str(v))
    write_results_to_csv_one_query("/Users/ra-mit/research/data-discovery/papers/dd-paper/evaluation_results/qp_performance/data/changing_hops_tc.dat", changing_hops_results, dat=True)

