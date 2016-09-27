from knowledgerepr import syn_network_generator as syn
from api.apiutils import  Relation
import numpy as np
from ddapi import API

import timeit
import time


api = None

"""
Global variables to use by the queries
"""
in_drs = None

def query1():
    # Lookup queries
    return


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
    #h1 = api.drs_from_hit(hits[0])
    #h2 = api.drs_from_hit(hits[1])
    s = time.time()
    #res = api.paths_between(h1, h2, Relation.CONTENT_SIM)
    e = time.time()
    return s, e


def run_all_queries(repetitions, api_obj=None, in_drs_obj=None):

    # Define global variables to use by the queries
    global api, in_drs
    api = api_obj
    in_drs = in_drs_obj

    # Query 1
    query1_times = []
    for i in range(repetitions):
        s = time.time()
        query1()
        e = time.time()
        query1_times.append((e - s))

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

    return query1_times, query2_times, query3_times, query4_times


def experiment_changing_input_size(repetitions=100):

    # Create a graph

    fn = syn.generate_network_with(num_nodes=200, num_nodes_per_table=10, num_schema_sim=200,
                                   num_content_sim=150, num_pkfk=50)

    api = API(fn)

    perf_results = dict()

    # input size from 1 to 100
    for i in range(20):
        i = i + 1
        nodes = fn.fields_degree(i)
        nids = [x for x, y in nodes]
        info = fn.get_info_for(nids)
        hits = fn.get_hits_from_info(info)
        in_drs = api.drs_from_hits(hits)

        q1, q2, q3, q4 = run_all_queries(repetitions, api_obj=api, in_drs_obj=in_drs)
        percentile_results = get_percentiles([q1, q2, q3, q4])
        perf_results[i] = percentile_results
    return perf_results


def experiment_changing_graph_size_constant_density(repetitions=100):
    sizes = [100, 1000, 10000, 100000]
    perf_results = dict()
    for size in sizes:
        relations = int(size/10)
        fn = syn.generate_network_with(num_nodes=size, num_nodes_per_table=10, num_schema_sim=relations,
                                   num_content_sim=relations, num_pkfk=relations)

        api = API(fn)

        nodes = fn.fields_degree(3)
        nids = [x for x, y in nodes]
        info = fn.get_info_for(nids)
        hits = fn.get_hits_from_info(info)
        in_drs = api.drs_from_hits(hits)

        q1, q2, q3, q4 = run_all_queries(repetitions, api_obj=api, in_drs_obj=in_drs)
        percentile_results = get_percentiles([q1, q2, q3, q4])
        perf_results[size] = percentile_results

    return perf_results


def experiment_changing_graph_density_constant_size(repetitions=10):
    size = 10000
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

        q1, q2, q3, q4 = run_all_queries(repetitions, api_obj=api, in_drs_obj=in_drs)
        percentile_results = get_percentiles([q1, q2, q3, q4])
        perf_results[density] = percentile_results

    return perf_results


def experiment_changing_max_hops_tc_queries(repetitions=100):
    perf_results = dict()
    for i in range(9):
        i = i +1
        fn = syn.generate_network_with(num_nodes=1000, num_nodes_per_table=10, num_schema_sim=500,
                                       num_content_sim=500, num_pkfk=500)

        api = API(fn)

        nodes = fn.fields_degree(1)
        nids = [x for x, y in nodes]
        info = fn.get_info_for(nids)
        hits = fn.get_hits_from_info(info)
        in_drs = api.drs_from_hits(hits)

        query_times = []
        for repet in range(repetitions):
            s = time.time()
            res = api.traverse_field(in_drs, Relation.CONTENT_SIM, max_hops=i)
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


if __name__ == "__main__":

    #changing_size_results = experiment_changing_input_size(repetitions=10)
    #for k, v in changing_size_results.items():
    #    print(str(k) + " -> " + str(v))

    #changing_graph_size_results = experiment_changing_graph_size_constant_density(repetitions=10)
    #for k, v in changing_graph_size_results.items():
    #    print(str(k) + " -> " + str(v))

    #changing_graph_density_results = experiment_changing_graph_density_constant_size(repetitions=10)
    #for k, v in changing_graph_density_results.items():
    #    print(str(k) + " -> " + str(v))

    changing_hops_results = experiment_changing_max_hops_tc_queries(repetitions=10)
    for k, v in changing_hops_results.items():
        print(str(k) + " -> " + str(v))

    exit()

    # Fixed graph density, differing sizes (nodes)

    fn = syn.generate_network_with(num_nodes=100, num_nodes_per_table=10, num_schema_sim=200,
                                   num_content_sim=150, num_pkfk=50)
    api = API(fn)

    nodes = fn.fields_degree(3)
    nids = [x for x, y in nodes]
    info = fn.get_info_for(nids)
    hits = fn.get_hits_from_info(info)
    in_drs = api.drs_from_hits(hits)

    q1, q2, q3, q4 = run_all_queries(100, api_obj=api, in_drs_obj=in_drs)

    nq1 = np.array(q1)
    p5 = np.percentile(nq1, 5)
    p50 = np.percentile(nq1, 50)
    p95 = np.percentile(nq1, 95)
    print("q1: " + str(p5) + " - " + str(p50) + " - " + str(p95))

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
