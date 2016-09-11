from knowledgerepr import syn_network_generator as syn
from knowledgerepr.fieldnetwork import FieldNetwork
import numpy as np
from ddapi import API

import timeit


api = None


def query1():
    return


def query2():
    return


def query3():
    return


def query4():
    return


def run_all_queries(repetitions):

    # Query 1
    query1_times = []
    total_time = timeit.timeit('query1()', setup="from query_processing_benchmarks import query1", number=repetitions)
    query1_times.append(total_time)

    # Query 2
    query2_times = []
    total_time = timeit.timeit('query2()', setup="from query_processing_benchmarks import query2", number=repetitions)
    query2_times.append(total_time)

    # Query 3
    query3_times = []
    total_time = timeit.timeit('query3()', setup="from query_processing_benchmarks import query3", number=repetitions)
    query3_times.append(total_time)

    # Query 4
    query4_times = []
    total_time = timeit.timeit('query4()', setup="from query_processing_benchmarks import query4", number=repetitions)
    query4_times.append(total_time)

    return query1_times, query2_times, query3_times, query4_times


if __name__ == "__main__":

    # Fixed graph density, differing sizes (nodes)

    fn = syn.generate_network_with(num_nodes=100, num_nodes_per_table=10, num_schema_sim=200,
                                   num_content_sim=150, num_pkfk=50)
    api = API(fn)

    q1, q2, q3, q4 = run_all_queries(100)

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
