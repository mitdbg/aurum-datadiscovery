from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from DoD.dod import DoD
from DoD import data_processing_utils as dpu
from DoD import view_4c_analysis_baseline as v4c

from tqdm import tqdm

import os
import time
from collections import defaultdict


def create_folder(base_folder, name):
    op = base_folder + name
    os.makedirs(op)
    return op


def run_dod(dod, attrs, values, output_path, max_hops=2):
    view_metadata_mapping = dict()
    i = 0
    for mjp, attrs_project, metadata in dod.virtual_schema_iterative_search(attrs, values, max_hops=max_hops,
                                                                            debug_enumerate_all_jps=False):
        proj_view = dpu.project(mjp, attrs_project)

        if output_path is not None:
            view_path = output_path + "/view_" + str(i)
            proj_view.to_csv(view_path, encoding='latin1', index=False)  # always store this
            # store metadata associated to that view
            view_metadata_mapping[view_path] = metadata

        i += 1


def assemble_views():
    # have a way of generating the views for each query-view in a different folder
    for qv_name, qv_attr, qv_values in tqdm(query_view_definitions_many):
        print("Running query: " + str(qv_name))
        # Create a folder for each query-view
        output_path = create_folder(eval_folder, "many/" + qv_name)
        print("Out path: " + str(output_path))
        run_dod(dod, qv_attr, qv_values, output_path=output_path)

    for qv_name, qv_attr, qv_values in tqdm(query_view_definitions_few):
        print("Running query: " + str(qv_name))
        # Create a folder for each query-view
        output_path = create_folder(eval_folder, "few/" + qv_name)
        print("Out path: " + str(output_path))
        run_dod(dod, qv_attr, qv_values, output_path=output_path)


def run_4c(path):
    groups_per_column_cardinality = v4c.main(path)
    return groups_per_column_cardinality


def run_4c_nochasing(path):
    groups_per_column_cardinality = v4c.nochasing_main(path)
    return groups_per_column_cardinality


def run_4c_valuewise_main(path):
    groups_per_column_cardinality = v4c.valuewise_main(path)
    return groups_per_column_cardinality


def output_4c_results(groups_per_column_cardinality):
    print("RESULTS: ")
    for k, v in groups_per_column_cardinality.items():
        print("")
        print("Analyzing group with columns = " + str(k))
        print("")
        compatible_groups = v['compatible']
        contained_groups = v['contained']
        complementary_group = v['complementary']
        contradictory_group = v['contradictory']

        print("Compatible views: " + str(len(compatible_groups)))
        print("Contained views: " + str(len(contained_groups)))
        s_containments = dict()
        if len(contained_groups) > 0:
            containments = defaultdict(set)
            for contg in contained_groups:
                contains, contained = contg[0], contg[1:]
                containments[contains].update(contained)
            # now summarize dict
            to_summarize = set()
            for k, v in containments.items():
                for k2, v2 in containments.items():
                    if k == k2:
                        continue
                    if k in v2:
                        to_summarize.add(k)
                        containments[k2].update(v)  # add containments of k to k2
            for k, v in containments.items():
                if k not in to_summarize:
                    s_containments[k] = v
            for k, v in s_containments.items():
                print(str(k) + " contains: " + str(v))
        print("Complementary views: " + str(len(complementary_group)))
        if len(complementary_group) > 0:
            for p1, p2, _, _ in complementary_group:
                print(str(p1) + " is complementary with: " + str(p2))
        print("Contradictory views: " + str(len(contradictory_group)))
        if len(contradictory_group) > 0:
            contradictions = defaultdict(lambda: defaultdict(list))
            for path1, k, contradictory_key1, path2 in contradictory_group:
                if path1 not in contradictions and path2 not in contradictions:
                    contradictions[path1][(k, contradictory_key1)].append(path2)
                elif path1 in contradictions:
                    if path2 not in contradictions[path1][(k, contradictory_key1)]:
                        contradictions[path1][(k, contradictory_key1)].append(path2)
                elif path2 in contradictions:
                    if path1 not in contradictions[path2][(k, contradictory_key1)]:
                        contradictions[path2][(k, contradictory_key1)].append(path1)
                        # print(path1 + " contradicts: " + path2 + " when " + str(k) + " = " + str(contradictory_key1))
            # contradictions_ordered = sorted(contradictions.items(), key=lambda x: len(x[0][x[1]]), reverse=True)
            for k, v in contradictions.items():
                for contradiction_value, tables in v.items():
                    attr_k, value_k = contradiction_value
                    print(k + " contradicts: " + str(len(tables)) + " tables when " +
                          str(attr_k) + " = " + str(value_k))
            print("Summarized contradictions: " + str(len(set(contradictions.keys()))))
            # print("Relevant contradictions: " + str(len([k for k, _ in contradictions.items() if k not in s_containments])))
            for k, v in contradictions.items():
                if k not in s_containments:
                    for contradiction_value, tables in v.items():
                        attr_k, value_k = contradiction_value
                        print(k + " contradicts: " + str(len(tables)) + " tables when " +
                              str(attr_k) + " = " + str(value_k))
            print("Relevant contradictions: " + str(
                len(set([k for k, _ in contradictions.items() if k not in s_containments]))))


def compare_4c_baselines(path):
    s = time.time()
    run_4c(path)
    e = time.time()
    print("Chasing: " + str((e-s)))

    s = time.time()
    run_4c_nochasing(path)
    e = time.time()
    print("No Chasing: " + str((e - s)))

    s = time.time()
    run_4c_valuewise_main(path)
    e = time.time()
    print("Value Wise: " + str((e - s)))


if __name__ == "__main__":
    print("DoD evaluation")

    # Eval parameters
    eval_folder = "dod_evaluation/vassembly/"

    query_view_definitions_many = [
        ("qv2", ["Building Name Long", "Ext Gross Area", "Building Room", "Room Square Footage"],
         ["", "", "", ""]),
        ("qv4", ["Email Address", "Department Full Name"],
         ["madden@csail.mit.edu", ""]),
        ("qv5", ["Last Name", "Building Name", "Bldg Gross Square Footage", "Department Name"],
         ["", "", "", ""])
    ]

    query_view_definitions_few = [
        ("qv1", ["Iap Category Name", "Person Name", "Person Email"],
         ["Engineering", "", ""]),
        ("qv3", ["Last Name", "Building Name", "Bldg Gross Square Footage", "Department Name"],
         ["Madden", "Ray and Maria Stata Center", "", "Dept of Electrical Engineering & Computer Science"]),
    ]

    # Configure DoD
    path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/mitdwh/"
    sep = ","
    store_client = StoreHandler()
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    dod = DoD(network=network, store_client=store_client, csv_separwator=sep)

    # 0- Assemble views for query views. To have raw number of views
    # assemble_views()

    # 1.5- then have a way for calling 4c on each folder -- on all folders. To compare savings (create strategy here)
    # path = "dod_evaluation/vassembly/many/qv5/"
    # groups_per_column_cardinality = run_4c(path)
    # output_4c_results(groups_per_column_cardinality)

    # 2- 4c efficienty
    # 2.1- with many views to show advantage with respect to other less sophisticated baselines
    # 2.2- with few views to show that the overhead it adds is negligible
    path = "dod_evaluation/vassembly/many/qv4/"
    compare_4c_baselines(path)

    # 3- Measure average time per join attempt. Add total times as well



