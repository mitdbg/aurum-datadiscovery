from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from DoD.dod import DoD
from DoD import data_processing_utils as dpu
from DoD import view_4c_analysis_baseline as v4c

from tqdm import tqdm
import pandas as pd

import os
import time
from collections import defaultdict
import pprint


pp = pprint.PrettyPrinter(indent=4)


def create_folder(base_folder, name):
    op = base_folder + name
    os.makedirs(op)
    return op


def run_dod(dod, attrs, values, output_path, max_hops=2, name=None):
    view_metadata_mapping = dict()
    i = 0
    perf_stats = dict()
    st_runtime = time.time()
    for mjp, attrs_project, metadata in dod.virtual_schema_iterative_search(attrs, values, perf_stats, max_hops=max_hops,
                                                                            debug_enumerate_all_jps=False):
        proj_view = dpu.project(mjp, attrs_project)

        if output_path is not None:
            view_path = output_path + "/view_" + str(i)
            proj_view.to_csv(view_path, encoding='latin1', index=False)  # always store this
            # store metadata associated to that view
            view_metadata_mapping[view_path] = metadata

        i += 1
    et_runtime = time.time()
    perf_stats['et_runtime'] = (et_runtime - st_runtime)
    print("#$# " + str(name))
    print("#$# ")
    print("")
    pp.pprint(perf_stats)
    total_join_graphs = sum(perf_stats['num_join_graphs_per_candidate_group'])
    total_materializable_join_graphs = sum(perf_stats['materializable_join_graphs'])
    print("Total join graphs: " + str(total_join_graphs))
    print("Total materializable join graphs: " + str(total_materializable_join_graphs))
    print("")
    print("Total views: " + str(i))
    print("#$# ")


def assemble_views():
    # have a way of generating the views for each query-view in a different folder
    # for qv_name, qv_attr, qv_values in tqdm(query_view_definitions_many):
    #     print("Running query: " + str(qv_name))
    #     # Create a folder for each query-view
    #     output_path = create_folder(eval_folder, "many/" + qv_name)
    #     print("Out path: " + str(output_path))
    #     run_dod(dod, qv_attr, qv_values, output_path=output_path)
    #
    # for qv_name, qv_attr, qv_values in tqdm(query_view_definitions_few):
    #     print("Running query: " + str(qv_name))
    #     # Create a folder for each query-view
    #     output_path = create_folder(eval_folder, "few/" + qv_name)
    #     print("Out path: " + str(output_path))
    #     run_dod(dod, qv_attr, qv_values, output_path=output_path)

    for qv_name, qv_attr, qv_values in tqdm(query_view_definitions_chembl):
        print("Running query: " + str(qv_name))
        # Create a folder for each query-view
        output_path = create_folder(eval_folder, "chembl/" + qv_name)
        print("Out path: " + str(output_path))
        run_dod(dod, qv_attr, qv_values, output_path=output_path)


def measure_dod_performance(qv_name, qv_attr, qv_values):
    # for qv_name, qv_attr, qv_values in tqdm(query_view_definitions_many):
    print("Running query: " + str(qv_name))
    # Create a folder for each query-view
    # output_path = create_folder(eval_folder, "many/" + qv_name)
    output_path = None
    print("Out path: " + str(output_path))
    run_dod(dod, qv_attr, qv_values, output_path=output_path, name=qv_name)


def run_4c(path):
    groups_per_column_cardinality = v4c.main(path)
    return groups_per_column_cardinality


def run_4c_nochasing(path):
    groups_per_column_cardinality = v4c.nochasing_main(path)
    return groups_per_column_cardinality


def run_4c_valuewise_main(path):
    groups_per_column_cardinality = v4c.valuewise_main(path)
    return groups_per_column_cardinality


def brancher(groups_per_column_cardinality):
    """
    Given the 4C output, determine how many interactions this demands
    :param groups_per_column_cardinality:
    :return:
    """
    # interactions_per_group_optimistic = []
    pruned_groups_per_column_cardinality = defaultdict(dict)
    human_selection = 0
    for k, v in groups_per_column_cardinality.items():
        compatible_groups = v['compatible']
        contained_groups = v['contained']
        complementary_group = v['complementary']
        complementary_group = [(a, b, "", "") for a, b, _, _ in complementary_group]
        contradictory_group = v['contradictory']

        # Optimistic path
        contradictions = defaultdict(list)
        if len(contradictory_group) > 0:
            for path1, _, _, path2 in contradictory_group:
                if path1 not in contradictions:
                    contradictions[path1].append(path2)
                if path2 not in contradictions:
                    contradictions[path2].append(path1)
                if path1 not in contradictions[path2]:
                    contradictions[path2].append(path1)
                if path2 not in contradictions[path1]:
                    contradictions[path1].append(path2)
        # Now we sort contradictions by value length. Second sort key for determinism
        contradictions = sorted(contradictions.items(), key=lambda x: (len(x[1]), x[0]), reverse=True)

        if len(contradictions) > 0:
            # Now we loop per each contradiction, after making a decision we prune space of views
            while len(contradictions) > 0:
                human_selection += 1

                pruned_compatible_groups = []
                pruned_contained_groups = []
                pruned_complementary_groups = []

                path1, path2 = contradictions.pop()
                # We assume path1 is good. Therefore, path2 tables are bad. Prune away all path2
                for cg in compatible_groups:
                    valid = True
                    for p2 in path2:
                        if p2 in set(cg):
                            # remove this compatible group
                            valid = False
                            break  # cg is not valid
                    if valid:
                        pruned_compatible_groups.append(cg)
                for contg in tqdm(contained_groups):
                    valid = True
                    for p2 in path2:
                        if p2 in set(contg):
                            valid = False
                            break
                    if valid:
                        pruned_contained_groups.append(contg)

                invalid_paths = set(path2)  # assist lookup for next two blocks
                for compg in complementary_group:
                    compp1, compp2, _, _ = compg
                    if compp1 not in invalid_paths and compp2 not in invalid_paths:
                        pruned_complementary_groups.append((compp1, compp2, "", ""))
                pruned_contradiction_group = []  # remove those with keys in invalid group
                for other_path1, other_path2 in contradictions:
                    if other_path1 not in invalid_paths:  # only check the key
                        pruned_contradiction_group.append((other_path1, other_path2))
                # update all groups with the pruned versions
                contradictions = [el for el in pruned_contradiction_group]
                compatible_groups = [el for el in pruned_compatible_groups]
                contained_groups = [el for el in pruned_contained_groups]
                complementary_group = [(a, b, "", "") for a, b, _, _ in pruned_complementary_groups]

        # Now removed contained views
        # 1- from complementary groups
        contained_views = set()  # all contained views across contained groups
        for contained_group in contained_groups:
            if len(contained_group) >= 2:
                contained_views.update(set(contained_group[1:]))
        pruned_complementary_groups = []
        for compp1, compp2, _, _ in complementary_group:
            if compp1 not in contained_views and compp2 not in contained_views:
                pruned_complementary_groups.append((compp1, compp2, "", ""))
        complementary_group = [(a, b, "", "") for a, b, _, _ in pruned_complementary_groups]

        # 2- from contanied groups
        pruned_compatible_groups = []
        for cg in compatible_groups:
            valid = True
            for el in cg:
                if el in contained_views:
                    # remove this compatible group
                    valid = False
                    break  # cg is not valid
            if valid:
                pruned_compatible_groups.append(cg)
        compatible_groups = [el for el in pruned_compatible_groups]

        # Now union complementary with compatible and coalesce contained with compatible
        compatible_views = set()
        pruned_complementary_groups = []
        for cg in compatible_groups:
            compatible_views.update(cg)
        for compp1, compp2, _, _ in complementary_group:
            if compp1 not in compatible_views and compp2 not in compatible_views:
                pruned_complementary_groups.append((compp1, compp2, "", ""))
        complementary_group = [(a, b, "", "") for a, b, _, _ in pruned_complementary_groups]

        pruned_contained_groups = []
        for contained_group in contained_groups:
            if contained_group[0] not in compatible_views:
                pruned_contained_groups.append(contained_group)
        contained_groups = [el for el in pruned_contained_groups]

        pruned_groups_per_column_cardinality[k]['compatible'] = compatible_groups
        pruned_groups_per_column_cardinality[k]['contained'] = contained_groups
        pruned_groups_per_column_cardinality[k]['complementary'] = complementary_group
        pruned_groups_per_column_cardinality[k]['contradictory'] = {p1: p2 for p1, p2 in contradictions}
    return pruned_groups_per_column_cardinality, human_selection


def summarize_4c_output(groups_per_column_cardinality, schema_id_info):
    interactions_per_group = []
    for k, v in groups_per_column_cardinality.items():
        print("")
        print("Analyzing group with columns = " + str(k))
        print("")
        compatible_groups = v['compatible']
        contained_groups = v['contained']
        complementary_group = v['complementary']
        contradictory_group = v['contradictory']

        # summary complements:
        complementary_summary = defaultdict(set)
        for compg in complementary_group:
            compp1, compp2, _, _ = compg
            if compp1 in complementary_summary:
                complementary_summary[compp1].add(compp2)
            elif compp2 in complementary_group:
                complementary_summary[compp2].add(compp1)
            else:
                complementary_summary[compp1].add(compp2)
        total_interactions = len(compatible_groups) + len(contained_groups) \
                             + len(complementary_summary.keys()) + len(contradictory_group)
        interactions_per_group.append((schema_id_info[k], total_interactions))
    return interactions_per_group


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


def compare_4c_baselines(many_views, few_views):
    for num_views, path in tqdm(many_views):
        print("#$# " + str(path))
        s = time.time()
        run_4c(path)
        e = time.time()
        print("#$# Chasing")
        print("#$# " + str(num_views) + " " + str((e-s)))

        s = time.time()
        run_4c_nochasing(path)
        e = time.time()
        print("#$# No Chasing")
        print("#$# " + str(num_views) + " " + str((e - s)))
    for num_views, path in tqdm(few_views):
        print("#$# " + str(path))
        s = time.time()
        run_4c(path)
        e = time.time()
        print("#$# Chasing")
        print("#$# " + str(num_views) + " " + str((e - s)))

        s = time.time()
        run_4c_nochasing(path)
        e = time.time()
        print("#$# No Chasing")
        print("#$# " + str(num_views) + " " + str((e - s)))

    # s = time.time()
    # run_4c_valuewise_main(path)
    # e = time.time()
    # print("Value Wise: " + str((e - s)))


def eval_sampling_join():
    sep = ';'
    base = "/Users/ra-mit/data/chembl_21/chembl/"
    r1 = 'public.assays.csv'
    r2 = 'public.activities.csv'
    a1 = 'assay_id'
    a2 = 'assay_id'

    # have pairs of tables to join as input -- large tables, which is when this makes sense

    # read tables in memory - dataframes
    df1 = pd.read_csv(base + r1, encoding='latin1', sep=sep)
    df2 = pd.read_csv(base + r2, encoding='latin1', sep=sep)

    s = time.time()
    # perform materialize, and sampling-materialize (with a given % sample size?)
    df_a = dpu.join_ab_on_key(df1, df2, a1, a2, suffix_str='_x')
    e = time.time()
    print("Join: " + str((e-s)))

    # force gc
    import gc
    df_a = None
    gc.collect()
    time.sleep(15)

    s = time.time()
    # sampling
    sample_size = 1000
    l, r = dpu.apply_consistent_sample(df1, df2, a1, a2, sample_size)
    df_b = dpu.join_ab_on_key(l, r, a1, a2, normalize=False, suffix_str='_x')
    e = time.time()
    print("s-Join: " + str((e - s)))

    return


if __name__ == "__main__":
    print("DoD evaluation")

    # eval_sampling_join()
    # exit()

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

    query_view_definitions_chembl = [
        ("qv1", ['assay_test_type', 'assay_category', 'journal', 'year', 'volume'],
         ['', '', '', '', '']),
        ("qv2", ['accession', 'sequence', 'organism', 'start_position', 'end_position'],
         ['O09028', '', 'Rattus norvegicus', '', '']),
        ("qv3", ['ref_type', 'ref_url', 'enzyme_name', 'organism'],
         ['', '', '', '']),
        ("qv4", ['hba', 'hbd', 'parenteral', 'topical'],
         ['', '', '', '']),
        ("qv5", ['accession', 'sequence', 'organism', 'start_position', 'end_position'],
         ['', '', '', '', ''])
    ]

    # Configure DoD
    # path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/mitdwh/"
    path_to_serialized_model = "/Users/ra-mit/development/discovery_proto/models/chembl_and_drugcentral/"
    # sep = ","
    sep = ";"
    store_client = StoreHandler()
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    dod = DoD(network=network, store_client=store_client, csv_separator=sep)

    # 0- Assemble views for query views. To have raw number of views
    # assemble_views()
    #
    # exit()

    # 1- measure dod performance
    # qv_name, qv_attr, qv_values = query_view_definitions_many[2]
    # print(qv_name)
    # print(qv_attr)
    # print(qv_values)
    # measure_dod_performance(qv_name, qv_attr, qv_values)

    # 1.5- then have a way for calling 4c on each folder -- on all folders. To compare savings (create strategy here)
    path = "dod_evaluation/vassembly/chembl/qv5/"
    groups_per_column_cardinality, schema_id_info = run_4c(path)
    import pickle
    with open("./tmp-4c-serial", 'wb') as f:
        pickle.dump(groups_per_column_cardinality, f)
        pickle.dump(schema_id_info, f)
    # with open("./tmp-4c-serial", 'rb') as f:
    #     groups_per_column_cardinality = pickle.load(f)
    #     schema_id_info = pickle.load(f)

    # print("!!!")
    # for k, v in groups_per_column_cardinality.items():
    #     print(k)
    #     compatible_groups = v['compatible']
    #     contained_groups = v['contained']
    #     complementary_group = v['complementary']
    #     contradictory_group = v['contradictory']
    #     print("Compatible: " + str(len(compatible_groups)))
    #     print("Contained: " + str(len(contained_groups)))
    #     print("Complementary: " + str(len(complementary_group)))
    #     print("Contradictory: " + str(len(contradictory_group)))
    # print("!!!")

    # output_4c_results(groups_per_column_cardinality)
    # print("")
    # print("")
    # print("PRUNING...")
    # print("")
    # print("")
    pruned_groups_per_column_cardinality, human_selection = brancher(groups_per_column_cardinality)
    #
    # print("!!!")
    # for k, v in pruned_groups_per_column_cardinality.items():
    #     print(k)
    #     compatible_groups = v['compatible']
    #     contained_groups = v['contained']
    #     complementary_group = v['complementary']
    #     contradictory_group = v['contradictory']
    #     print("Compatible: " + str(len(compatible_groups)))
    #     print("Contained: " + str(len(contained_groups)))
    #     print("Complementary: " + str(len(complementary_group)))
    #     print("Contradictory: " + str(len(contradictory_group)))
    # print("!!!")
    #
    i_per_group = summarize_4c_output(pruned_groups_per_column_cardinality, schema_id_info)
    # #
    # # print("Pruned!!!")
    # # pp.pprint(pruned_groups_per_column_cardinality)
    print("Total interactions: " + str(sorted(i_per_group, key=lambda x: x[0], reverse=True)))
    print("+ human selections: " + str(human_selection))
    exit()

    # 2- 4c efficienty
    # 2.1- with many views to show advantage with respect to other less sophisticated baselines
    # 2.2- with few views to show that the overhead it adds is negligible
    # path1 = "dod_evaluation/vassembly/many/qv4/"
    # path2 = "dod_evaluation/vassembly/many/qv2-50/"
    # path3 = "dod_evaluation/vassembly/many/qv5/"
    # path4 = "dod_evaluation/vassembly/few/qv1/"
    # path5 = "dod_evaluation/vassembly/few/qv3/"
    # # compare_4c_baselines(many_views=[('9', path1), ('177', path2), ('99', path3)],
    # #                      few_views=[('2', path4), ('2', path5)])

    path = "dod_evaluation/vassembly/many/qv5/"
    compare_4c_baselines(many_views=[('12', path)],
                         few_views=[])

    # 3- Measure average time per join attempt. Add total times as well


