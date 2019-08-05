from os import listdir
from os.path import isfile, join
from collections import defaultdict
import pandas as pd
from pandas.util import hash_pandas_object
from DoD import material_view_analysis as mva

from tqdm import tqdm


def normalize(df):
    for c in df.columns:
        df[c].apply(lambda x: str(x).lower())
    return df


def get_dataframes(path):
    files = [path + "/" + f for f in listdir(path) if isfile(join(path, f)) and f != '.DS_Store']
    dfs = []
    for f in files:
        df = pd.read_csv(f, encoding='latin1')
        df = mva.curate_view(df)
        df = normalize(df)
        if len(df) > 0:  # only append valid df
            dfs.append((df, f))
    return dfs


def identify_compatible_groups(dataframes_with_metadata):
    already_classified = set()
    compatible_groups = []

    for t1, path1, md1 in dataframes_with_metadata:
        # these local variables are for this one view
        compatible_group = [path1]
        hashes1 = hash_pandas_object(t1, index=False)
        ht1 = hashes1.sum()
        if path1 in already_classified:
            continue
        for t2, path2, md2 in dataframes_with_metadata:
            if path1 == path2:  # same table
                continue
            # if t2 is in remove group
            if path2 in already_classified:
                continue
            hashes2 = hash_pandas_object(t2, index=False)
            ht2 = hashes2.sum()

            # are views compatible
            if ht1 == ht2:
                compatible_group.append(path2)
                already_classified.add(path1)
                already_classified.add(path2)
        # if len(compatible_group) > 1:
        #  cannot check this condition because now all views are analyzed from compatible groups
        compatible_groups.append(compatible_group)
    return compatible_groups


def summarize_views_and_find_candidate_complementary(dataframes_with_metadata):
    already_processed_complementary_pairs = set()

    contained_groups = []
    candidate_complementary_groups = []

    for df1, path1, md1 in dataframes_with_metadata:
        # these local variables are for this one view
        contained_group = [path1]

        hashes1_list = hash_pandas_object(df1, index=False)  # we only consider content
        hashes1_set = set(hashes1_list)
        for df2, path2, md2 in dataframes_with_metadata:
            if path1 == path2:  # same table
                continue
            hashes2_list = hash_pandas_object(df2, index=False)
            hashes2_set = set(hashes2_list)
            # are views potentially contained
            if len(hashes1_set) > len(hashes2_set):
                # is t2 contained in t1?
                if len(hashes2_set - hashes1_set) == 0:
                    contained_group.append(path2)
            else:
                if (path1 + "%%%" + path2) in already_processed_complementary_pairs\
                        or (path2 + "%%%" + path1) in already_processed_complementary_pairs:
                    continue  # already processed, skip computation
                # Verify that views are potentially complementary
                s12 = (hashes1_set - hashes2_set)
                s1_complement = set()
                if len(s12) > 0:
                    s1_complement.update((s12))
                s21 = (hashes2_set - hashes1_set)
                s2_complement = set()
                if len(s21) > 0:
                    s2_complement.update((s21))
                if len(s1_complement) > 0 and len(s2_complement) > 0:  # and, otherwise it's a containment rel
                    idx1 = [idx for idx, value in enumerate(hashes1_list) if value in s1_complement]
                    idx2 = [idx for idx, value in enumerate(hashes2_list) if value in s2_complement]
                    candidate_complementary_groups.append((df1, md1, path1, idx1, df2, md2, path2, idx2))
                    already_processed_complementary_pairs.add((path1 + "%%%" + path2))
                    already_processed_complementary_pairs.add((path2 + "%%%" + path1))
        if len(contained_group) > 1:
            contained_groups.append(contained_group)
    return contained_groups, candidate_complementary_groups


def pick_most_likely_key_of_pair(md1, md2):
    most_likely_key1 = sorted(md1.items(), key=lambda x: x[1], reverse=True)
    most_likely_key2 = sorted(md2.items(), key=lambda x: x[1], reverse=True)
    candidate_k1 = most_likely_key1[0]
    candidate_k2 = most_likely_key2[0]
    # pick only one key so we make sure the group-by is compatible
    if candidate_k1 >= candidate_k2:
        k = candidate_k1[0]
    else:
        k = candidate_k2[0]
    return k


def find_contradiction_pair(t1, idx1, t2, idx2, k):
    complementary_key1 = set()
    complementary_key2 = set()
    contradictory_key1 = set()
    contradictory_key2 = set()

    selection1 = t1.iloc[idx1]
    selection2 = t2.iloc[idx2]

    s1 = selection1[k]
    s2 = set(selection2[k])
    for key in tqdm(s1):
        if len(contradictory_key1) > 0:  # check this condition for early skip
            break
        if key in s2:
            for c in selection1.columns:
                cell_value1 = set(selection1[selection1[k] == key][c])
                cell_value2 = set(selection2[selection2[k] == key][c])
                if len(cell_value1 - cell_value2) != 0:
                    contradictory_key1.add(key)
                    break  # one contradictory example is sufficient
        else:
            complementary_key1.add(key)
    if len(contradictory_key1) == 0:  # if we found a contradictory example, no need to go on
        s2 = set(selection2[k]) - set(s1)  # we only check the set difference to save some lookups
        s1 = set(selection1[k])
        for key in tqdm(s2):
            if len(contradictory_key2) > 0:  # check this condition for early skip
                break
            if key in s1:
                for c in selection2.columns:
                    cell_value2 = set(selection2[selection2[k] == key][c])
                    cell_value1 = set(selection1[selection1[k] == key][c])
                    if len(cell_value2 - cell_value1) != 0:
                        contradictory_key2.add(key)
                        break
            else:
                complementary_key2.add(key)
    return complementary_key1, complementary_key2, contradictory_key1, contradictory_key2


def tell_contradictory_and_complementary_allpairs(candidate_complementary_group, t_to_remove):
    complementary_group = list()
    contradictory_group = list()

    contradictory_pairs = set()
    complementary_pairs = set()

    for t1, md1, path1, idx1, t2, md2, path2, idx2 in tqdm(candidate_complementary_group):
        if path1 in t_to_remove or path2 in t_to_remove:
            continue  # this will be removed, no need to worry about them

        k = pick_most_likely_key_of_pair(md1, md2)

        complementary_key1 = set()
        complementary_key2 = set()
        contradictory_key1 = set()
        contradictory_key2 = set()

        selection1 = t1.iloc[idx1]
        selection2 = t2.iloc[idx2]

        s1 = selection1[k]
        s2 = set(selection2[k])
        for key in s1:
            if len(contradictory_key1) > 0:  # check this condition for early skip
                break
            for c in selection1.columns:
                cell_value1 = set(selection1[selection1[k] == key][c])
                if key in s2:
                    cell_value2 = set(selection2[selection2[k] == key][c])
                    if len(cell_value1 - cell_value2) != 0:
                        contradictory_key1.add(key)
                        break  # one contradictory example is sufficient
                else:
                    complementary_key1.add(key)
        if len(contradictory_key1) == 0:  # if we found a contradictory example, no need to go on
            s2 = set(selection2[k]) - set(s1)  # we only check the set difference to save some lookups
            s1 = set(selection1[k])
            for key in s2:
                if len(contradictory_key2) > 0:  # check this condition for early skip
                    break
                for c in selection2.columns:
                    cell_value2 = set(selection2[selection2[k] == key][c])
                    if key in s1:
                        cell_value1 = set(selection1[selection1[k] == key][c])
                        if len(cell_value2 - cell_value1) != 0:
                            contradictory_key2.add(key)
                            break
                    else:
                        complementary_key2.add(key)
        if len(contradictory_key1) > 0:
            contradictory_group.append((path1, k, contradictory_key1, path2))
            contradictory_pairs.add(path1 + "%$%" + path2)
            contradictory_pairs.add(path2 + "%$%" + path1)
        if len(contradictory_key2) > 0:
            contradictory_group.append((path2, k, contradictory_key2, path1))
            contradictory_pairs.add(path1 + "%$%" + path2)
            contradictory_pairs.add(path2 + "%$%" + path1)
        if len(contradictory_key1) == 0 and len(contradictory_key2) == 0:
            if path1 + "%$%" + path2 in contradictory_pairs or path2 + "%$%" + path1 in contradictory_pairs \
                    or path1 + "%$%" + path2 in complementary_pairs or path2 + "%$%" + path1 in complementary_pairs:
                continue
            else:
                complementary_group.append((path1, path2, complementary_key1, complementary_key2))
                complementary_pairs.add(path1 + "%$%" + path2)
                complementary_pairs.add(path2 + "%$%" + path1)
    return complementary_group, contradictory_group


def tell_contradictory_and_complementary_chasing(candidate_complementary_group, t_to_remove):
    complementary_group = list()
    contradictory_group = list()

    contradictory_pairs = set()
    complementary_pairs = set()

    graph = defaultdict(dict)

    # create undirected graph
    for df1, md1, path1, idx1, df2, md2, path2, idx2 in tqdm(candidate_complementary_group):
        # if the view is gonna be summarized, then there's no need to check this one either. Not in graph
        if path1 in t_to_remove or path2 in t_to_remove:
            continue  # this will be removed, no need to worry about them
        graph[path1][path2] = (df1, md1, path1, idx1, df2, md2, path2, idx2)
        graph[path2][path1] = (df1, md1, path1, idx1, df2, md2, path2, idx2)

    there_are_unexplored_pairs = True

    marked_nodes = set()

    while there_are_unexplored_pairs:

        while len(marked_nodes) > 0:

            marked_node = marked_nodes.pop()
            # print(marked_node)
            path, k_attr_name, contradictory_key = marked_node  # pop on insertion
            # contradictory_key = contradictory_keys.pop()

            neighbors_graph = graph[path]
            # chase all neighbors of involved node
            for neighbor_k, neighbor_v in neighbors_graph.items():
                df1, md1, path1, idx1, df2, md2, path2, idx2 = neighbor_v
                # skip already processed pairs
                if path1 + "%$%" + path2 in contradictory_pairs or path2 + "%$%" + path1 in contradictory_pairs\
                        or path1 + "%$%" + path2 in complementary_pairs or path2 + "%$%" + path1 in complementary_pairs:
                    continue
                selection1 = df1.iloc[idx1]
                selection2 = df2.iloc[idx2]
                for c in selection1.columns:
                    cell_value1 = set(selection1[selection1[k_attr_name] == contradictory_key][c])
                    cell_value2 = set(selection2[selection2[k_attr_name] == contradictory_key][c])
                    if len(cell_value1) > 0 and len(cell_value2) > 0 and len(cell_value1 - cell_value2) != 0:
                        contradictory_group.append((path1, k_attr_name, contradictory_key, path2))
                        contradictory_pairs.add(path1 + "%$%" + path2)
                        contradictory_pairs.add(path2 + "%$%" + path1)
                        marked_nodes.add((neighbor_k, k_attr_name, contradictory_key))
                        break  # one contradiction is enough to move on, no need to check other columns

        # At this point all marked nodes are processed. If there are no more candidate pairs, then we're done
        if len(candidate_complementary_group) == 0:
            there_are_unexplored_pairs = False
            break

        # TODO: pick any pair (later refine hwo to choose this, e.g., pick small cardinality one)
        df1, md1, path1, idx1, df2, md2, path2, idx2 = candidate_complementary_group.pop()  # random pair
        # check we havent processed this pair yet -- we may have done it while chasing from marked_nodes
        if path1 + "%$%" + path2 in contradictory_pairs or path2 + "%$%" + path1 in contradictory_pairs \
                or path1 + "%$%" + path2 in complementary_pairs or path2 + "%$%" + path1 in complementary_pairs:
            continue

        # find contradiction in pair (if not put in complementary group and choose next pair)
        k = pick_most_likely_key_of_pair(md1, md2)
        complementary_key1, complementary_key2, \
        contradictory_key1, contradictory_key2 = find_contradiction_pair(df1, idx1, df2, idx2, k)

        # if contradiction found, mark keys and nodes of graph
        if len(contradictory_key1) > 0:
            # tuple is: (path1: name of table, k: attribute_name, contradictory_key: set of contradictory keys)
            contr_key1 = contradictory_key1.pop()
            marked_nodes.add((path1, k, contr_key1))
        if len(contradictory_key2) > 0:
            contr_key2 = contradictory_key2.pop()
            marked_nodes.add((path2, k, contr_key2))

        # record the classification between complementary/contradictory of this iteration
        if len(contradictory_key1) > 0:
            contradictory_group.append((path1, k, contradictory_key1, path2))
            contradictory_pairs.add(path1 + "%$%" + path2)
            contradictory_pairs.add(path2 + "%$%" + path1)
        if len(contradictory_key2) > 0:
            contradictory_group.append((path2, k, contradictory_key2, path1))
            contradictory_pairs.add(path1 + "%$%" + path2)
            contradictory_pairs.add(path2 + "%$%" + path1)
        if len(contradictory_key1) == 0 and len(contradictory_key2) == 0:
            if path1 + "%$%" + path2 in contradictory_pairs or path2 + "%$%" + path1 in contradictory_pairs:
                continue
            else:
                complementary_group.append((path1, path2, complementary_key1, complementary_key2))
                complementary_pairs.add(path1 + "%$%" + path2)
                complementary_pairs.add(path2 + "%$%" + path1)

    return complementary_group, contradictory_group


def chasing_4c(dataframes_with_metadata):

    # sort relations by cardinality to avoid reverse containment
    # (df, path, metadata)
    dataframes_with_metadata = sorted(dataframes_with_metadata, key=lambda x: len(x[0]), reverse=True)

    compatible_groups = identify_compatible_groups(dataframes_with_metadata)
    # We pick one representative from each compatible group
    selection = set([x[0] for x in compatible_groups])
    dataframes_with_metadata_selected = [(df, path, metadata) for df, path, metadata in dataframes_with_metadata
                                         if path in selection]

    contained_groups, candidate_complementary_group = \
        summarize_views_and_find_candidate_complementary(dataframes_with_metadata_selected)

    # complementary_group, contradictory_group = \
    #     tell_contradictory_and_complementary_allpairs(candidate_complementary_group, t_to_remove)
    t_to_remove = set()

    complementary_group, contradictory_group = \
        tell_contradictory_and_complementary_chasing(candidate_complementary_group, t_to_remove)

    # prepare found groups for presentation
    compatible_groups = [cg for cg in compatible_groups if len(cg) > 1]

    return compatible_groups, contained_groups, complementary_group, contradictory_group


def no_chasing_4c(dataframes_with_metadata):

    # sort relations by cardinality to avoid reverse containment
    # (df, path, metadata)
    dataframes_with_metadata = sorted(dataframes_with_metadata, key=lambda x: len(x[0]), reverse=True)

    compatible_groups = identify_compatible_groups(dataframes_with_metadata)
    # We pick one representative from each compatible group
    selection = set([x[0] for x in compatible_groups])
    dataframes_with_metadata_selected = [(df, path, metadata) for df, path, metadata in dataframes_with_metadata
                                         if path in selection]

    contained_groups, candidate_complementary_group = \
        summarize_views_and_find_candidate_complementary(dataframes_with_metadata_selected)

    t_to_remove = set()
    complementary_group, contradictory_group = \
        tell_contradictory_and_complementary_allpairs(candidate_complementary_group, t_to_remove)

    # complementary_group, contradictory_group = \
    #     tell_contradictory_and_complementary_chasing(candidate_complementary_group, t_to_remove)

    # prepare found groups for presentation
    compatible_groups = [cg for cg in compatible_groups if len(cg) > 1]

    return compatible_groups, contained_groups, complementary_group, contradictory_group


def brute_force_4c_valuewise(dataframes_with_metadata):
    def are_views_contradictory(t1, t2, md1, md2):
        mlk1 = sorted(md1.items(), key=lambda x: x[1], reverse=True)
        mlk2 = sorted(md2.items(), key=lambda x: x[1], reverse=True)
        candidate_k1 = mlk1[0]
        candidate_k2 = mlk2[0]
        # pick only one key so we make sure the group-by is compatible
        if candidate_k1 >= candidate_k2:
            k = candidate_k1
        else:
            k = candidate_k2
        vg1 = t1.groupby([k[0]])
        vg2 = t2.groupby([k[0]])
        if len(vg1.groups) > len(vg2.groups):
            vref = vg1
            voth = vg2
        else:
            vref = vg2
            voth = vg1
        contradictions = []
        for gn, gv in vref:
            if gn not in voth:  # this cannot be accounted for contradiction -> could be complementary
                continue
            v = voth.get_group(gn)
            are_equivalent, equivalency_type = mva.equivalent(gv, v)  # local group
            if not are_equivalent:
                contradictions.append((k, gn))
                break # one contradiction is enough to stop
        if len(contradictions) != 0:
            return True, contradictions
        return False, []

    def are_they_complementary(t1, t2, md1, md2):
        mlk1 = sorted(md1.items(), key=lambda x: x[1], reverse=True)
        mlk2 = sorted(md2.items(), key=lambda x: x[1], reverse=True)
        k1 = mlk1[0]
        k2 = mlk2[0]
        s1 = set(t1[k1[0]])
        s2 = set(t2[k2[0]])
        s12 = (s1 - s2)
        sdiff = set()
        if len(s12) > 0:
            sdiff.update((s12))
        s21 = (s2 - s1)
        if len(s21) > 0:
            sdiff.update((s21))
        if len(sdiff) == 0:
            return False, set([])
        return True, sdiff

    summarized_group = list()
    complementary_group = list()
    contradictory_group = list()

    t_to_remove = set()

    for t1, path1, md1 in dataframes_with_metadata:
        # let's first detect whether this view is going to be removed
        ht1 = hash_pandas_object(t1).sum()
        if ht1 in t_to_remove:
            continue
        for t2, path2, md2 in dataframes_with_metadata:
            if path1 == path2:  # same table
                continue
            ht2 = hash_pandas_object(t2).sum()
            # if t2 is in remove group
            if path2 in t_to_remove:
                continue
            # Are they compatible
            if ht1 == ht2:
                t_to_remove.add(path2)

            # t2 smaller -> it may be contained in t1
            elif len(t1) > len(t2):
                is_contained = True
                for c in t1.columns:
                    small_set = t2[c].apply(lambda x: str(x).lower())
                    large_set = t1[c].apply(lambda x: str(x).lower())
                    dif = set(small_set) - set(large_set)
                    if len(dif) != 0:
                        is_contained = False
                        break
                if is_contained:
                    t_to_remove.add(path2)
            else:
                # Detect if views are contradictory...
                contradictory, contradictions = are_views_contradictory(t1, t2, md1, md2)
                if contradictory:
                    contradictory_group.append((path1, path2, contradictions))
                else:
                    # not contradictory, are they complementary?
                    complementary, complement = are_they_complementary(t1, t2, md1, md2)
                    if complementary:
                        complementary_group.append((path1, path2, complement))

    # summarize out contained and compatible views
    for t, path, md in dataframes_with_metadata:
        if path not in t_to_remove:
            summarized_group.append(path)
    return summarized_group, complementary_group, contradictory_group


def classify_per_table_schema(dataframes):
    """
    Two schemas are the same if they have the exact same number and type of columns
    :param dataframes:
    :return:
    """
    schema_id_info = dict()
    schema_to_dataframes = defaultdict(list)
    for df, path in dataframes:
        the_hashes = [hash(el) for el in df.columns]
        schema_id = sum(the_hashes)
        schema_id_info[schema_id] = len(the_hashes)
        schema_to_dataframes[schema_id].append((df, path))
    return schema_to_dataframes, schema_id_info


def get_df_metadata(dfs):
    dfs_with_metadata = []
    for df, path in dfs:
        metadata = mva.uniqueness(df)
        dfs_with_metadata.append((df, path, metadata))
    return dfs_with_metadata


def main(input_path):
    groups_per_column_cardinality = defaultdict(dict)

    dfs = get_dataframes(input_path)
    print("Found " + str(len(dfs)) + " valid tables")

    dfs_per_schema, schema_id_info = classify_per_table_schema(dfs)
    print("View candidates classify into " + str(len(dfs_per_schema)) + " groups based on schema")
    print("")
    for key, group_dfs in dfs_per_schema.items():
        print("Num elements with schema " + str(key) + " is: " + str(len(group_dfs)))
        dfs_with_metadata = get_df_metadata(group_dfs)

        # summarized_group, complementary_group, contradictory_group = brute_force_4c(dfs_with_metadata)
        compatible_group, contained_group, complementary_group, contradictory_group = chasing_4c(dfs_with_metadata)
        groups_per_column_cardinality[key]['compatible'] = compatible_group
        groups_per_column_cardinality[key]['contained'] = contained_group
        groups_per_column_cardinality[key]['complementary'] = complementary_group
        groups_per_column_cardinality[key]['contradictory'] = contradictory_group

    return groups_per_column_cardinality, schema_id_info


def nochasing_main(input_path):
    groups_per_column_cardinality = defaultdict(dict)

    dfs = get_dataframes(input_path)
    print("Found " + str(len(dfs)) + " valid tables")

    dfs_per_schema, schema_id_info = classify_per_table_schema(dfs)
    print("View candidates classify into " + str(len(dfs_per_schema)) + " groups based on schema")
    print("")
    for key, group_dfs in dfs_per_schema.items():
        print("Num elements with schema " + str(key) + " is: " + str(len(group_dfs)))
        dfs_with_metadata = get_df_metadata(group_dfs)

        # summarized_group, complementary_group, contradictory_group = brute_force_4c(dfs_with_metadata)
        compatible_group, contained_group, complementary_group, contradictory_group = no_chasing_4c(dfs_with_metadata)
        groups_per_column_cardinality[key]['compatible'] = compatible_group
        groups_per_column_cardinality[key]['contained'] = contained_group
        groups_per_column_cardinality[key]['complementary'] = complementary_group
        groups_per_column_cardinality[key]['contradictory'] = contradictory_group

    return groups_per_column_cardinality


def valuewise_main(input_path):
    groups_per_column_cardinality = defaultdict(dict)

    dfs = get_dataframes(input_path)
    print("Found " + str(len(dfs)) + " valid tables")

    dfs_per_schema = classify_per_table_schema(dfs)
    print("View candidates classify into " + str(len(dfs_per_schema)) + " groups based on schema")
    print("")
    for key, group_dfs in dfs_per_schema.items():
        print("Num elements with schema " + str(key) + " is: " + str(len(group_dfs)))
        dfs_with_metadata = get_df_metadata(group_dfs)

        # summarized_group, complementary_group, contradictory_group = brute_force_4c(dfs_with_metadata)
        summarized_group, complementary_group, contradictory_group = brute_force_4c_valuewise(dfs_with_metadata)
        # groups_per_column_cardinality[key]['compatible'] = compatible_group
        # groups_per_column_cardinality[key]['contained'] = contained_group
        # groups_per_column_cardinality[key]['complementary'] = complementary_group
        # groups_per_column_cardinality[key]['contradictory'] = contradictory_group

    return groups_per_column_cardinality

if __name__ == "__main__":
    print("View 4C Analysis - Baseline")

    # SNIPPET of CODE TO FIGURE OUT HASH ERROR BECAUSE USE OF INDEX
    # import pandas as pd
    #
    # df7 = pd.read_csv("/Users/ra-mit/development/discovery_proto/DoD/dod_evaluation/vassembly/many/qv2/view_7",
    #                   encoding='latin1')
    # df21 = pd.read_csv("/Users/ra-mit/development/discovery_proto/DoD/dod_evaluation/vassembly/many/qv2/view_21",
    #                    encoding='latin1')
    # dfs_with_metadata = get_df_metadata([(df7, 'path7'), (df21, 'path21')])
    # compatible_groups = identify_compatible_groups(dfs_with_metadata)
    # contained_groups, candidate_complementary_group = \
    #     summarize_views_and_find_candidate_complementary(dfs_with_metadata)
    # exit()

    # input_path = "/Users/ra-mit/development/discovery_proto/data/dod/mitdwh/two/"
    input_path = "/Users/ra-mit/development/discovery_proto/data/dod/test/"

    groups_per_column_cardinality = main(input_path)

    for k, v in groups_per_column_cardinality.items():
        compatible_group = v['compatible']
        contained_group = v['contained']
        complementary_group = v['complementary']
        contradictory_group = v['contradictory']

        print("Compatible groups: " + str(len(compatible_group)))
        print("Contained groups: " + str(len(contained_group)))
        print("Complementary views: " + str(len(complementary_group)))
        print("Contradictory views: " + str(len(contradictory_group)))

        print("Compatible groups:")
        for group in compatible_group:
            print(group)

        print("Contained groups:")
        for group in contained_group:
            print(group)

        print("Complementary views: ")
        for path1, path2, _, _ in complementary_group:
            print(path1 + " - " + path2)

        print("Contradictory views: ")
        for path1, key_column, key_value, path2 in contradictory_group:
            df1 = pd.read_csv(path1)
            df2 = pd.read_csv(path2)
            row1 = df1[df1[key_column] == key_value]
            row2 = df2[df2[key_column] == key_value]
            print(path1 + " - " + path2)
            print("ROW - 1")
            print(row1)
            print("ROW - 2")
            print(row2)
            print("")
            print("")

        # analyzing contradictory views:
        mapping = defaultdict(list)
        for path1, _, _, path2 in contradictory_group:
            mapping[path1].append(path2)
            mapping[path2].append(path1)
        for k, v in mapping.items():
            print(k + " : " + str(len(v)))


