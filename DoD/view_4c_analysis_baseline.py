from os import listdir
from os.path import isfile, join
from collections import defaultdict
import pandas as pd
from pandas.util import hash_pandas_object
from DoD import material_view_analysis as mva


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
        dfs.append((df, f))
    return dfs


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


def brute_force_4c(dataframes_with_metadata):

    # sort relations by cardinality to avoid reverse containment
    dataframes_with_metadata = sorted(dataframes_with_metadata, key=lambda x: len(x[0]), reverse=True)

    summarized_group = list()
    candidate_complementary_group = list()
    complementary_group = list()
    contradictory_group = list()

    t_to_remove = set()
    for t1, path1, md1 in dataframes_with_metadata:
        hashes1 = hash_pandas_object(t1)
        ht1 = hashes1.sum()
        if ht1 in t_to_remove:
            continue
        for t2, path2, md2 in dataframes_with_metadata:
            if path1 == path2:  # same table
                continue
            hashes2 = hash_pandas_object(t2)
            ht2 = hashes2.sum()
            # if t2 is in remove group
            if path2 in t_to_remove:
                continue

            # are views are compatible
            if ht1 == ht2:
                t_to_remove.add(path2)
            # are views potentially contained
            elif len(hashes1) > len(hashes2):
                # are views contained
                if len(set(hashes2) - set(hashes1)) == 0:
                    t_to_remove.add(path2)
            else:
                # Verify that views are potentially complementary
                s1 = set(hashes1)
                s2 = set(hashes2)

                s12 = (s1 - s2)
                s1_complement = set()
                if len(s12) > 0:
                    s1_complement.update((s12))
                s21 = (s2 - s1)
                s2_complement = set()
                if len(s21) > 0:
                    s2_complement.update((s21))
                if len(s1_complement) > 0 or len(s2_complement) > 0:
                    idx1 = [idx for idx, value in enumerate(hashes1) if value in s1_complement]
                    idx2 = [idx for idx, value in enumerate(hashes2) if value in s2_complement]
                    candidate_complementary_group.append((t1, md1, path1, idx1, t2, md2, path2, idx2))

    # now we'd check contradictory
    for t1, md1, path1, idx1, t2, md2, path2, idx2 in candidate_complementary_group:
        selection1 = t1.iloc[idx1]
        selection2 = t2.iloc[idx2]
        mlk1 = sorted(md1.items(), key=lambda x: x[1], reverse=True)
        mlk2 = sorted(md2.items(), key=lambda x: x[1], reverse=True)
        candidate_k1 = mlk1[0]
        candidate_k2 = mlk2[0]
        # pick only one key so we make sure the group-by is compatible
        if candidate_k1 >= candidate_k2:
            k = candidate_k1
        else:
            k = candidate_k2

        complementary_key1 = set()
        complementary_key2 = set()
        contradictory_key1 = set()
        contradictory_key2 = set()

        s1 = selection1[k]
        s2 = set(selection2[k])
        for key in s1:
            for c in selection1.columns:
                cell_value1 = selection1[k == key][c]
                if key in s2:
                    cell_value2 = selection2[k == key][c]
                    if cell_value1 != cell_value2:
                        contradictory_key1.add(key)
                else:
                    complementary_key1.add(key)
        s2 = set(selection2[k]) - set(s1)  # we only check the set difference to save some lookups
        s1 = set(selection1[k])
        for key in s2:
            for c in selection2.columns:
                cell_value2 = selection2[k == key][c]
                if key in s1:
                    cell_value1 = selection1[k == key][c]
                    if cell_value2 != cell_value1:
                        contradictory_key2.add(key)
                else:
                    complementary_key2.add(key)
        if len(contradictory_key1) > 0:
            for ck1 in contradictory_key1:
                contradictory_group.append((path1, k, ck1, path2))
        if len(contradictory_key2) > 0:
            for ck2 in contradictory_key2:
                contradictory_group.append((path2, k, ck2, path1))
        if len(contradictory_key1) == 0 and len(contradictory_key2) == 0:
            for cmk1 in complementary_key1:
                complementary_group.append((path1, k, cmk1))
            for cmk2 in complementary_key2:
                complementary_group.append((path2, k, cmk2))

    # summarize out contained and compatible views
    for t, path, md in dataframes_with_metadata:
        if path not in t_to_remove:
            summarized_group.append(path)
    return summarized_group, complementary_group, contradictory_group


def brute_force_4c_valuewise(dataframes_with_metadata):

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


def classify_per_column_cardinality(dataframes):
    per_column_cardinality = defaultdict(list)
    for df, path in dataframes:
        num_col = len(df.columns)
        per_column_cardinality[num_col].append((df, path))
    return per_column_cardinality


def get_df_metadata(dfs):
    dfs_with_metadata = []
    for df, path in dfs:
        metadata = mva.uniqueness(df)
        dfs_with_metadata.append((df, path, metadata))
    return dfs_with_metadata


if __name__ == "__main__":
    print("View 4C Analysis - Baseline")

    input_directory = "/Users/ra-mit/development/discovery_proto/data/dod/mitdwh/two/"

    dfs = get_dataframes(input_directory)
    print("Found " + str(len(dfs)) + " tables")

    dfs_per_column_cardinality = classify_per_column_cardinality(dfs)

    for key, group_dfs in dfs_per_column_cardinality.items():
        print("Num elements with: " + str(key) + " columns: " + str(len(group_dfs)))
        dfs_with_metadata = get_df_metadata(group_dfs)

        summarized_group, complementary_group, contradictory_group = brute_force_4c(dfs_with_metadata)

    # group by how well they fulfill definition - and maybe cleanliness




## old way of checking complementary -> not debugged, robust to syntactic caps

# if len(t1) == len(t2):
#     are_compatible = True
#     for c in t1.columns:
#         s1 = t1[c].apply(lambda x: str(x).lower()).sort_values().reset_index(drop=True)
#         s2 = t2[c].apply(lambda x: str(x).lower()).sort_values().reset_index(drop=True)
#         idx = (s1 == s2)
#         if not idx.all():
#             are_compatible = False
#             break
#     if are_compatible:
#         t_to_remove.add(hash_pandas_object(t2).sum())
#     else:
#         # They are not compatible, they may be contradictory
#         contradictory, contradictions = are_views_contradictory(t1, t2, md1, md2)
#         if contradictory:
#             contradictory_group.add((path1, path2, contradictions))
#         else:
#             # not contradictory, are they complementary?
#             complementary, complement = are_they_complementary(t1, t2, md1, md2)
#             if complementary:
#                 complementary_group.add((path1, path2, complement))
