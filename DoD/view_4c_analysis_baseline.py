from os import listdir
from os.path import isfile, join
import pandas as pd
from DoD import material_view_analysis as mva


def get_dataframes(path):
    files = [f for f in listdir(path) if isfile(join(path, f))]
    dfs = []
    for f in files:
        df = pd.read_csv(f, 'r', encoding='latin1')
        df = mva.curate_view(df)
        dfs.append(df)
    return dfs


def brute_force_4c(dataframes_with_metadata):

    complementary = set()
    contradictory = set()

    for t1, md1 in dataframes_with_metadata:
        t_to_remove = set()
        for t2, md2 in dataframes_with_metadata:

            # cardinality
            if len(t1) == len(t2):

                are_compatible = True
                for c in t1.columns:
                    s1 = t1[c].apply(lambda x: str(x).lower()).sort_values().reset_index(drop=True)
                    s2 = t2[c].apply(lambda x: str(x).lower()).sort_values().reset_index(drop=True)
                    idx = (s1 == s2)
                    if not idx.all():
                        are_compatible = False
                        break
                if are_compatible:
                    t_to_remove.add(t2)
            # t2 smaller -> it may be contained in t1
            if len(t1) > len(t2):

                is_contained = True
                for c in t1.columns:
                    small_set = t2[c].apply(lambda x: str(x).lower())
                    large_set = t1[c].apply(lambda x: str(x).lower())
                    dif = set(small_set) - set(large_set)
                    if len(dif) != 0:
                        is_contained = False
                        break
                    if is_contained:
                        t_to_remove.add(t2)



def get_df_metadata(dfs):
    dfs_with_metadata = []
    for df in dfs:
        metadata = mva.uniqueness(df)
        dfs_with_metadata.append((df, metadata))
    return dfs_with_metadata


if __name__ == "__main__":
    print("View 4C Analysis - Baseline")

    input_directory = "/Users/ra-mit/development/discovery_proto/data/dod/"

    dfs = get_dataframes(input_directory)

    dfs_with_metadata = get_df_metadata(dfs)

    # group by how well they fulfill definition - and maybe cleanliness


