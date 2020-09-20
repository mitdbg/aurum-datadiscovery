import pandas as pd
from collections import defaultdict
import math

from DoD.utils import FilterType
import config as C
import os
import psutil
from tqdm import tqdm
import time
import pprint


pp = pprint.PrettyPrinter(indent=4)
# Cache reading and transformation of DFs
cache = dict()

memory_limit_join_processing = C.memory_limit_join_processing * psutil.virtual_memory().total

data_separator = C.separator

tmp_spill_file = "./tmp_spill_file.tmp"
# tmp_df_chunk = "./chunk_df"


def configure_csv_separator(separator):
    global data_separator
    data_separator = separator


def estimate_output_row_size(a: pd.DataFrame, b: pd.DataFrame):
    # 1. check each dataframe's size in memory and number of rows
    # a_bytes = sum(a.memory_usage(deep=True))
    # b_bytes = sum(b.memory_usage(deep=True))
    a_bytes = sum(a.memory_usage(deep=False))
    b_bytes = sum(b.memory_usage(deep=False))
    a_len_rows = len(a)
    b_len_rows = len(b)

    # 2. estimate size per row from previous
    a_row_size = float(a_bytes/a_len_rows)
    b_row_size = float(b_bytes/b_len_rows)

    # 3. estimate row size of output join (no selections)
    o_row_size = a_row_size + b_row_size

    return o_row_size


def does_join_fit_in_memory(chunk, ratio, o_row_size):
    estimated_output_num_rows = (float)((chunk / ratio))
    estimated_output_size = estimated_output_num_rows * o_row_size
    if estimated_output_size >= memory_limit_join_processing:
        # eos_gb = estimated_output_size / 1024 / 1024 / 1024
        # print("Estimated Output size in GB: " + str(eos_gb))
        return False, estimated_output_size/1024/1024/1024
    return True, estimated_output_size/1024/1024/1024


def join_ab_on_key_optimizer(a: pd.DataFrame, b: pd.DataFrame, a_key: str, b_key: str,
                             suffix_str=None, chunksize=C.join_chunksize, normalize=True):
    # clean up temporal stuff -- i.e., in case there was a crash
    try:
        # os.remove(tmp_df_chunk)
        os.remove(tmp_spill_file)
    except FileNotFoundError:
        pass

    # if normalize:
    a[a_key] = a[a_key].apply(lambda x: str(x).lower())
    try:
        b[b_key] = b[b_key].apply(lambda x: str(x).lower())
    except KeyError:
        print("COLS: " + str(b.columns))
        print("KEY: " + str(b_key))

    # drop NaN/Null values
    a.dropna(subset=[a_key], inplace=True)
    b.dropna(subset=[b_key], inplace=True)
    a_drop_indices = [i for i, el in enumerate(a[a_key]) if el == 'nan' or el == 'null' or el is pd.NaT]
    b_drop_indices = [i for i, el in enumerate(b[b_key]) if el == 'nan' or el == 'null' or el is pd.NaT]
    a.drop(a_drop_indices, inplace=True)
    b.drop(b_drop_indices, inplace=True)
    a.reset_index(drop=True)
    b.reset_index(drop=True)

    if len(a) == 0 or len(b) == 0:
        return False

    # Estimate output join row size
    o_row_size = estimate_output_row_size(a, b)

    # join by chunks
    def join_chunk(chunk_df, header=False):
        # print("First chunk? : " + str(header))
        # print("a: " + str(len(a)))
        # print("b: " + str(len(chunk_df)))
        # worst_case_estimated_join_size = chunksize * len(a) * o_row_size
        # if worst_case_estimated_join_size >= memory_limit_join_processing:
        #     print("Can't join sample. Size: " + str(worst_case_estimated_join_size))
        #     return False  # can't even join a sample
        # print(a[a_key].head(10))
        # print(chunk_df[b_key].head(10))
        target_chunk = pd.merge(a, chunk_df, left_on=a_key, right_on=b_key, sort=False, suffixes=('', suffix_str))
        if header:  # header is only activated the first time. We only want to do this check the first time
            # sjt = time.time()
            fits, estimated_join_size = does_join_fit_in_memory(len(target_chunk), (float)(chunksize/len(b)), o_row_size)
            # ejt = time.time()
            # join_time = (float)((ejt - sjt) * (float)(len(b)/chunksize))
            # print("Est. join time: " + str(join_time))
            print("Estimated join size: " + str(estimated_join_size))
            # if estimated_join_size < 0.01:
            #     print("TC: " + str(len(target_chunk)))
            #     print("Ratio: " + str((float)(chunksize/len(b))))
            #     print("row size: " + str(o_row_size))
            #     print("FITS? : " + str(fits))
            if fits:
                return True
            else:
                return False
        target_chunk.to_csv(tmp_spill_file, mode="a", header=header, index=False)
        return False

    def chunk_reader(df):
        len_df = len(df)
        init_index = 0
        num_chunks = math.ceil(len_df / chunksize)
        for i in range(num_chunks):
            chunk_df = df[init_index:init_index + chunksize]
            init_index += chunksize
            yield chunk_df

    # swap row order of b to approximate uniform sampling
    b = b.sample(frac=1).reset_index(drop=True)
    first_chunk = True
    all_chunks = [chunk for chunk in chunk_reader(b)]
    # for chunk in tqdm(all_chunks):
    for chunk in all_chunks:
        scp = time.time()
        if first_chunk:
            fits_in_memory = join_chunk(chunk, header=True)
            first_chunk = False
            if fits_in_memory:  # join in memory and exit
                return join_ab_on_key(a, b, a_key, b_key, suffix_str=suffix_str, normalize=False)
            else:  # just ignore no-fit in memory chunks
                return False
        else:
            join_chunk(chunk)
        ecp = time.time()
        chunk_time = ecp - scp
        estimated_total_time = chunk_time * len(all_chunks)
        print("ETT: " + str(estimated_total_time))
        if estimated_total_time > 60 * 3:  # no more than 3 minutes
            return False  # cancel this join without breaking the whole pipeline

    print("Reading written down relation: ")
    # [join_chunk(chunk) for chunk in chunk_reader(b)]
    joined = pd.read_csv(tmp_spill_file, encoding='latin1', sep=data_separator)

    # clean up temporal stuff
    try:
        # os.remove(tmp_df_chunk)
        os.remove(tmp_spill_file)
    except FileNotFoundError:
        pass

    return joined


def join_ab_on_key_spill_disk(a: pd.DataFrame, b: pd.DataFrame, a_key: str, b_key: str, suffix_str=None, chunksize=C.join_chunksize):
    # clean up temporal stuff -- i.e., in case there was a crash
    try:
        # os.remove(tmp_df_chunk)
        os.remove(tmp_spill_file)
    except FileNotFoundError:
        pass

    a[a_key] = a[a_key].apply(lambda x: str(x).lower())
    try:
        b[b_key] = b[b_key].apply(lambda x: str(x).lower())
    except KeyError:
        print("COLS: " + str(b.columns))
        print("KEY: " + str(b_key))

    # Calculate target columns
    # a_columns = set(a.columns)
    # b_columns = pd.Index([column if column not in a_columns else column + suffix_str for column in b.columns])
    #
    # # Write to disk the skeleton of the target
    # df_target = pd.DataFrame(columns=(a.columns.append(b_columns)))
    # df_target.to_csv(tmp_spill_file, index_label=False)

    # join by chunks
    def join_chunk(chunk_df, header=False):
        # chunk_df[b_key] = chunk_df[b_key].apply(lambda x: str(x).lower())  # transform to string for join
        target_chunk = pd.merge(a, chunk_df, left_on=a_key, right_on=b_key, sort=False, suffixes=('', suffix_str))
        target_chunk.to_csv(tmp_spill_file, mode="a", header=header, index=False)

    def chunk_reader(df):
        len_df = len(df)
        init_index = 0
        num_chunks = math.ceil(len_df / chunksize)
        for i in range(num_chunks):
            chunk_df = df[init_index:init_index + chunksize]
            init_index += chunksize
            yield chunk_df

    # b.to_csv(tmp_df_chunk, index_label=False)
    # chunk_reader = pd.read_csv(tmp_df_chunk, encoding='latin1', sep=data_separator, chunksize=chunksize)

    first_chunk = True
    for chunk in chunk_reader(b):
        if first_chunk:
            join_chunk(chunk, header=True)
            first_chunk = False
        else:
            join_chunk(chunk)

    # [join_chunk(chunk) for chunk in chunk_reader(b)]
    joined = pd.read_csv(tmp_spill_file, encoding='latin1', sep=data_separator)

    # clean up temporal stuff
    try:
        # os.remove(tmp_df_chunk)
        os.remove(tmp_spill_file)
    except FileNotFoundError:
        pass

    return joined


def join_ab_on_key(a: pd.DataFrame, b: pd.DataFrame, a_key: str, b_key: str, suffix_str=None, normalize=True):
    if normalize:
        a[a_key] = a[a_key].apply(lambda x: str(x).lower())
        b[b_key] = b[b_key].apply(lambda x: str(x).lower())
    joined = pd.merge(a, b, how='inner', left_on=a_key, right_on=b_key, sort=False, suffixes=('', suffix_str))
    return joined


# def update_relation_cache(relation_path, df):
#     if relation_path in cache:
#         cache[relation_path] = df


def read_relation(relation_path):
    if relation_path in cache:
        df = cache[relation_path]
    else:
        df = pd.read_csv(relation_path, encoding='latin1', sep=data_separator)
        cache[relation_path] = df
    return df


def read_relation_on_copy(relation_path):
    """
    This is assuming than copying a DF is cheaper than reading it back from disk
    :param relation_path:
    :return:
    """
    if relation_path in cache:
        df = cache[relation_path]
    else:
        df = pd.read_csv(relation_path, encoding='latin1', sep=data_separator)
        cache[relation_path] = df
    return df.copy()


def empty_relation_cache():
    global cache
    cache = dict()


def get_dataframe(path):
    # TODO: only csv is supported
    df = pd.read_csv(path, encoding='latin1', sep=data_separator)
    return df



def _join_ab_on_key(a: pd.DataFrame, b: pd.DataFrame, a_key: str, b_key: str, suffix_str=None):
    # First make sure to remove empty/nan values from join columns
    # TODO: Generate data event if nan values are found
    a_valid_index = (a[a_key].dropna()).index
    b_valid_index = (b[b_key].dropna()).index
    a = a.iloc[a_valid_index]
    b = b.iloc[b_valid_index]

    # Normalize join columns
    # a_original = a[a_key].copy()
    # b_original = b[b_key].copy()
    a[a_key] = a[a_key].apply(lambda x: str(x).lower())
    b[b_key] = b[b_key].apply(lambda x: str(x).lower())

    joined = pd.merge(a, b, how='inner', left_on=a_key, right_on=b_key, sort=False, suffixes=('', suffix_str))

    # # Recover format of original columns
    # FIXME: would be great to do this, but it's broken
    # joined[a_key] = a_original
    # joined[b_key] = b_original

    return joined


def apply_filter(relation_path, attribute, cell_value):
    # if relation_path in cache:
    #     df = cache[relation_path]
    # else:
    #     df = pd.read_csv(relation_path, encoding='latin1', sep=data_separator)
    #     # store in cache
    #     cache[relation_path] = df
    df = read_relation_on_copy(relation_path)  # FIXME FIXE FIXME
    # df = get_dataframe(relation_path)
    df[attribute] = df[attribute].apply(lambda x: str(x).lower().strip())
    # update_relation_cache(relation_path, df)
    df = df[df[attribute] == str(cell_value).lower()]
    return df


def find_key_for(relation_path, key, attribute, value):
    """
    select key from relation where attribute = value;
    """
    # normalize this value
    value = str(value).lower()
    # Check if DF in cache
    if relation_path in cache:
        df = cache[relation_path]
    else:
        df = pd.read_csv(relation_path, encoding='latin1', sep=data_separator)
        #df = df.apply(lambda x: x.astype(str).str.lower())
        # cache[relation_path] = df  # cache for later
    try:
        key_value_df = df[df[attribute].map(lambda x: str(x).lower()) == value][[key]]
    except KeyError:
        print("!!!")
        print("Attempt to access attribute: '" + str(attribute) + "' from relation: " + str(df.columns))
        print("Attempt to project attribute: '" + str(key) + "' from relation: " + str(df.columns))
        print("!!!")
    return {x[0] for x in key_value_df.values}


def is_value_in_column(value, relation_path, column):
    # normalize this value
    value = str(value).lower()
    if relation_path in cache:
        df = cache[relation_path]
    else:
        df = pd.read_csv(relation_path, encoding='latin1', sep=data_separator)
        #df = df.apply(lambda x: x.astype(str).str.lower())
        cache[relation_path] = df  # cache for later
    return value in df[column].map(lambda x: str(x).lower()).unique()


def obtain_attributes_to_project(filters):
    attributes_to_project = set()
    for f in filters:
        f_type = f[1].value
        if f_type is FilterType.ATTR.value:
            attributes_to_project.add(f[0][0])
        elif f_type is FilterType.CELL.value:
            attributes_to_project.add(f[0][1])
    return attributes_to_project


def _obtain_attributes_to_project(jp_with_filters):
    filters, jp = jp_with_filters
    attributes_to_project = set()
    for f in filters:
        f_type = f[1].value
        if f_type is FilterType.ATTR.value:
            attributes_to_project.add(f[0][0])
        elif f_type is FilterType.CELL.value:
            attributes_to_project.add(f[0][1])
    return attributes_to_project


def project(df, attributes_to_project):
    print("Project: " + str(attributes_to_project))
    df = df[list(attributes_to_project)]
    return df


class InTreeNode:
    def __init__(self, node):
        self.node = node
        self.parent = None
        self.payload = None

    def add_parent(self, parent):
        self.parent = parent

    def set_payload(self, payload: pd.DataFrame):
        self.payload = payload

    def get_payload(self) -> pd.DataFrame:
        return self.payload

    def get_parent(self):
        return self.parent

    def __hash__(self):
        return hash(self.node)

    def __eq__(self, other):
        # compare with both strings and other nodes
        if type(other) is str:
            return self.node == other
        elif type(other) is InTreeNode:
            return self.node == other.node


def materialize_join_graph(jg, dod):
    print("Materializing:")
    pp.pprint(jg)

    def build_tree(jg):
        # Build in-tree (leaves to root)
        intree = dict()  # keep reference to all nodes here
        leaves = []

        hops = jg

        while len(hops) > 0:
            pending_hops = []  # we use this variable to maintain the temporarily disconnected hops
            for l, r in hops:
                if len(intree) == 0:
                    node = InTreeNode(l.source_name)
                    node_path = dod.aurum_api.helper.get_path_nid(l.nid) + "/" + l.source_name
                    df = read_relation_on_copy(node_path)# FIXME FIXME FIXME
                    # df = get_dataframe(node_path)
                    node.set_payload(df)
                    intree[l.source_name] = node
                    leaves.append(node)
                # now either l or r should be in intree
                if l.source_name in intree.keys():
                    rnode = InTreeNode(r.source_name)  # create node for r
                    node_path = dod.aurum_api.helper.get_path_nid(r.nid) + "/" + r.source_name
                    df = read_relation_on_copy(node_path)# FIXME FIXME FIXME
                    # df = get_dataframe(node_path)
                    rnode.set_payload(df)
                    r_parent = intree[l.source_name]
                    rnode.add_parent(r_parent)  # add ref
                    # r becomes a leave, and l stops being one
                    if r_parent in leaves:
                        leaves.remove(r_parent)
                    leaves.append(rnode)
                    intree[r.source_name] = rnode
                elif r.source_name in intree.keys():
                    lnode = InTreeNode(l.source_name)  # create node for l
                    node_path = dod.aurum_api.helper.get_path_nid(l.nid) + "/" + l.source_name
                    df = read_relation_on_copy(node_path)# FIXME FIXME FIXME
                    # df = get_dataframe(node_path)
                    lnode.set_payload(df)
                    l_parent = intree[r.source_name]
                    lnode.add_parent(l_parent)  # add ref
                    if l_parent in leaves:
                        leaves.remove(l_parent)
                    leaves.append(lnode)
                    intree[l.source_name] = lnode
                else:
                    # temporarily disjoint hop which we store for subsequent iteration
                    pending_hops.append((l, r))
            hops = pending_hops
        return intree, leaves

    def find_l_r_key(l_source_name, r_source_name, jg):
        # print(l_source_name + " -> " + r_source_name)
        for l, r in jg:
            if l.source_name == l_source_name and r.source_name == r_source_name:
                return l.field_name, r.field_name
            elif l.source_name == r_source_name and r.source_name == l_source_name:
                return r.field_name, l.field_name

    intree, leaves = build_tree(jg)
    # find groups of leaves with same common ancestor
    suffix_str = '_x'
    go_on = True
    while go_on:
        if len(leaves) == 1 and leaves[0].get_parent() is None:
            go_on = False
            continue  # we have now converged
        leave_ancestor = defaultdict(list)
        for leave in leaves:
            if leave.get_parent() is not None:  # never add the parent's parent, which does not exist
                leave_ancestor[leave.get_parent()].append(leave)
        # pick ancestor and find its join info with each children, then join, then add itself to leaves (remove others)
        for k, v in leave_ancestor.items():
            for child in v:
                l = k.get_payload()
                r = child.get_payload()
                l_key, r_key = find_l_r_key(k.node, child.node, jg)

                # print("L: " + str(k.node) + " - " + str(l_key) + " size: " + str(len(l)))
                # print("R: " + str(child.node) + " - " + str(r_key) + " size: " + str(len(r)))

                df = join_ab_on_key_optimizer(l, r, l_key, r_key, suffix_str=suffix_str)
                # df = join_ab_on_key(l, r, l_key, r_key, suffix_str=suffix_str)
                if df is False:  # happens when join is outlier - (causes run out of memory)
                    return False

                suffix_str += '_x'
                k.set_payload(df)  # update payload
                if child in leaves:
                    leaves.remove(child)  # removed merged children
            # joined all children, now we include joint df on leaves
            if k not in leaves:  # avoid re-adding parent element
                leaves.append(k)  # k becomes a new leave
    materialized_view = leaves[0].get_payload()  # the last leave is folded onto the in-tree root
    return materialized_view


def apply_consistent_sample(dfa, dfb, a_key, b_key, sample_size):
    # Normalize values
    dfa[a_key] = dfa[a_key].apply(lambda x: str(x).lower())
    dfb[b_key] = dfb[b_key].apply(lambda x: str(x).lower())
    # Chose consistently sample of IDs
    a_len = len(set(dfa[a_key]))
    b_len = len(set(dfb[b_key]))
    if a_len > b_len:
        sampling_side = dfa
        sampling_key = a_key
    else:
        sampling_side = dfb
        sampling_key = b_key
    id_to_hash = dict()
    for el in set(sampling_side[sampling_key]):  # make sure you don't draw repetitions
        h = hash(el)
        id_to_hash[h] = el
    sorted_hashes = sorted(id_to_hash.items(), key=lambda x: x[1], reverse=True)  # reverse or not does not matter
    chosen_ids = [id for hash, id in sorted_hashes[:sample_size]]

    # Apply selection on both DFs
    dfa = dfa[dfa[a_key].isin(chosen_ids)]
    dfb = dfb[dfb[b_key].isin(chosen_ids)]

    # Remove duplicate keys before returning
    dfa = dfa.drop_duplicates(subset=a_key)
    dfb = dfb.drop_duplicates(subset=b_key)
    dfa.reset_index(drop=True)
    dfb.reset_index(drop=True)

    return dfa, dfb


def materialize_join_graph_sample(jg, dod, sample_size=100):
    print("Materializing:")
    pp.pprint(jg)

    def build_tree(jg):
        # Build in-tree (leaves to root)
        intree = dict()  # keep reference to all nodes here
        leaves = []

        hops = jg

        while len(hops) > 0:
            pending_hops = []  # we use this variable to maintain the temporarily disconnected hops
            for l, r in hops:
                if len(intree) == 0:
                    node = InTreeNode(l.source_name)
                    node_path = dod.aurum_api.helper.get_path_nid(l.nid) + "/" + l.source_name
                    df = read_relation_on_copy(node_path)# FIXME FIXME FIXME
                    # df = get_dataframe(node_path)
                    node.set_payload(df)
                    intree[l.source_name] = node
                    leaves.append(node)
                # now either l or r should be in intree
                if l.source_name in intree.keys():
                    rnode = InTreeNode(r.source_name)  # create node for r
                    node_path = dod.aurum_api.helper.get_path_nid(r.nid) + "/" + r.source_name
                    df = read_relation_on_copy(node_path)# FIXME FIXME FIXME
                    # df = get_dataframe(node_path)
                    rnode.set_payload(df)
                    r_parent = intree[l.source_name]
                    rnode.add_parent(r_parent)  # add ref
                    # r becomes a leave, and l stops being one
                    if r_parent in leaves:
                        leaves.remove(r_parent)
                    leaves.append(rnode)
                    intree[r.source_name] = rnode
                elif r.source_name in intree.keys():
                    lnode = InTreeNode(l.source_name)  # create node for l
                    node_path = dod.aurum_api.helper.get_path_nid(l.nid) + "/" + l.source_name
                    df = read_relation_on_copy(node_path)# FIXME FIXME FIXME
                    # df = get_dataframe(node_path)
                    lnode.set_payload(df)
                    l_parent = intree[r.source_name]
                    lnode.add_parent(l_parent)  # add ref
                    if l_parent in leaves:
                        leaves.remove(l_parent)
                    leaves.append(lnode)
                    intree[l.source_name] = lnode
                else:
                    # temporarily disjoint hop which we store for subsequent iteration
                    pending_hops.append((l, r))
            hops = pending_hops
        return intree, leaves

    def find_l_r_key(l_source_name, r_source_name, jg):
        for l, r in jg:
            if l.source_name == l_source_name and r.source_name == r_source_name:
                return l.field_name, r.field_name
            elif l.source_name == r_source_name and r.source_name == l_source_name:
                return r.field_name, l.field_name

    intree, leaves = build_tree(jg)
    # find groups of leaves with same common ancestor
    suffix_str = '_x'
    go_on = True
    while go_on:
        if len(leaves) == 1 and leaves[0].get_parent() is None:
            go_on = False
            continue  # we have now converged
        leave_ancestor = defaultdict(list)
        for leave in leaves:
            if leave.get_parent() is not None:  # never add the parent's parent, which does not exist
                leave_ancestor[leave.get_parent()].append(leave)
        # pick ancestor and find its join info with each children, then join, then add itself to leaves (remove others)
        for k, v in leave_ancestor.items():
            for child in v:
                l = k.get_payload()
                r = child.get_payload()
                l_key, r_key = find_l_r_key(k.node, child.node, jg)

                # print("L: " + str(k.node) + " - " + str(l_key) + " size: " + str(len(l)))
                # print("R: " + str(child.node) + " - " + str(r_key) + " size: " + str(len(r)))

                l, r = apply_consistent_sample(l, r, l_key, r_key, sample_size)

                # normalize false because I ensure it happens in the apply-consistent-sample function above
                df = join_ab_on_key(l, r, l_key, r_key, suffix_str=suffix_str, normalize=True)

                # df = join_ab_on_key_optimizer(l, r, l_key, r_key, suffix_str=suffix_str)
                # df = join_ab_on_key(l, r, l_key, r_key, suffix_str=suffix_str)
                if len(df) == 0:
                    df = False
                if df is False:  # happens when join is outlier - (causes run out of memory)
                    print("FALSE")
                    return False

                suffix_str += '_x'
                k.set_payload(df)  # update payload
                if child in leaves:
                    leaves.remove(child)  # removed merged children
            # joined all children, now we include joint df on leaves
            if k not in leaves:  # avoid re-adding parent element
                leaves.append(k)  # k becomes a new leave
    materialized_view = leaves[0].get_payload()  # the last leave is folded onto the in-tree root
    return materialized_view


def estimate_join_memory(a: pd.DataFrame, b: pd.DataFrame):
    # 1. check each dataframe's size in memory and number of rows
    a_bytes = sum(a.memory_usage(deep=True))
    b_bytes = sum(b.memory_usage(deep=True))
    a_len_rows = len(a)
    b_len_rows = len(b)

    # 2. estimate size per row from previous
    a_row_size = float(a_bytes/a_len_rows)
    b_row_size = float(b_bytes/b_len_rows)

    # 3. estimate row size of output join (no selections)
    o_row_size = a_row_size + b_row_size

    # 4. estimate cartesian product size in rows
    o_num_rows = len(a) * len(b)

    # 5. estimate cartesian product size in bytes
    o_size_est = o_num_rows * o_row_size

    # 6. check with memory limit and pick plan
    if o_size_est > memory_limit_join_processing:
        return False
    else:
        return True


def compute_table_cleanliness_profile(table_df: pd.DataFrame) -> dict:
    columns = table_df.columns
    # TODO: return a dict with a col profile. perhaps do some aggr at the end to return table-wide stats as well
    # unique / total
    # num null values
    # uniqueness column in the whole dataset -> information
    # FIXME: cardinality of the join - this is specific to a pair and not the invidivual
    #

    return columns


if __name__ == "__main__":

    # JOIN
    # a = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Drupal_employee_directory.csv", encoding='latin1', sep=data_separator)
    # b = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Employee_directory.csv", encoding='latin1', sep=data_separator)
    #
    # a_key = 'Mit Id'
    # b_key = 'Mit Id'
    # joined = join_ab_on_key(a, b, a_key, b_key)

    # JOIN causes trouble
    # a = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Fclt_building_list.csv", encoding='latin1', sep=data_separator)
    # b = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Fclt_building.csv", encoding='latin1', sep=data_separator)
    # a_key = 'Building Sort'
    # b_key = 'Building Number'

    # joined = join_ab_on_key(a, b, a_key, b_key)
    # joined = join_ab_on_key_optimizer(a, b, a_key, b_key)
    # exit()

    # Find KEY
    # path = "/Users/ra-mit/data/mitdwhdata/Warehouse_users.csv"
    # attribute_name = 'Unit Name'
    # attribute_value = 'Mechanical Engineering'
    # key_attribute = 'Krb Name Uppercase'
    #
    # keys = find_key_for(path, key_attribute, attribute_name, attribute_value)
    #
    # print(str(keys))

    # Find and remove nan values
    a = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Fclt_organization.csv", encoding='latin1')
    a_key = 'Organization Number'
    a[a_key] = a[a_key].apply(lambda x: str(x).lower())
    print("Original size: " + str(len(a)))

    a.dropna(subset=[a_key], inplace=True)
    print("After dropna: " + str(len(a)))

    # pp.pprint(a[a_key])
    # print(type(a[a_key].loc[4]))

    a_null_indices = [i for i, el in enumerate(a[a_key]) if el == 'null' or el == 'nan' or el is pd.NaT]
    a.drop(a_null_indices, inplace=True)
    print("after individual indices: " + str(len(a)))
