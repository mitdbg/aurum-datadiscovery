import pandas as pd
from collections import defaultdict

from DoD.utils import FilterType
import config as C

# Cache reading and transformation of DFs
cache = dict()

data_separator = C.separator


def configure_csv_separator(separator):
    global data_separator
    data_separator = separator


def join_ab_on_key(a: pd.DataFrame, b: pd.DataFrame, a_key: str, b_key: str, suffix_str=None):
    a[a_key] = a[a_key].apply(lambda x: str(x).lower())
    b[b_key] = b[b_key].apply(lambda x: str(x).lower())
    joined = pd.merge(a, b, how='inner', left_on=a_key, right_on=b_key, sort=False, suffixes=('', suffix_str))
    return joined


def read_relation(relation_path):
    if relation_path in cache:
        df = cache[relation_path]
    else:
        df = pd.read_csv(relation_path, encoding='latin1', sep=data_separator)
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
    if relation_path in cache:
        df = cache[relation_path]
    else:
        df = pd.read_csv(relation_path, encoding='latin1', sep=data_separator)
    df = df[df[attribute] == cell_value]
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
                    df = get_dataframe(node_path)
                    node.set_payload(df)
                    intree[l.source_name] = node
                    leaves.append(node)
                # now either l or r should be in intree
                if l.source_name in intree.keys():
                    rnode = InTreeNode(r.source_name)  # create node for r
                    node_path = dod.aurum_api.helper.get_path_nid(r.nid) + "/" + r.source_name
                    df = get_dataframe(node_path)
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
                    df = get_dataframe(node_path)
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

                df = join_ab_on_key(l, r, l_key, r_key, suffix_str=suffix_str)
                suffix_str += '_x'
                k.set_payload(df)  # update payload
                if child in leaves:
                    leaves.remove(child)  # removed merged children
            # joined all children, now we include joint df on leaves
            if k not in leaves:  # avoid re-adding parent element
                leaves.append(k)  # k becomes a new leave
    materialized_view = leaves[0].get_payload()  # the last leave is folded onto the in-tree root
    return materialized_view


def compute_table_cleanliness_profile(table_df: pd.DataFrame) -> dict:
    columns = table_df.columns
    # TODO: return a dict with a col profile. perhaps do some aggr at the end to return table-wide stats as well
    # unique / total
    # num null values
    # uniqueness column in the whole dataset -> information
    # FIXME: cardinality of the join - this is specific to a pair and not the invidivual
    #

    return columns


def get_dataframe(path):
    # TODO: only csv is supported
    df = pd.read_csv(path, encoding='latin1', sep=data_separator)
    return df


if __name__ == "__main__":

    # JOIN
    a = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Drupal_employee_directory.csv", encoding='latin1', sep=data_separator)
    b = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Employee_directory.csv", encoding='latin1', sep=data_separator)

    a_key = 'Mit Id'
    b_key = 'Mit Id'
    joined = join_ab_on_key(a, b, a_key, b_key)

    # Find KEY
    path = "/Users/ra-mit/data/mitdwhdata/Warehouse_users.csv"
    attribute_name = 'Unit Name'
    attribute_value = 'Mechanical Engineering'
    key_attribute = 'Krb Name Uppercase'

    keys = find_key_for(path, key_attribute, attribute_name, attribute_value)

    print(str(keys))
