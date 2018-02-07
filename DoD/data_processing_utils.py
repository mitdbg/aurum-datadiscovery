import pandas as pd
from DoD.utils import FilterType

# Cache reading and transformation of DFs
cache = dict()


def join_ab_on_key(a: pd.DataFrame, b: pd.DataFrame, a_key: str, b_key: str):
    joined = pd.merge(a, b, how='inner', left_on=a_key, right_on=b_key, sort=False, suffixes=('_x', ''))
    return joined


def find_key_for(relation_path, key, attribute, value):
    """
    select key from relation where attribute = value;
    """
    value = value.lower()
    # Check if DF in cache
    if relation_path in cache:
        df = cache[relation_path]
    else:
        df = pd.read_csv(relation_path, encoding='latin1')
        df = df.apply(lambda x: x.astype(str).str.lower())
        # cache[relation_path] = df  # cache for later
    try:
        key_value_df = df[df[attribute] == value][[key]]
    except KeyError:
        print("wtf")
        a = 1
    return {x[0] for x in key_value_df.values}


def is_value_in_column(value, relation_path, column):
    if relation_path in cache:
        df = cache[relation_path]
    else:
        df = pd.read_csv(relation_path, encoding='latin1')
        df = df.apply(lambda x: x.astype(str).str.lower())
        cache[relation_path] = df  # cache for later
    return value in df[column].unique()


def materialize_join_path(jp_with_filters, dod):
    filters, jp = jp_with_filters
    attributes_to_project = set()
    for f in filters:
        f_type = f[1].value
        if f_type is FilterType.ATTR.value:
            attributes_to_project.add(f[0][0])
        elif f_type is FilterType.CELL.value:
            attributes_to_project.add(f[0][1])

    df = None
    for l, r in jp:
        l_path = dod.api.helper.get_path_nid(l.nid)
        r_path = dod.api.helper.get_path_nid(r.nid)
        l_key = l.field_name
        r_key = r.field_name
        print("Joining: " + str(l.source_name) + "." + str(l_key) + " with: " + str(r.source_name) + "." + str(r_key))
        if df is None:  # first iteration
            path = l_path + '/' + l.source_name
            if path in cache:
                l = cache[path]
            else:
                df = pd.read_csv(path, encoding='latin1')
                l = df.apply(lambda x: x.astype(str).str.lower())
            path = r_path + '/' + r.source_name
            if path in cache:
                r = cache[path]
            else:
                df = pd.read_csv(path, encoding='latin1')
                r = df.apply(lambda x: x.astype(str).str.lower())
        else:  # roll the partially joint
            l = df
            path = r_path + '/' + r.source_name
            if path in cache:
                r = cache[path]
            else:
                df = pd.read_csv(path, encoding='latin1')
                r = df.apply(lambda x: x.astype(str).str.lower())
        df = join_ab_on_key(l, r, l_key, r_key)
    df = df[list(attributes_to_project)]
    return df


if __name__ == "__main__":

    # JOIN
    a = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Drupal_employee_directory.csv", encoding='latin1')
    b = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Employee_directory.csv", encoding='latin1')

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
