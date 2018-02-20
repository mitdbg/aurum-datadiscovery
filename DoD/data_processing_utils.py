import pandas as pd
from DoD.utils import FilterType

# Cache reading and transformation of DFs
cache = dict()


def join_ab_on_key(a: pd.DataFrame, b: pd.DataFrame, a_key: str, b_key: str):
    # First make sure to remove empty/nan values from join columns
    # TODO: Generate data event if nan values are found
    a_valid_index = (a[a_key].dropna()).index
    b_valid_index = (b[b_key].dropna()).index
    a = a.iloc[a_valid_index]
    b = b.iloc[b_valid_index]

    # Normalize join columns
    a_original = a[a_key].copy()
    b_original = b[b_key].copy()
    a[a_key] = a[a_key].apply(lambda x: str(x).lower())
    b[b_key] = b[b_key].apply(lambda x: str(x).lower())

    joined = pd.merge(a, b, how='inner', left_on=a_key, right_on=b_key, sort=False, suffixes=('_x', ''))

    # # Recover format of original columns
    # FIXME: would be great to do this, but it's broken
    # joined[a_key] = a_original
    # joined[b_key] = b_original

    return joined


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
        df = pd.read_csv(relation_path, encoding='latin1')
        #df = df.apply(lambda x: x.astype(str).str.lower())
        # cache[relation_path] = df  # cache for later
    try:
        key_value_df = df[df[attribute].map(lambda x: str(x).lower()) == value][[key]]
    except KeyError:
        print("wtf")
        a = 1
    return {x[0] for x in key_value_df.values}


def is_value_in_column(value, relation_path, column):
    # normalize this value
    value = str(value).lower()
    if relation_path in cache:
        df = cache[relation_path]
    else:
        df = pd.read_csv(relation_path, encoding='latin1')
        #df = df.apply(lambda x: x.astype(str).str.lower())
        cache[relation_path] = df  # cache for later
    return value in df[column].map(lambda x: str(x).lower()).unique()


def obtain_attributes_to_project(jp_with_filters):
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
    print("Projecting: " + str(attributes_to_project))
    df = df[list(attributes_to_project)]
    return df


def materialize_join_path(jp_with_filters, dod):
    filters, jp = jp_with_filters

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
                l = pd.read_csv(path, encoding='latin1')
                #l = df.apply(lambda x: x.astype(str).str.lower())
            path = r_path + '/' + r.source_name
            if path in cache:
                r = cache[path]
            else:
                r = pd.read_csv(path, encoding='latin1')
                #r = df.apply(lambda x: x.astype(str).str.lower())
        else:  # roll the partially joint
            l = df
            path = r_path + '/' + r.source_name
            if path in cache:
                r = cache[path]
            else:
                r = pd.read_csv(path, encoding='latin1')
                #r = df.apply(lambda x: x.astype(str).str.lower())
        df = join_ab_on_key(l, r, l_key, r_key)
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
