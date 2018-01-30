import pandas as pd

# Cache reading and transformation of DFs
cache = dict()

def join_ab_on_key(a, b, a_key, b_key):
    joined = pd.merge(a, b, how='inner', left_on=a_key, right_on=b_key, sort=False)
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
        cache[relation_path] = df  # cache for later
    key_value_df = df[df[attribute] == value][[key]]
    return {x[0] for x in key_value_df.values}


if __name__ == "__main__":

    # # JOIN
    # a = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Drupal_employee_directory.csv", encoding='latin1')
    # b = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Employee_directory.csv", encoding='latin1')
    #
    # a_key = 'Mit Id'
    # b_key = 'Mit Id'
    # joined = join_ab_on_key(a, b, a_key, b_key)

    # Find KEY
    path = "/Users/ra-mit/data/mitdwhdata/Warehouse_users.csv"
    attribute_name = 'Unit Name'
    attribute_value = 'Mechanical Engineering'
    key_attribute = 'Krb Name Uppercase'

    keys = find_key_for(path, key_attribute, attribute_name, attribute_value)

    print(str(keys))
