import pandas as pd


def join_ab_on_key(a, b, a_key, b_key):
    joined = pd.merge(a, b, how='inner', left_on=a_key, right_on=b_key, sort=False)
    return joined


def find_key_for(relation_path, key, attribute, value):
    """
    select key from relation where attribute = value;
    """
    df = pd.read_csv(relation_path, encoding='latin1')
    key_value_df = df[df[attribute] == value][[key]]
    return key_value_df.values[0][0]


if __name__ == "__main__":

    a = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Drupal_employee_directory.csv", encoding='latin1')
    b = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Employee_directory.csv", encoding='latin1')

    a_key = 'Mit Id'
    b_key = 'Mit Id'
    joined = join_ab_on_key(a, b, a_key, b_key)
