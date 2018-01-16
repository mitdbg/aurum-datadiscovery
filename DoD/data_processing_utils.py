import pandas as pd


def join_ab_on_key(a, b, a_key, b_key):
    joined = pd.merge(a, b, how='inner', left_on=a_key, right_on=b_key, sort=False)
    return joined


if __name__ == "__main__":

    a = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Drupal_employee_directory.csv", encoding='latin1')
    b = pd.read_csv("/Users/ra-mit/data/mitdwhdata/Employee_directory.csv", encoding='latin1')

    a_key = 'Mit Id'
    b_key = 'Mit Id'
    joined = join_ab_on_key(a, b, a_key, b_key)
