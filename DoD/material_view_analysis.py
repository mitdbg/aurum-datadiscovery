import pandas as pd

def inconsistent_value_on_key(df1, df2, key=None):
    missing_keys = []
    non_unique_df1 = set()
    non_unique_df2 = set()
    conflicting_pair = []
    cols = df1.columns  # should be same in both df1 and df2
    for key_value in df1[key]:
        row1 = df1[df1[key] == key_value]
        row2 = df2[df2[key] == key_value]
        if len(row1) == 0 or len(row2) == 0:
            missing_keys.append(key_value)
            continue
        do_continue = False
        if len(row1) > 1:
            non_unique_df1.add(key_value)
            do_continue = True
        if len(row2) > 1:
            non_unique_df2.add(key_value)
            do_continue = True
        if do_continue:
            continue
        for c in cols:
            if len(row1[c]) > 0 and len(row2[c]) > 0:
                val_1 = row1[c].values
                val_2 = row2[c].values
                if val_1 != val_2 and not pd.isnull(val_1) and not pd.isnull(val_2):
                    conflicting_pair.append((row1[c].values, row2[c].values))
                    #print("Inconsistent Value Found")
                    #print("Key: " + str(key_value))
                    #print(str(row1[c].any()) + " vs " + str(row2[c].any()))
    return missing_keys, list(non_unique_df1), list(non_unique_df2), conflicting_pair


def most_likely_key(df):
    """
    Given a view, which of its attributes is most likely a key
    :param df:
    :return:
    """
    df_length = len(df)
    likely_keys = []
    columns = df.columns
    for c in columns:
        n_unique_values = df[c].nunique()
        values = (c, n_unique_values, (n_unique_values/df_length))
        likely_keys.append(values)
    likely_keys = sorted(likely_keys, key=lambda x: x[1], reverse=True)
    return likely_keys

if __name__ == "__main__":
    print("Materialized View Analysis")
