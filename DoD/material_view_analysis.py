import pandas as pd
from enum import Enum


class EQUI(Enum):
    EQUIVALENT = 1
    DIF_CARDINALITY = 2
    DIF_SCHEMA = 3
    DIF_VALUES = 4


"""
UTILS
"""


def most_likely_key(df):
    res = uniqueness(df)
    res = sorted(res.items(), key=lambda x: x[1], reverse=True)
    return res[0]


def uniqueness(df):
    res = dict()
    for c in df.columns:
        total = len(df[c])
        unique = len(df[c].unique())
        uniqueness = float(unique)/float(total)
        res[c] = uniqueness
    return res


def curate_view(df):
    df = df.dropna()  # drop nan
    df = df.drop_duplicates()
    # this may tweak indexes, so need to reset that
    df = df.reset_index(drop=True)
    # make sure it's sorted according to some order
    df.sort_index(inplace=True, axis=1)
    df.sort_index(inplace=True, axis=0)
    return df


"""
VIEW CLASSIFICATION FUNCTIONS
"""


def equivalent(v1, v2):
    v1 = curate_view(v1)
    v2 = curate_view(v2)
    if len(v1) != len(v2):
        return False, EQUI.DIF_CARDINALITY
    if len(v1.columns) != len(v2.columns):
        return False, EQUI.DIF_SCHEMA
    if not len(set(v1.columns).intersection(set(v2.columns))) == len(v1.columns):
        return False, EQUI.DIF_SCHEMA  # dif attributes
    for c in v1.columns:
        s1 = v1[c].apply(lambda x: str(x).lower()).sort_values().reset_index(drop=True)
        s2 = v2[c].apply(lambda x: str(x).lower()).sort_values().reset_index(drop=True)
        idx = (s1 == s2)
        if not idx.all():
            return False, EQUI.DIF_VALUES
    return True, EQUI.EQUIVALENT


def contained(v1, v2):
    v1 = curate_view(v1)
    v2 = curate_view(v2)
    if len(v1) > len(v2):
        l = v1
        s = v2
    elif len(v2) > len(v1):
        l = v2
        s = v1
    elif len(v1) == len(v2):
        for c in v1.columns:
            tv1 = v1[c].apply(lambda x: str(x).lower())
            tv2 = v2[c].apply(lambda x: str(x).lower())
            v12 = len(set(tv1) - set(tv2))
            v21 = len(set(tv2) - set(tv1))
            if v12 > 0:
                return False, v12
            elif v21 > 0:
                return False, v21
        return True
    for c in l.columns:
        print(c)
        small_set = s[c].apply(lambda x: str(x).lower())
        large_set = l[c].apply(lambda x: str(x).lower())
        dif = set(small_set) - set(large_set)
        print(str(len(small_set)) + " - " + str(len(large_set)))
        if len(dif) > 0:
            return False, len(dif)
    return True


def complementary(v1, v2):
    v1 = curate_view(v1)
    v2 = curate_view(v2)
    k1 = most_likely_key(v1)[0]
    k2 = most_likely_key(v2)[0]
    s1 = set(v1[k1])
    s2 = set(v2[k2])
    s12 = (s1 - s2)
    sdiff = set()
    if len(s12) > 0:
        sdiff.update((s12))
    s21 = (s2 - s1)
    if len(s21) > 0:
        sdiff.update((s21))
    if len(sdiff) == 0:
        return False
    return True, sdiff


def contradictory(v1, v2):
    v1 = curate_view(v1)
    v2 = curate_view(v2)
    k1 = most_likely_key(v1)[0]
    k2 = most_likely_key(v2)[0]
    vg1 = v1.groupby([k1])
    vg2 = v2.groupby([k2])
    vref = None
    voth = None
    if len(vg1.groups) > len(vg2.groups):
        vref = vg1
        voth = vg2
    else:
        vref = vg2
        voth = vg1
    contradictions = []
    for gn, gv in vref:
        v = voth.get_group(gn)
        are_equivalent, equivalency_type = equivalent(gv, v)
        if not are_equivalent:
            contradictions.append((k1, k2, gn))
            #             print(contradictions)
            #             break
    if len(contradictions) == 0:
        return False
    return True, len(contradictions)


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
    return missing_keys, list(non_unique_df1), list(non_unique_df2), conflicting_pair


if __name__ == "__main__":
    print("Materialized View Analysis")

    raw_views_txt = {
        0: '/Users/ra-mit/development/discovery_proto/data/dod/raw_view_0',
        1: '/Users/ra-mit/development/discovery_proto/data/dod/raw_view_1',
        2: '/Users/ra-mit/development/discovery_proto/data/dod/raw_view_2'
    }

    views_txt = {
        0: '/Users/ra-mit/development/discovery_proto/data/dod/view_0',
        1: '/Users/ra-mit/development/discovery_proto/data/dod/view_1',
        2: '/Users/ra-mit/development/discovery_proto/data/dod/view_2'
    }

    raw_views = dict()
    for k, v in raw_views_txt.items():
        raw_views[k] = pd.read_csv(v, encoding='latin1')
    views = dict()
    for k, v in views_txt.items():
        views[k] = pd.read_csv(v, encoding='latin1')

    exact_views = dict()
    for k, v in views.items():
        print(str(k) + " -> " + str(len(v)))
        if len(v.columns) == 2:
            exact_views[k] = v

    equivalent(views[0], views[2])
