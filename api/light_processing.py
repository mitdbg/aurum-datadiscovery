from modelstore import elasticstore
from api.apiutils import compute_field_id as id_from
import pandas as pd


def __obtain_dataframe(path):
    # TODO: analyze path format so that we can choose the appropriate read method
    df = pd.read_csv(path)
    return df


def head(dbname, sname, fname=False):
    # TODO:
    return


def tail(dbname, sname, fname=False):
    # TODO:
    return


def random(dbname, sname, fname=False):
    # TODO:
    return


def sample(dbname, sname, fname=False):
    # TODO:
    return


def pandas_handler(dbname, sname, fname):
    """
    Just obtain a handler to a pandas DataFrame, without exposing the format
    of the underlying data source
    :param dbname:
    :param sname:
    :param fname:
    :return:
    """
    nid = id_from(dbname, sname, fname)
    path = elasticstore.get_path_of(nid)
    df = __obtain_dataframe(path)
    return df

if __name__ == "__main__":
    print("Lightweight processing layer here")