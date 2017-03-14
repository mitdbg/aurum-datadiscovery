from api.apiutils import compute_field_id as id_from
import pandas as pd


def __obtain_dataframe(path, nrows=None):
    """
    :param path: path to the file
    :param nrows: number of rows to read
    """
    # TODO: analyze path format so that we can choose the appropriate read method
    df = pd.read_csv(path, nrows=nrows)
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


def pandas_handler(store_handler, hit, nrows):
    """
    Just obtain a handler to a pandas DataFrame, without exposing the format
    of the underlying data source
    :param store_handler:
    :param nid:
    :return:
    """
    nid = hit.nid
    sname = hit.source_name

    path = store_handler.get_path_of(nid) + sname
    df = __obtain_dataframe(path, nrows)
    return df

if __name__ == "__main__":
    print("Lightweight processing layer here")