from elasticsearch_dsl.connections import connections
import config as c

# Store client
client = None


def get_all_fields():
    """
    Reads all fields, described as (id, source_name, field_name) from the store.
    :return: a list of all fields with the form (id, source_name, field_name)
    """
    print("TODO")


def peek_values(field, num_values):
    """
    Reads sample values for the given field
    :param field: The field from which to read values
    :param num_values: The number of values to read
    :return: A list with the sample values read for field
    """
    print("TODO")


def search_keywords(keywords, elasticfieldname):
    """
    Performs a search query on elastic_field_name to match the provided keywords
    :param keywords: the list of keyword to match
    :return: the list of documents that contain the keywords
    """
    print("TODO")


def init_store():
    """
    Uses the configuration file to create a connection to the store
    :return:
    """
    global client
    client = connections.create_connection(hosts=[c.db_location], timeout=20)

if __name__ == "__main__":
    print("Elastic Store")
