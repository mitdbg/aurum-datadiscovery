from elasticsearch_dsl.connections import connections
import config as c


class StoreHandler:

    # Store client
    client = None

    def __init__(self):
        """
            Uses the configuration file to create a connection to the store
            :return:
            """
        global client
        client = connections.create_connection(hosts=[c.db_location], timeout=20)

    def close(self):
        print("TODO")

    def get_all_fields(self):
        """
        Reads all fields, described as (id, source_name, field_name) from the store.
        :return: a list of all fields with the form (id, source_name, field_name)
        """
        print("TODO")

    def peek_values(self, field, num_values):
        """
        Reads sample values for the given field
        :param field: The field from which to read values
        :param num_values: The number of values to read
        :return: A list with the sample values read for field
        """
        print("TODO")

    def search_keywords(self, keywords, elasticfieldname):
        """
        Performs a search query on elastic_field_name to match the provided keywords
        :param keywords: the list of keyword to match
        :return: the list of documents that contain the keywords
        """
        print("TODO")

    def get_all_fields_entities(self):
        """
        Retrieves all fields and entities from the store
        :return: (fields, entities)
        """
        print("TODO")

    def get_all_fields_textsignatures(self):
        """
        Retrieves textual fields and signatures from the store
        :return: (fields, textsignatures)
        """
        print("TODO")

    def get_all_fields_numsignatures(self):
        """
        Retrieves numerical fields and signatures from the store
        :return: (fields, numsignatures)
        """
        print("TODO")


if __name__ == "__main__":
    print("Elastic Store")
