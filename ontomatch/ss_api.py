import sys
from algebra import API
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler

# Have a list of accepted formats in the ontology parser


class SSAPI:

    def __init__(self, network, store_client):
        self.srql = API(network)
        self.krs = []

    def add_krs(self, krs):
        """
        Register the given KR for processing. Validate accepted format, etc
        :param krs: the list of (path_to_kr, format)
        :return:
        """
        return

    def parse_krs(self, analyze=False):
        """
        Parse registered KRs with the corresponding parser and optionally analyze to retrieve statistics
        :return:
        """
        return

    def find_coarse_grain_hooks(self):
        """
        Given the model and the parsed KRs, find coarse grain hooks and register them
        :return:
        """
        return

    def find_mappings(self):
        """
        Given found coarse grain hooks, perform an in-depth analysis to find mappings
        :return:
        """
        return

    def find_links(self):
        """
        Given existings mappings and parsed KRs, find existing links in the data
        :return:
        """
        return

    def write_semantics(self):
        """
        Push found mappings, links and constraints (properties) to the model
        Reconciliation mechanism goes here (or does it go model side?)
        :return:
        """
        return

    """
    ### OUTPUT FUNCTIONS
    """

    def output_registered_krs(self):
        return

    def output_krs_statistics(self):
        return

    def output_coarse_grain_hooks(self):
        return

    def output_mappings(self):
        return

    def output_links(self):
        return


def main(path_to_serialized_model):
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    store_client = StoreHandler()

    om = SSAPI(network, store_client)

    return om

if __name__ == "__main__":
    print("SSAPI")

    path_to_model = ""
    if len(sys.argv) >= 2:
        path_to_model = sys.argv[2]

    else:
        print("USAGE")
        print("db: the name of the model to use")
        exit()

    om = main(path_to_model)

    # do things with om now, for example, for testing

