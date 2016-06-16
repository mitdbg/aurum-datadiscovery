from IPython import embed
import sys

from knowledgerepr import fieldnetwork
from ddapi import API


def main(path_to_serialized_model):
    print('Loading: ' + str(path_to_serialized_model))
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    api = API(network)
    api.init_store()
    embed()

if __name__ == "__main__":
    if len(sys.argv) >= 2:
        path_to_serialized_model = sys.argv[2]

    else:
        print("USAGE")
        print("db: the name of the model to serve")
        print("python web.py --db <db>")
        exit()
    main(path_to_serialized_model)
