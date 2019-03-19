from IPython.terminal.embed import InteractiveShellEmbed
from IPython.display import Markdown, display
import sys
import time

from api.reporting import Report
from knowledgerepr import fieldnetwork
from modelstore.elasticstore import StoreHandler
from ddapi import API as oldAPI
from algebra import API

init_banner = "Welcome to Aurum. \nYou can access the API via the object api"
exit_banner = "Bye!"


def print_md(string):
    display(Markdown(string))


#@DeprecationWarning
def __init_system(path_to_serialized_model, create_reporting=True):
    print_md('Loading: *' + str(path_to_serialized_model) + "*")
    sl = time.time()
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    api = oldAPI(network)
    if create_reporting:
        reporting = Report(network)
    api.init_store()
    api.help()
    el = time.time()
    print("Took " + str(el - sl) + " to load all data")
    return api, reporting


def init_system(path_to_serialized_model, create_reporting=False):
    print_md('Loading: *' + str(path_to_serialized_model) + "*")
    sl = time.time()
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    store_client = StoreHandler()
    api = API(network=network, store_client=store_client)
    if create_reporting:
        reporting = Report(network)
    else:
        # Better always return a tuple even if second element is `None`
        reporting = None
    api.helper.help()
    el = time.time()
    print("Took " + str(el - sl) + " to load model")
    return api, reporting


def main(path_to_serialized_model):
    print('Loading: ' + str(path_to_serialized_model))
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    store_client = StoreHandler()
    api = API(network, store_client)
    ip_shell = InteractiveShellEmbed(banner1=init_banner, exit_msg=exit_banner)
    ip_shell()


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        path_to_serialized_model = sys.argv[2]

    else:
        print("USAGE")
        print("path_to_model: path to the folder with the model")
        print("python main.py --path_to_model <path>")
        exit()
    main(path_to_serialized_model)
