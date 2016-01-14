from IPython import embed
import sys

import api as API
import utils as U
import config as C


def main(path):
    if path is not None:
        API.analyze_dataset(path, "raw")
    else:
        API.load_precomputed_model()
    print("Discovery_Prototype entering interactive mode...")
    embed() 

if __name__ == "__main__":
    path = None
    if len(sys.argv) is 3:
        if sys.argv[1] == "-p":
            print("Analyzing PATH: " + str(sys.argv[2]))
            path = sys.argv[2]
        elif sys.argv[1] == "-l":
            print("Loading precomputed model")
        elif sys.argv[1] == "-db":
            print("Using DB to access model")
        else:
            print("USAGE:")
            print("-p: analyze path")
            print("-l: load precomputed model")
            print("-db: use model stored in db")
    main(path)

