from IPython import embed
import sys

#import api as API
import utils as U
import config as C

mode = None

def main(path, mode, modelname):
    print("PATH: " + str(path))
    print("MODE: " + str(mode))
    print("MODELNAME: " + str(modelname))
    if mode is "load_from_scratch":
        import api as API
        API.analyze_dataset(path, "raw", modelname)
    elif mode is "deserialize_model":
        import api as API
        API.load_precomputed_model(modelname)
    elif mode is "model_in_DB":
        from api import p as API
        # load specifid model bits (graph) by importing 
        # one specific function that is not visible to others
        from api import load_precomputed_model_DBVersion
        load_precomputed_model_DBVersion(modelname)
    else:
        print("Choose input mode")
        exit()
    print("Discovery_Prototype entering interactive mode...")
    embed() 

if __name__ == "__main__":
    path = None
    global mode
    mode = None
    if len(sys.argv) >= 2:
        if sys.argv[1] == "-p":
            print("Analyzing PATH: " + str(sys.argv[2]))
            path = sys.argv[2]
            mode = "load_from_scratch"
        elif sys.argv[1] == "-l":
            print("Loading precomputed model")
            mode = "deserialize_model"
        elif sys.argv[1] == "-db":
            print("Using DB to access model")
            mode = "model_in_DB"
    else:
        print("USAGE:")
        print("-p: analyze path")
        print("-l: load precomputed model")
        print("-db: use model stored in db")
        print("<modelname>")
    # model name is provided as the last parameter
    modelname = sys.argv[len(sys.argv)-1]
    main(path, mode, modelname)

