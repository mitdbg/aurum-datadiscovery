import pickle

import config as C

def serialize_signature_collection(tcol_dist, ncol_dist):
    path_to_serialize = C.serdepath + "T_" + C.signcollectionfile
    with open(path_to_serialize, "wb") as f:
        pickle.dump(tcol_dist, f)
    path_to_serialize = C.serdepath + "N_" + C.signcollectionfile
    with open(path_to_serialize, "wb") as f:
        pickle.dump(ncol_dist, f)
    print("Done serialization of signature collections!")

def serialize_graph(obj):
    path_to_serialize = C.serdepath + C.graphfile
    with open(path_to_serialize, "wb") as f:
        pickle.dump(obj, f)
    print("Done serialization of graph!")

def serialize_dataset_columns(obj):
    path_to_serialize = C.serdepath + C.datasetcolsfile
    with open(path_to_serialize, "wb") as f:
        pickle.dump(obj, f)
    print("Done serialization of dataset columns!")

def deserialize_signature_collections():
    path_to_deserialize = C.serdepath + "T_" + C.signcollectionfile
    with open(path_to_deserialize, "rb") as f:
        tcol_dist = pickle.load(f)
    path_to_deserialize = C.serdepath + "N_" + C.signcollectionfile
    with open(path_to_deserialize, "rb") as f:
        ncol_dist = pickle.load(f)
    print("Done deserialization of signature collections!")
    return (tcol_dist, ncol_dist)

def deserialize_graph():
    path_to_deserialize = C.serdepath + C.graphfile
    with open(path_to_deserialize, "rb") as f:
        graph = pickle.load(f)
    print("Done deserialization of signature collection!")
    return graph

def deserialize_dataset_columns():
    path_to_deserialize = C.serdepath + C.datasetcolsfile
    with open(path_to_deserialize, "rb") as f:
        dcols = pickle.load(f)
    print("Done deserialization of dataset columns!")
    return dcols 

if __name__ == "__main__":
    print("SERDE module")
