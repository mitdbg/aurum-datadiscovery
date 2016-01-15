from pymongo import MongoClient

import config as C

# db client
dbc = None
# Model database
modeldb = None
# Source tracking database
srctrackingdb = None

def build_column_key(filename, columname):
    key = str(filename + "-" + columname)
    return key

def new_column(f, c, t, sig, n_data, t_data):
    '''
    f -> file name
    c -> column name
    t -> column type
    sig -> column signature
    n_data -> numerical data
    t_data -> textual data
    '''
    key = build_column_key(f, c)
    doc = {
    "key" : key,
    "filename" : f,
    "column" : c,
    "type" : t,
    "signature" : sig,
    "t_data" : t_data,
    "n_data" : n_data
    }
    print(str(doc))
    modeldb.columns.insert_one(doc)
    #print("KEY: " + key + " t: " + str(t))

def get_all_concepts():
    '''
    Return all keys dataset, copied into a collection
    '''
    print("todo")

def get_fields_from_concept(concept, arg1, arg2):
    '''
    Project the given attributes that are returned in a tuple
    '''
    print("todo")

def get_all_num_cols_for_comp():
    '''
    Return a cursor to the num columns, projecting (key, sig)
    '''
    print("todo")

def get_all_text_cols_for_comp():
    ''' 
    Return a cursor to the num columns, projecting (key, sig)
    '''
    print("todo")

def init(dataset_name):
    # Connection to DB system
    global dbc
    dbc = MongoClient(C.db_location)
    # Configure right DB
    global modeldb
    modeldb = dbc[dataset_name]
    print(str(modeldb))
    

if __name__ == "__main__":
    print("todo")
   
