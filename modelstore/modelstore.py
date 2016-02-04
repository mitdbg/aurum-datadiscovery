from pymongo import MongoClient
from pymongo import TEXT

import config as C

# db client
dbc = None
# Database
database = None
# Model collection 
modeldb = None

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
    try:
        modeldb.insert_one(doc)
    except pymongo.errors.DocumentTooLarge:
        print("Trying to load: " + str(f) + " - " + str(c))

def get_all_concepts():
    '''
    Return all keys dataset, copied into a collection
    '''
    concepts = []
    res_cursor = modeldb.find({}, {"filename" : 1, 
                            "column":1, 
                            "_id" : 0})
    for el in res_cursor:
        key = (el["filename"], el["column"])
        concepts.append(key)
    return concepts

def get_values_of_concept(concept):
    '''
    Get n_data or t_data depending on type
    '''
    (fname, cname) = concept
    key = build_column_key(fname, cname)
    res = modeldb.find({"key": key}, {"type": 1, 
                                    "n_data": 1,
                                    "t_data": 1})
    values = None
    type_col = None
    n_data = None
    t_data = None
    for r in res:
        type_col = r["type"]
        n_data = r["n_data"]
        t_data = r["t_data"]
    if type_col == "N":
        values = n_data
    elif type_col == "T":
        values = t_data
    return values

def get_fields_from_concept(concept, arg1, arg2):
    '''
    Project the given attributes that are returned in a tuple
    '''
    (filename, columnname) = concept
    key = build_column_key(filename, columnname)    
    res = modeldb.find({ "key" : key}, 
                       {arg1:1, arg2:1, "_id":0})
    res = res[0] # should be only one due to the query_by_prim_key
    return (res[arg1], res[arg2])

def get_all_num_cols_for_comp():
    '''
    Return a cursor to the num columns, projecting (key, sig)
    '''
    cursor = modeldb.find({"type":"N"}, 
                          {"filename":1, 
                           "column":1, 
                           "signature":1,
                           "_id":0})
    return cursor

def get_all_text_cols_for_comp():
    ''' 
    Return a cursor to the num columns, projecting (key, sig)
    '''
    cursor = modeldb.find({"type":"T"},
                          {"filename":1, 
                           "column":1, 
                           "signature":1,
                           "_id":0})
    return cursor

def search_keyword(keyword):
    '''
    Search keyword
    '''
    res = modeldb.find({"t_data": keyword},
                 {"filename":1, "column":1, "_id":0})
    colmatches = [r for r in res]
    return colmatches

def init(dataset_name):
    # Connection to DB system
    global dbc
    dbc = MongoClient(C.db_location)
    # Configure right DB
    global database
    database = dbc[dataset_name]
    global modeldb
    modeldb = database.columns
    # Create full text search index
    modeldb.create_index([('t_data', TEXT)])

def main():
    db_name = "testtiny3"
    init(db_name)
    #concepts = get_all_concepts()
    #print(str(concepts))
    #print(str(len(concepts)))
    #concept = \
    #('NYS_Liquor_Authority_New_Applications_Received.csv', \
    #'License Certificate Number')
    #res = get_fields_from_concept(concept, "key", "type")
    #print(str(res))
    #res = get_all_num_cols_for_comp()
    #for r in res:
    #    print(str(r))
    #res = get_all_text_cols_for_comp()
    #for r in res:
    #    print(str(r))
    #concept = ('Cip.csv', 'Version')
    keyword = "Colorado"
    values = search_keyword(keyword)
    for v in values:
        print(v)

    print("done!")
   
if __name__ == "__main__":
    main()
    
