import sys
import os
import time
from os import listdir
from os.path import isfile, join
from collections import OrderedDict

import utils
import config as C
from inputoutput import inputoutput as iod
from inputoutput import serde
from dataanalysis import dataanalysis  as da
from dataanalysis import jointsignatureanalysis as jsa
from conceptgraph import cgraph as cg
from conceptgraph import simrank as sr

cgraph = OrderedDict()
simrank = None
dataset_columns = dict()
ncol_dist = dict()
tcol_dist = dict()

now = lambda: int(round(time.time())*1000)

class DB_adapted_API():
    '''
    This class is used to wrap up functions
    that need special treatment
    '''
    
    def test1(self):
        return len(dataset_columns)

    def search_keyword(keyword):
        '''
        Returns [(dataset,column)] that contain the
        given keyword
        '''
        print("todo") 
        return None

    def columns_like((filename, columnname)):
        '''
        Returns all columns similar to the provided
        '''
        

        return None

    def columns_in_context_with((filename, columnname)):
        '''
        Structural similarity
        '''

        return None

# Instantiate class to make it importable
p = DB_adapted_API()

def get_dataset_files(dataset_path):
    '''
        Get all non-hidden files in a given directory
    '''
    files = iod.get_files_in_dir(dataset_path)
    print("Dataset with: " + str(len(files)) + " files")
    return files

def get_dataset_columns_from_files(files):
    ''' 
        Extracts all columns from dataset provided as 
        list of filepaths 
    '''
    #global dataset_columns
    cols = iod.get_column_iterator_csv(files)
    dataset_columns = process_columns_types(cols)
    print(  "Extracted " + 
            str(len(dataset_columns.items())) +
            " columns")
    return dataset_columns

def clean_column(column):
    '''
    TODO: will become a complex process, move to a dif module
    '''
    clean_c = dict()
    column_type = None
    (key, value) = column
    if utils.is_column_num(value):
        newvalue = utils.cast_list_to_float(value)
        clean_c[key] = newvalue
        column_type = 'N'
    else:
        clean_c[key] = value
        column_type = 'T'
    return (clean_c, column_type)

def process_columns_types(cols):
    toret = dict()
    for col in cols.items():
        (clean_c, column_type) = clean_column(col)
        toret.update(clean_c)
    return toret

def dataset_columns(path):
    '''
       Parses all files in the path and return columns in the dataset 
    '''
    files = get_dataset_files(path)
    columns = get_dataset_columns_from_files(files)
    return columns

def show_columns_of(filename, dataset_columns):
    '''
        Returns the columns of a given file
    '''
    columns = []
    for (fname, cname) in dataset_columns.keys():
        if fname == filename:
            columns.append(cname)
    return columns
    
def get_column(filename, columname, dataset_columns):
    ''' 
        Return the column values
    '''
    key = (filename, columname)
    if key in dataset_columns:
        return dataset_columns[key]

def get_signature_for(column, method):
    ''' 
        Return the distribution for the indicated column,
        according to the provided method
    '''
    return da.get_column_signature(column, method)

def get_jsignature_for(fileA, columnA, fileB, columnB, method):
    '''
        Return the joint signature for the indicated columns
    '''
    columnA = dataset_columns[(fileA, columnA)]
    columnB = dataset_columns[(fileB, columnB)]
    if utils.is_column_num(columnA) and utils.is_column_num(columnB): 
        return jsa.get_jsignature(columnA, columnB, method)
    else:
        print("Column types not supported")

def get_similarity_columns(columnA, columnB, method):
    '''
        Return similarity metric given a method (ks)
    '''
    if method == "ks":
        return da.compare_num_columns_dist_ks(columnA, columnB)
    elif method == "odsvm":
        return da.compare_num_columns_dist_odsvm(columnA, columnB)

def pairs_similar_to_pair(X, Y, method):
    '''
        Given two columns, it finds a jsignature for them and then 
        returns all pairs with similar score.
    '''
    (fileA, columnA) = X
    (fileB, columnB) = Y
    jsig = get_jsignature_for(fileA, columnA, fileB, columnB, method) 
    if jsig is False:
        print("Could not compute joint signature for the provided columns")
        return False
    return pairs_similar_to_jsig(jsig, method)

def pairs_similar_to_jsig(jsignature, method):
    '''
        Return all pairs whose jsignature is similar to the provided.
    '''
    return jsa.get_similar_pairs(jsignature, dataset_columns, method)

def columns_similar_to_jsig(filename, column, jsignature, method):
    '''
        Return columns similar to the given joint signature
    '''
    key = (filename, column)
    column_data = dataset_columns[key]
    sim = jsa.get_columns_similar_to_jsignature(
                column_data,
                jsignature,
                dataset_columns,
                method)
    return sim

def columns_similar_to_DBCONN(concept):
    '''
    Iterate over entire db to find similar cols
    '''
    sim_cols = []
    (c_type, sig) = MS.get_fields_from_concept(concept, "type", "sig")
    if c_type is 'N':
        ncol_cursor = MS.get_all_num_cols_for_comp()
        for el in ncol_cursor:
            are_sim = da.compare_pair_num_columns(sig, el["sig"])
            if are_sim:
                sim_cols.append(el["key"])
    elif c_type is 'T':
        tcol_cursor = MS.get_all_text_cols_for_comp()
        for el in tcol_cursor:
            are_sim = da.compare_pair_text_columns(sig, el["sig"])
            if are_sim:
                sim_cols.append(el["key"])
    return sim_cols

def columns_similar_to(filename, column, similarity_method):
    '''
        Return columns similar to the provided one,
        according to some notion of similarity
    ''' 
    key = (filename, column)
    sim_vector = None
    sim_columns = []
    if key in ncol_dist: # numerical
        #print("Numerical search")
        if similarity_method is "ks":
            sim_items = da.get_sim_items_ks(key, ncol_dist)
            sim_columns.extend(sim_items)
    elif key in tcol_dist: # text
        #print("Textual search")
        sim_vector = da.get_sim_vector_text(key, tcol_dist)
        for (filekey, sim) in sim_vector.items():
            #print(str(sim) + " > " + str(C.cosine["threshold"]))
            if sim > C.cosine["threshold"]: # right threshold?
                sim_columns.append(filekey)
    return sim_columns

def neighbors_of(concept):
    '''
        Returns all concepts that are neighbors
        of concept
    '''
    return cg.give_neighbors_of(concept, cgraph)

def give_structural_sim_of(concept):
    '''
        Returns all concepts that are similar (structure)
        to concept after a given threshold
    '''
    return cg.give_structural_sim_of(concept, cgraph, simrank)

def analyze_dataset(list_path, signature_method):
    ''' Gets files from directory, columns from 
        dataset, and distribution for each column 
    '''
    all_files_in_dir = iod.get_files_in_dir(list_path)
    print("FILES:")
    for f in all_files_in_dir:
        print(str(f))
    print("Processing " + str(len(all_files_in_dir))+ " files")
    st = now()
    global dataset_columns
    dataset_columns = get_dataset_columns_from_files(all_files_in_dir)
    et = now()
    t_to_extract_cols = str(et-st)

    # Form graph skeleton
    st = now()
    global cgraph
    cgraph = cg.build_graph_skeleton(list(dataset_columns.keys()))
    et = now()
    t_to_build_graph_skeleton = str(et-st)

    # Store dataset info in mem
    st = now()
    global ncol_dist
    global tcol_dist
    (ncol_dist, tcol_dist) = da.get_columns_signature(
                            dataset_columns,
                            signature_method)
    et = now()
    t_to_extract_signatures = str(et-st)
    # Refine concept graph
    st = now()
    cgraph = cg.refine_graph_with_columnsignatures(
            ncol_dist, 
            tcol_dist, 
            cgraph
    )
    et = now()
    t_to_refine_graph = str(et-st)
    st = now()
    global simrank
    simrank = sr.simrank(cgraph, C.sr_maxiter, C.sr_eps, C.sr_c)
    et = now()
    t_to_simrank = str(et-st)
    print("Took: " +t_to_extract_cols+ "ms to extract columns")
    print("Took: " +t_to_build_graph_skeleton+ "ms to build cgraph skeleton")
    print("Took: " +t_to_extract_signatures+ "ms to extract column signatures")
    print("Took: " +t_to_refine_graph+ "ms to refine concept graph")
    print("Took: " +t_to_simrank+ "ms to compute simrank")
    return (ncol_dist, tcol_dist)

def store_precomputed_model():
    '''
    Store dataset columns, signature collection (2 files) and
    graph
    '''
    print("Storing signatures...")
    serde.serialize_signature_collection(tcol_dist, ncol_dist)    
    print("Storing signatures...DONE!")
    print("Storing graph...")
    serde.serialize_graph(cgraph)
    print("Storing graph...DONE!")
    print("Storing simrank matrix...")
    serde.serialize_simrank_matrix(simrank)
    print("Storing simrank matrix...DONE!")
    print("Storing dataset columns...")
    serde.serialize_dataset_columns(dataset_columns)
    print("Storing dataset columns...DONE!")

def load_precomputed_model():
    print("Loading signature collections...")
    global tcol_dist
    global ncol_dist
    (tcol_dist, ncol_dist) = serde.deserialize_signature_collections()
    print("Loading signature collections...DONE!")
    print("Loading graph...")
    global cgraph
    cgraph = serde.deserialize_graph()
    print("Loading graph...DONE!")
    print("Loading simrank matrix...")
    global simrank
    simrank = serde.deserialize_simrank_matrix()
    print("Loading simrank matrix...DONE!")
    print("Loading dataset columns...")
    global dataset_columns
    dataset_columns = serde.deserialize_dataset_columns()
    print("Loading dataset columns...DONE!")

def process_files(files, signature_method, similarity_method):
    dataset_columns = get_dataset_columns_from_files(files)

    (ncol_dist, tcol_dist) = da.get_columns_signature(
                            dataset_columns, 
                            signature_method)
    sim_matrix_num = da.get_sim_matrix_numerical(
                ncol_dist, 
                similarity_method
    )
    sim_matrix_text = da.get_sim_matrix_text(tcol_dist)
    print("")
    print("Similarity for numerical columns")
    utils.print_dict(sim_matrix_num)
    print("")
    print("Similarity for textual columns")
    utils.print_dict(sim_matrix_text)

def print_result(result):
    from IPython.display import Markdown, display
    def printmd(string):
        display(Markdown(string))
    grouped_by_dataset = dict()
    for dataset, column in result:
        if dataset not in grouped_by_dataset:
            grouped_by_dataset[dataset] = []
        grouped_by_dataset[dataset].append(column)
    for key, value in grouped_by_dataset.items():
        printmd("**" + str(key) + "**")
        for v in value:
            print("   " + str(v))
        

def main():
    # Parse input parameters
    mode = sys.argv[1]
    arg = sys.argv[2]
    signature_method = sys.argv[3]
    similarity_method = sys.argv[4]
    # Container for files to parse
    files = [] 
    if mode == "-p":
        print('Working on path: ' + str(arg))
        all_files_in_dir = iod.get_files_in_dir(arg)
        for f in all_files_in_dir:
            print(str(f))
        files.extend(all_files_in_dir)
    elif mode == "-f":
        print('Working on file: ' + str(arg))
        files.append(arg)
    print("Processing " + str(len(files))+ " files")
    process_files(files, signature_method, similarity_method)
   

if __name__ == "__main__":
    if len(sys.argv) is not 5:
        print("HELP")
        print("-p  <path> to directory with CSV files")
        print("-f  <path> to CSV file")
        print("USAGE")
        print("python main.py -p/-f <path> " + \
               "<numerical_signature_method> \
                <numerical_similarity_method>")
        exit()
    main()

