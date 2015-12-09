import sys
import os
from os import listdir
from os.path import isfile, join

import utils
import config as C
from inputoutput import inputoutput as iod
from dataanalysis import dataanalysis  as da
from dataanalysis import jointsignatureanalysis as jsa

dataset_columns = dict()
ncol_dist = dict()
tcol_dist = dict()

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

def process_columns_types(cols):
    toret = dict()
    for key, value in cols.items():
        # Make it numerical if possible
        if utils.is_column_num(value):
            newvalue = utils.cast_list_to_float(value)
            toret[key] = newvalue
        # keep it as text when not possible
        else:
            toret[key] = value
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
        print("Column type mismatch")

def get_similarity_columns(columnA, columnB, method):
    '''
        Return similarity metric given a method (ks)
    '''
    if method == "ks":
        return da.compare_num_columns_dist_ks(columnA, columnB)
    elif method == "odsvm":
        return da.compare_num_columns_dist_odsvm(columnA, columnB)

def pairs_similar_to_jsig(jsignature, method):
    '''
        Return all pairs whose jsignature is similar to the provided
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

def columns_similar_to(filename, column, similarity_method):
    '''
        Return columns similar to the provided one,
        according to some notion of similarity
    ''' 
    key = (filename, column)
    sim_vector = None
    sim_columns = []
    if key in ncol_dist: # numerical
        print("Numerical search")
        if similarity_method is "ks":
            sim_items = da.get_sim_items_ks(key, ncol_dist)
            sim_columns.extend(sim_items)
    elif key in tcol_dist: # text
        print("Textual search")
        sim_vector = da.get_sim_vector_text(key, tcol_dist)
        for filekey, sim in sim_vector.items():
            #print(str(sim) + " > " + str(C.cosine["threshold"]))
            if sim > C.cosine["threshold"]: # right threshold?
                sim_columns.append(filekey)
    return sim_columns

def analyze_dataset(list_path, signature_method):
    ''' Gets files from directory, columns from 
        dataset, and distribution for each column 
    '''
    all_files_in_dir = iod.get_files_in_dir(list_path)
    print("FILES:")
    for f in all_files_in_dir:
        print(str(f))
    print("Processing " + str(len(all_files_in_dir))+ " files")
    global dataset_columns
    dataset_columns = get_dataset_columns_from_files(all_files_in_dir)

    # Store dataset info in mem
    global ncol_dist
    global tcol_dist
    (ncol_dist, tcol_dist) = da.get_columns_signature(
                            dataset_columns,
                            signature_method)
    return (ncol_dist, tcol_dist)

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
        print("python main.py -p/-f <path> " +
               "<numerical_signature_method> <numerical_similarity_method>")
        exit()
    main()

