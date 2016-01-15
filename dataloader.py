import signal
import sys
import queue
from collections import OrderedDict

import config as C
import api as API
from dataanalysis import dataanalysis as da
from modelstore import modelstore as MS
from inputoutput import inputoutput as iod
from conceptgraph import cgraph as cg
from conceptgraph import simrank as sr

# Capturing ctrl+C
def signal_handler(signal, frame):
    print('Finishing pending work...')
    goOn = False
    import time
    time.sleep(3) # wait 3 secs before shutting down
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)
#print('Press Ctrl+C')
#signal.pause()

# All load task types that are supported
loadtask_types = ["CSV"]

# Queue that keeps tasks to process
workqueue = queue.Queue()

# Control variable to keep working
goOn = True

def load_csv_file(filename):
    '''
    Given a CSV file it creates a task for the loader
    '''
    task = ("CSV", filename)
    workqueue.put(task)

def create_work_from_path_csv_files(path):
    '''
    Given a path full of CSV files it creates
    tasks for the loader
    '''
    all_files = iod.get_files_in_dir(path)
    for f in all_files:
        load_csv_file(f)

def process_csv_file(filename):
    '''
    Incorporates the filename to the model
    '''
    # Extract columns from file
    columns = iod.get_columns_from_csv_file(filename)
    concepts = []
    for column in columns.items():
        # Clean columns
        # clean_c is a dict with 1 key
        # c_type is the value of the types
        (clean_c, c_type) = API.clean_column(column)
        values = list(clean_c.values())[0]
        (f_name, c_name) = list(clean_c.keys())[0]
        concepts.append((f_name, c_name))
        # Extract signature
        sig = None
        num_data = []
        text_data = []
        if c_type is 'N':
            # num signature
            method = C.preferred_num_method
            sig = da.get_num_dist(values, method)
            num_data = values
        elif c_type is 'T':
            # text signature
            method = C.preferred_text_method
            sig = da.get_textual_dist(values, method) 
            text_data = values
        # Load info to model store
        MS.new_column(f_name, 
                      c_name, 
                      c_type, 
                      sig, 
                      num_data, 
                      text_data)
        # Add new concepts to graph

def load():
    '''
    Consumes load tasks from queue and process them
    '''
    total_tasks_processed = 0
    # TODO: change to goOn var once this is always living
    while(workqueue.qsize() > 0):
        task = workqueue.get()
        (t_type, resource) = task
        if t_type is "CSV":
            process_csv_file(resource)    
        else:
            print("Unrecognized data type for column")
        # Keep count of processed tasks
        aprox_size = workqueue.qsize()
        total_tasks_processed = total_tasks_processed + 1
        print("Processed Tasks: " \
        + str(total_tasks_processed)+"/" + str(aprox_size))

def refine_from_modelstore(cgraph):
    '''
    This method is an adaptation of api.refine_graph_with_csig
    to work with the store directly
    '''
    # Iterate over all nodes in the graph
    for concept in list(cgraph.keys()):
        # For each node, detect its type and compare 
        # all nodes of the same type
        sim_cols = API.columns_similar_to_DBCONN(concept)
        for col in sim_cols:
            cgraph[concept].append(col)
    return cgraph

def add_table_neighbors(concepts, cgraph):
    '''
    Add additional edges to the existent graph, those of columns
    that are in the same table
    '''
    for col in concepts:
        (fname1, _) = col 
        for col2 in concepts:
            (fname2, _) = col2
            if fname2 is fname1:
                if col2 not in cgraph[col]:
                    cgraph[col].append(col2)
    return cgraph

def build_graph():
    '''
    Build cgraph and simrank
    '''
    # First construct graph
    cgraph_cache = OrderedDict()
    concepts = MS.get_all_concepts()
    cgraph_cache = refine_from_modelstore(cgraph_cache)
    cgraph = copy.deepcopy(cgraph_cache)
    cgraph = add_table_neighbors(concepts, cgraph_table)
    # Then run simrank
    simrank = sr.simrank(cgraph, C.sr_maxiter, C.sr_eps, C.sr_c)
    return (cgraph_cache, cgraph, simrank)

def build_graph_and_store(dataset):
     if buildgraph: 
        # Build graph and simrank 
        (cgraph_cache, cgraph, simrank) = build_graph()

        # Serialize graph and simrank
        print("Storing graph...")
        serde.serialize_graph(cgraph, dataset)
        print("Storing graph...DONE!")
        print("Storing graph (cache)...")
        serde.serialize_cached_graph(cgraph_cache, dataset)
        print("Storing graph (cache)...DONE!")
        print("Storing simrank matrix...")
        serde.serialize_simrank_matrix(simrank, dataset)
        print("Storing simrank matrix...DONE!")

def main():
    mode = sys.argv[2]
    dirOrDataset = sys.argv[3]
    arg = sys.argv[4]
    dataset = sys.argv[6]
    # Initialize model store
    MS.init(dataset)
    if mode is 'ALL' or mode is 'LOAD':
        if mode == "--dir":
            print("Working on path: " + str(arg))
            create_work_from_path_csv_files(arg)
        load()
        print("FINISHED LOADING DATA TO STORE!")
        if mode is 'ALL':
            build_graph_and_store(dataset)
            print("FINISHED MODEL CREATION!!")
    elif mode is 'BGRAPH':
        build_graph_and_store(dataset)
        print("FINISHED MODEL CREATION!!")
    else:
        print("Unrecognized mode")
        exit()


if __name__ == "__main__":
    if len(sys.argv) is not 7:
        print("HELP")
        print("--mode <mode> : specifies ALL, LOAD, BGRAPH")
        print("--dir <path> to directory with CSV files")
        print("--dataset <name> of the db")
        print("USAGE")
        exit()
    main()

