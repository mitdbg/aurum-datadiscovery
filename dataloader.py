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

cgraph = OrderedDict()

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
    update_concept_graph(concepts)

def update_concept_graph(concepts):
    '''
    Update concept graph incrementally with new concepts
    TODO: evolving
    '''
    global cgraph
    cgraph = cg.build_graph_skeleton(concepts, cgraph)

def load():
    '''
    Consumes load tasks from queue and process them
    '''
    total_tasks_processed = 0
    while(goOn):
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

def main():
    mode = sys.argv[1]
    arg = sys.argv[2]
    dataset = sys.argv[4]
    # Initialize model store
    MS.init(dataset)
    if mode == "--dir":
        print("Working on path: " + str(arg))
        create_work_from_path_csv_files(arg)
    load()
    print("Done!!")


if __name__ == "__main__":
    if len(sys.argv) is not 5:
        print("HELP")
        print("--dir <path> to directory with CSV files")
        print("--dataset <name> of the db")
        print("USAGE")
        exit()
    main()

