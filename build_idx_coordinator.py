import queue
import signal
import sys
import time
import copy
from collections import OrderedDict

import config as C
import api as API
from inputoutput import serde
from inputoutput import inputoutput as iod
import ddworker as ASYNC

# Capturing ctrl+C
def signal_handler(signal, frame):
    print('Finishing pending work...')
    goOn = False
    import time
    time.sleep(3) # wait 3 secs before shutting down
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

'''
State
'''

# Queue that keeps tasks to process
workqueue = queue.Queue()

# Coordinator for index building

maxsize = C.max_future_list_size
list_of_future_results = []

def process_result():
    print("TODO")

def process_futures():
    '''
    Process finished futures and returns only when there is
    space available in the queue
    '''
    goOn = True
    while goOn:
        for el in list_of_future_result:
            if el.ready():
                result = el.get()
                process_result(result)
        if len(el) < maxsize:
            goOn = False
        else:
            # sleep 1 second
            time.sleep(1)

def build_indexes():
    # Get all concepts
    cgraph_cache = OrderedDict()
    concepts = MS.get_all_concepts()
    ASYNC.distributed_concepts.delay(concepts) # broadcast
    # Choose pivot concept
    for c in concepts:
        (p_values, p_type) = MS.get_values_and_type(c) 
        process_futures()
        future = ASYNC.send_col.delay(p_values, p_type) #broadcast
        list_of_future_results.append(future)
        

# Loading functions

def process_csv_file(filename):
    '''
    Incorporates the filename to the model
    '''
    # Extract columns from file
    columns = iod.get_columns_from_csv_file(filename)
    for column in columns.items():
        # Clean columns
        # clean_c is a dict with 1 key
        # c_type is the value of the types
        (clean_c, c_type) = API.clean_column(column)
        values = list(clean_c.values())[0]
        (f_name, c_name) = list(clean_c.keys())[0]
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
        print("Finished processing: " + str(resource))

def create_work_from_path_csv_files(path):
    '''
    Given a path full of CSV files it creates
    tasks for the loader
    '''
    def load_csv_file(filename):
        '''
        Given a CSV file it creates a task for the loader
        '''
        task = ("CSV", filename)
        workqueue.put(task)

    all_files = iod.get_files_in_dir(path)
    for f in all_files:
        load_csv_file(f)

def create_work_from_db(dbconn):
    print("TODO")


# Starting function 


def main():
    mode = sys.argv[2]
    dirOrDataset = sys.argv[3]
    sourceInputType = sys.argv[4]
    arg = sys.argv[6]
    dataset = sys.argv[8]
    print("MODE: " + str(mode))
    print("DATASET: " + str(dataset))
    # Initialize model store
    MS.init(dataset)
    if mode == "ALL" or mode == "LOAD":
        if dirOrDataset == "--input":
            if sourceInputType == "csvfiles":
                print("Working on path: " + str(arg))
                create_work_from_path_csv_files(arg)
            elif sourceInputType == "db":
                print("Working on db: " + str(arg))
                create_work_from_db(arg)
        load()
        print("FINISHED LOADING DATA TO STORE!")
        if mode == "ALL":
            build_indexes(dataset)
            print("FINISHED MODEL CREATION!!")
    elif mode == "BGRAPH":
        build_indexes(dataset)
        print("FINISHED MODEL CREATION!!")
    else:
        print("Unrecognized mode")
        exit()

if __name__ == "__main__":
    if len(sys.argv) is not 7:
        print("HELP")
        print("--mode <mode> : ALL (load and bgraph), LOAD, BGRAPH")
        print("--input <type>: (db, csvfiles), <path> to dbconnector or
directory with CSV files")
        print("--dataset <name> : name given to this data repo")
        print("USAGE")
        exit()
    main()

