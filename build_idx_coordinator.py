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
from modelstore import mongomodelstore as MS
from dataanalysis import dataanalysis as da
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

cgraph = dict() # for similarity
jgraph = dict() # for overlap

# Coordinator for index building

maxsize = C.max_future_list_size
list_of_future_results = []

def process_result(result):
    (sim_map, ove_map) = result
    for k, v in sim_map.items():
        if k not in cgraph:
            cgraph[k] = []
        print("extending cgraph with: " + str(len(v)))
        cgraph[k].extend(v)
    for k, v in ove_map.items():
        if k not in jgraph:
            jgraph[k] = []
        print("extending jgraph with: " + str(len(v)))
        jgraph[k].extend(v)

def process_all_futures_till_completion():
    '''
    Just to run at the end to empty the future list
    '''
    while len(list_of_future_results) > 0:
        for el in list_of_future_results:
            if el.ready():
                result = el.get()
                process_result(result)
                list_of_future_results.remove(el)

def process_futures():
    '''
    Process finished futures and returns only when there is
    space available in the queue
    '''
    goOn = True
    while goOn:
        for el in list_of_future_results:
            #print("STATUS: " + str(el.status))
            if el.ready():
                result = el.get()
                process_result(result)
                list_of_future_results.remove(el)
        if len(list_of_future_results) < maxsize:
            goOn = False
        else:
            # sleep 1 second
            time.sleep(1)

def partition_concepts(concepts, workers):
    '''
    Hash-partitions concepts per workers
    '''
    numw = len(workers)
    print("Partition concepts into num_chunks: " + str(numw))
    partitions = dict()
    for w in workers:
        partitions[w] = []
    for c in concepts:
        hashc = hash(c) % numw
        key = workers[hashc]
        partitions[key].append(c)
    return partitions

def build_indexes(dbname, workers):
    # Get all concepts
    concepts = MS.get_all_concepts()
    partitions = partition_concepts(concepts, workers)
    for q in workers: # send to all workers RR
        ASYNC.distribute_concepts.apply_async(args=[partitions[q]], queue=q)
        ASYNC.init_db.apply_async(args=[dbname], queue=q) 
    it = 0
    batch = []
    batchsize = C.parallel_index_batch_size
    tasks_in_batch = 0
    # Choose pivot concept
    for c in concepts:
        print("Computing for: " + str(it))
        it = it + 1
        (p_values, p_type) = MS.get_values_and_type_of_concept(c) 
        batch.append((c, p_values, p_type))
        tasks_in_batch = tasks_in_batch + 1
        if tasks_in_batch >= batchsize:
            process_futures()
            for q in workers:
                f = ASYNC.compute_index.apply_async(args=[batch], queue=q)
                list_of_future_results.append(f)
            # reset batch for next iteration
            tasks_in_batch = 0
            batch = []
    process_all_futures_till_completion()

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
        if column is None:
             print("Found column None in file: " + str(filename)) 
             column = "NULL"
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

def serialize_model(dbname):
    '''
    Serialize the two graph structures
    '''
    # Store cgraph
    print("Storing graph (cache)...")
    serde.serialize_cached_graph(cgraph, dbname)
    print("Storing graph (cache)...DONE!")
    # Store jgraph
    print("Storing jgraph...")
    serde.serialize_jgraph(jgraph, dbname)
    print("Storing jgraph...DONE!")
    

# Starting function 


def main():
    mode = sys.argv[2]
    sourceInputType = sys.argv[4]
    arg = sys.argv[5]
    dbname = sys.argv[7]
    workersqueuestring = sys.argv[9]
    workers = workersqueuestring.split(',')
    print("MODE: " + str(mode))
    print("dbname: " + str(dbname))
    print("WORKER QUEUES: " + str(workers))
    # Initialize model store
    MS.init(dbname)
    if mode == "ALL" or mode == "LOAD":
        if sourceInputType == "csvfiles":
            print("Working on path: " + str(arg))
            create_work_from_path_csv_files(arg)
        elif sourceInputType == "db":
            print("Working on db: " + str(arg))
            create_work_from_db(arg)
        load()
        print("FINISHED LOADING DATA TO STORE!")
        if mode == "ALL":
            build_indexes(dbname, workers)
            print("FINISHED MODEL CREATION!!")
    elif mode == "BGRAPH":
        build_indexes(dbname, workers)
        print("FINISHED MODEL CREATION!!")
    else:
        print("Unrecognized mode")
        exit()
    serialize_model(dbname)

def test():
    mode = sys.argv[2]
    sourceInputType = sys.argv[4]
    arg = sys.argv[5]
    dbname = sys.argv[7]
    workersqueuestring = sys.argv[9]
    workers = workersqueuestring.split(',')
    print("MODE: " + str(mode))
    print("dbname: " + str(dbname))
    print("WORKER QUEUES: " + str(workers))
    MS.init(dbname)
    concepts = MS.get_all_concepts()
    print("Total concepts: " + str(len(concepts)))
    partitions = partition_concepts(concepts, workers)
    print("Partition concepts: ")
    for k,v in partitions.items():
        print(str(k) + ": " + str(len(v)))
    for w in workers:
        print("Sending task to " + str(w))
        ASYNC.test.apply_async(args=[w], queue=w)
    

if __name__ == "__main__":
    print("INPUT PARAMETERS: " + str(len(sys.argv)))
    print(str(sys.argv))
    # python build_idx_coordinator.py --mode BGRAPH 
    # --input csvfiles
    # /Users/ra-mit/Desktop/mitdwhdataslice --dataset slice 
    # --workers w1,w2
    if len(sys.argv) is not 10:
        print("HELP")
        print("--mode <mode> : ALL (load and bgraph), LOAD, BGRAPH")
        print("--input <type>: (db, csvfiles), <path> to dbconnector or directory with CSV files")
        print("--dataset <name> : name given to this data repo")
        print("--workers <queues> : comma separated, no space worker queues")
        print("EXAMPLE")
        print("python build_idx_coordinator.py --mode ALL --input csvfiles whatever/fake --dataset fakeagain --workers w1,w2,w3")
        exit()
    main()
    #test()

