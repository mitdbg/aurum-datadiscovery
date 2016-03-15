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
from modelstore import modelstore as MS
from dataanalysis import dataanalysis as da
import fullworker as ASYNC

# Capturing ctrl+C
def signal_handler(signal, frame):
    print('Finishing pending work...')
    goOn = False
    import time
    time.sleep(3) # wait 3 secs before shutting down
    sys.exit(0)
signal.signal(signal.SIGINT, signal_handler)

# ALL (load and build index simultaneously)
# LOAD (load data only)
# BGRAPH (read signatures from store and build indexes)
mode = None

def partition_concepts(concepts, workers):
    '''
    Hash-partitions concepts per filename
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

def create_task(tables, source_input_type, arg):
    batch = []
    batchsize = C.parallel_index_batch_size
    tasks_in_batch = 0

    while tasks_in_batch < batchsize:
        if len(tables) > 0:
            table = tables[0]
            batch.append((source_input_type, arg, table))
            tables.remove(table)
        else:
            print("No tables left")
            break
    return batch
        
def process_result(v):
    '''
    Process a result returned by a worker.
    The processing will depend on the mode
    '''
    global mode
    if mode == "LOAD":
        return True
    if mode == "ALL":
        # in this case we need to load into next stage already
        return False # TODO: fix this

def assign_task(q, task):
    '''
    Sends the task to the queue q
    '''
    return ASYNC.load_tables.apply_async(args=[task], queue=q)

def load_data_parallel(source_input_type, arg, dbname, workers):
    '''
    Reads concepts, partitions them, assign them to workers
    and finally triggers the loading process
    '''
    # Read all tables of DB
    tables = None
    if source_input_type == "db":
        tables = iod.get_tables_from_db(arg)
    elif source_input_type == "csvfiles":
        tables = iod.get_files_in_dir(arg)
    print("Num tables: " + str(len(tables)))
    # Initialize workers
    for q in workers:
        ASYNC.init_worker.apply_async(args=[dbname], queue=q)
    # create and assign tasks while there are some available
    futures = dict()
    for q in workers:
        futures[q] = None
    while len(tables) > 0:
        task = create_task(tables, source_input_type, arg)
        not_assigned = True
        while not_assigned:
            for k,v in futures.items():
                if v == None:
                    new_future = assign_task(k, task)
                    futures[k] = new_future
                    not_assigned = False
                    break
                elif v.ready():
                    process_result(v)
                    new_future = assign_task(k, task)
                    futures[k] = new_future
                    not_assigned = False
                    break # so that we can grab a new task
            time.sleep(1)

def main():
    mode = sys.argv[2]
    source_input_type = sys.argv[4]
    arg = sys.argv[5]
    dbname = sys.argv[7]
    workersqueuestring = sys.argv[9]
    workers = workersqueuestring.split(',')
    print("MODE: " + str(mode))
    print("dbname: " + str(dbname))
    print("WORKER QUEUES: " + str(workers))
    # Initialize model store
    MS.init(dbname)
    global mode
    if mode == "ALL" or mode == "LOAD":
        # do this
        load_data_parallel(source_input_type, arg, dbname, workers)
        print("DONE loading data")
        exit()
    elif mode == "BGRAPH":
        # build graphs reading signatures from store
        build_indexes()
        print("DONE building indexes")
    serialize_model(dbname)
    
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
        print("python indexer.py --mode ALL --input csvfiles whatever/fake --dataset fakeagain --workers w1,w2,w3")
        exit()
    main()
