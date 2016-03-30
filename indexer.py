import queue
import signal
import sys
import time
import copy
from collections import OrderedDict

from nearpy import Engine
from nearpy.hashes import RandomBinaryProjections
from nearpy.distances import EuclideanDistance
from nearpy.distances import CosineDistance
import numpy as np

import config as C
import api as API
from inputoutput import serde
from inputoutput import inputoutput as iod
from modelstore import modelstore as MS
from dataanalysis import dataanalysis as da
import fullworker as ASYNC

dimension = 30
rbp = RandomBinaryProjections('rbp', 30)

num_engine = Engine(dimension, 
            lshashes=[rbp], 
            distance=EuclideanDistance())

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
            tasks_in_batch = tasks_in_batch + 1
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
    print("Assigning task to Q: " + str(q))
    return ASYNC.load_tables.apply_async(args=[task], queue=q)

def process_futures(futures, task):
    not_assigned = True
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
            break
    return not_assigned

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
            not_assigned = process_futures(futures, task)
            time.sleep(1)
    # Block until all futures are processed
    remaining_futures = len(futures.items())
    for k,v in futures.items():
        if v == None:
            print(str(k) + " is None")
            remaining_futures = remaining_futures - 1
            futures[k] = 1 # cannot delete while iterating
        elif v == 1:
            futures[k] = 1 # just continue loop
        elif v.ready():
            process_result(v)
        if remaining_futures == 0:
            break
        time.sleep(1)
    print("DONE")
    
def create_sim_graph_num(cgraph, num_eng, num_sig):
    '''
    Given the LSH indexed signatures and the signatures,
    creates the graphs (indexes)
    '''
    for ns in num_sig:
        (key, sig) = ns
        cgraph[key] = []
        print("sim to: " + str(key))
        N = num_eng.neighbours(np.array(sig))
        for n in N:
            (data, label, value) = n
            if value > 0.0001:
                continue
            tokens = label.split('%&%&%')
            label_key = (tokens[0], tokens[1])
            #print(str(label_key) + " -- " + str(value))
            print(str(key)+" -> "+str(label_key) +":"+str(value))
            cgraph[key].append(label_key)
        print(" ")
        print(" ")
        print(" ")
        #time.sleep(3)
    return cgraph

def create_sim_graph_text(cgraph, text_engine, text_sig, tfidf):
    rowidx = 0
    for ts in text_sig:
        (key, sig) = ts
        cgraph[key] = []
        sparse_row = tfidf.getrow(rowidx)
        rowidx = rowidx + 1
        dense = sparse_row.todense()
        array = dense.A[0]
        N = text_engine.neighbours(array)
        for n in N:
            (data, label, value) = n
            tokens = label.split('%&%&%')
            label_key = (tokens[0], tokens[1])
            cgraph[key].append(label_key)
    return cgraph

def serialize_model(cgraph, dbname):
    print("Storing graph (cache)...")
    serde.serialize_cached_graph(cgraph, dbname)
    print("Storing graph (cache)...DONE!")

def build_indexes():
    '''
    Reads signatures and creates similarity graphs
    '''
    num_sig = MS.get_numerical_signatures()
    print("Total numerical sig: " + str(len(num_sig)))
    st = time.time()
    for s in num_sig:
        (name, signature) = s
        (fname, cname) = name
        key = str(fname)+"%&%&%"+str(cname)
        num_engine.store_vector(np.array(signature), key)
    et = time.time()
    print("Total time to index all num sigs: " + str((et-st)))

    cgraph = OrderedDict()
    cgraph = create_sim_graph_num(cgraph, num_engine, num_sig)
    #create_sim_graph(num_engine, None, num_sig,  None)
    text_sig = MS.get_textual_signatures()
    docs = []
    for ts in text_sig:
        (name, signature) = ts
        doc = ' '.join(signature) 
        docs.append(doc)
    tfidf = da.get_tfidf_docs(docs)
    num_features = tfidf.shape[1]
    print("tfidf shape: " + str(tfidf.shape))

    text_engine = Engine(num_features,
            lshashes=[rbp],
            distance=CosineDistance())
 
    st = time.time()
    rowidx = 0
    for ts in text_sig:
        (name, signature) = ts
        (fname, cname) = name
        key = str(fname)+"%&%&%"+str(cname)
        sparse_row = tfidf.getrow(rowidx)
        dense = sparse_row.todense() 
        array = dense.A[0]
        #print(str(array))
        text_engine.store_vector(array, key)
        rowidx = rowidx + 1
    et = time.time()
    print("total store text: " + str((et-st)))

    cgraph=create_sim_graph_text(cgraph, text_engine, text_sig, tfidf)
    return cgraph

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
        cgraph = build_indexes()
        serialize_model(cgraph, dbname)
        print("DONE building indexes")
    
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
