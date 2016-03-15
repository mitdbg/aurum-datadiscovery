from celery import Celery
from celery import Task

import celeryconfig as CC
import api as API
import config as C
from dataanalysis import dataanalysis as da
from modelstore import modelstore as MS
from inputoutput import inputoutput as iod

app = Celery('fullworker',backend=CC.CELERY_RESULT_BACKEND,broker=CC.BROKER_URL)

@app.task()
def init_worker(dbname):
    MS.init(dbname, create_index=False)
    print("Initialized db: " + str(dbname))

@app.task()
def load_tables(batch_of_tasks):
    '''
    Loads each of the data columns in concepts, then 
    create a type-dependent signature, stores all data 
    in the store and sends the signature to the coordinator.  
    '''
    colsigs = []
    for task in batch_of_tasks:
        (source_input_type, arg, t) = task
        columns = iod.get_columns_from_csv_file(t)
        for column in columns.items():
            # basic preprocess and clean columns
            (clean_c, c_type) = API.clean_column(column)
            values = list(clean_c.values())[0]
            (f_name, c_name) = list(clean_c.keys())[0]
            num_data = []
            text_data = []
            sig = None
            if c_type is 'N':
                # num signature
                method = C.preferred_num_method
                #sig = da.get_num_dist(values, method)
                sig = da.get_numerical_signature(values, C.sig_v_size)
                num_data = values
            elif c_type is 'T':
                # text signature
                method = C.preferred_text_method
                #sig = da.get_textual_dist(values, method) 
                sig = da.get_textual_signature(values, C.sig_v_size)
                text_data = values
            # Load info to model store
            MS.new_column(f_name, 
                      c_name, 
                      c_type, 
                      sig, 
                      num_data, 
                      text_data)
            key = (f_name, c_name)
            colsig = (key, c_type, sig)
            colsigs.append(colsig)
        print("Processed signatures for: " + str(f_name))
    return colsigs

