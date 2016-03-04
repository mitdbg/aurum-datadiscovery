from celery import Celery
from celery import Task

import celeryconfig as CC
from dataanalysis import dataanalysis as da

app = Celery('ddworker', backend=CC.CELERY_RESULT_BACKEND, broker=CC.BROKER_URL)

class State():
    _pivotc = 0

    @property
    def pivotc(self):
        return self._pivotc

    @pivotc.setter
    def pivotc(self, value):
        self._pivotc = value

state = State()

@app.task()
def distribute_col(col):
    '''
    Updates the column for which we are computing indexes
    '''
    state.pivotc = col

@app.task()
def get_distributed_col():
    '''
    Return the current column for which we are computing indexes
    '''
    return state.pivotc

@app.task()
def compute_index(icol, datatype):
    '''
    Receives original data (probably pre-processed and cleaned)
    and the data type. It computes signature, similarity, overlap
    and whatever other information is necessary for indexing
    '''
    # Compute similarity first # FIXME: assuming sig is original data
    sim = None
    if datatype is 'N':
        sim = da.compare_pair_num_columns(state.pivotc, icol)#FIXME
    elif datatype is 'T':
        sim = da.compare_pair_text_columns(state.pivotc, icol)#FIXME
    # Compute overlap
    ove = da.compute_overlap(state.pivotc, icol)
    res = {'key': state.pivotc, 'col': icol, 'sim': sim, 'ove': ove}
    return res

