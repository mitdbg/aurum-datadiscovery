from celery import Celery
from celery import Task

import celeryconfig as CC
from dataanalysis import dataanalysis as da
from modelstore import modelstore as MS

app = Celery('ddworker', backend=CC.CELERY_RESULT_BACKEND, broker=CC.BROKER_URL)

class State():
    _concepts = 0

    @property
    def concepts(self):
        return self._concepts

    @concepts.setter
    def concepts(self, value):
        self._concepts = value

state = State()

@app.task()
def distribute_concepts(concepts):
    state.concepts = concepts
    print("Responsible for: " + str(len(concepts)) + " concepts")

@app.task()
def init_db(dbname):
    MS.init(dbname, create_index=False)
    print("Initialized db: " + str(dbname))

@app.task()
def compute_index(concept, icol, datatype):
    '''
    Receives a columns. Checks whether it is similar and overlaps
    with the current maintained ones
    '''
    sim_m = dict()
    sim_m[concept] = []
    ove_m = dict()
    ove_m[concept] = []
    for c in state.concepts:
        (c_values, c_type) = MS.get_values_and_type_of_concept(c)
        # Compute similarity
        if c_type != datatype:
            continue # incomparable different types
        if c_type is 'N':
            sim = da.compare_pair_num_columns(c_values, icol)
        if c_type is 'T':
            p_1 = ' '.join(c_values)
            p_2 = ' '.join(icol)
            sim = da.compare_pair_text_columns(p_1, p_2)
        if sim:
            sim_m[concept].append(c)
        # Compute overlap
        ove = da.compute_overlap_of_columns(c_values, icol)
        if ove:
            ove_m[concept].append(c)
    return (sim_m, ove_m)

@app.task
def test(name):
    print("TEST MSG RX I am : " + str(name))
    return True
