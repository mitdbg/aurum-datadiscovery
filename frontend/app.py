import os
import sys
import inspect
from flask import Flask, jsonify
from flask_cors import CORS, cross_origin

# move to top level and import some more things
currentdir = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from api.apiutils import Relation
from api.light_processing import pandas_handler
from modelstore.elasticstore import StoreHandler
from knowledgerepr import fieldnetwork
from algebra import API
from modelstore.elasticstore import KWType

path_to_serialized_model = parentdir + "/models/dwh3/"
network = fieldnetwork.deserialize_network(path_to_serialized_model)
store_client = StoreHandler()
api = API(network, store_client)

# add the tuple builtin function
safe_dict = {}
safe_dict['tuple'] = tuple

# short names for functions
safe_dict['keyword_search'] = api.keyword_search
safe_dict['neighbor_search'] = api.neighbor_search
safe_dict['union'] = api.union
safe_dict['intersection'] = api.intersection
safe_dict['difference'] = api.difference
safe_dict['_general_to_drs'] = api._general_to_drs

# Short names for scopes
# safe_dict['db'] = NA
safe_dict['source'] = KWType.KW_TABLE  # table/file/source name
safe_dict['field'] = KWType.KW_SCHEMA  # colum names/fields
safe_dict['content'] = KWType.KW_TEXT


# short names for Relations
safe_dict['schema'] = Relation.SCHEMA
safe_dict['schema_sim'] = Relation.SCHEMA_SIM
safe_dict['content_sim'] = Relation.CONTENT_SIM
safe_dict['entity_sim'] = Relation.ENTITY_SIM
safe_dict['pkfk'] = Relation.PKFK


app = Flask(__name__)
CORS(app)


memory_tables = set()

@app.route('/query/<query>')
def query(query):
    try:
        res = eval(query, {"__builtins__": None}, safe_dict)

        # memory tables
        res.set_table_mode()
        for t in res:
            memory_tables.add(t)

        # Repeat query to avoid dead iterator
        res = eval(query, {"__builtins__": None}, safe_dict)

        res = jsonify(res.__dict__())

        return res
    except Exception as e:
        res = "error: " + str(e)
        return InvalidUsage(res, status_code=400)
        # return res, invalid

@app.route('/export')
def export():
    print("MEMORY: " + str(memory_tables))

    import json
    sources_str = ",".join([str(sn) for sn in memory_tables])[:-1]
    json_dict = dict()
    json_dict["CSV"] = dict()
    json_dict["CSV"]["dir"] = "./"
    json_dict["CSV"]["table"] = sources_str
    json_obj = json.dumps(json_dict)
    with open("data.json", 'w') as f:
        f.write(json_obj)

    return jsonify("ok!")
    # try:
    #     res = eval(query, {"__builtins__": None}, safe_dict)
    #     res = jsonify(res.__dict__())
    #
    #     return res
    # except Exception as e:
    #     res = "error: " + str(e)
    #     return InvalidUsage(res, status_code=400)
    #     # return res, invalid


@app.route('/convert/<input>')
def convert(input):
    try:
        res = api._general_to_drs(input)
        res = jsonify(res.__dict__())
    except Exception as e:
        res = "error: " + str(e)
    return res

@app.route('/inspect/<general_input>')
def inspect(general_input):
    drs = api._general_to_drs(general_input)
    drs.set_table_mode()

    #  if the DRS is empty, it was a bad request
    if len(drs.data) < 1:
        res = 'error:  The table or HIT, ' + general_input + 'does not exist';
        return res;

    # get the first nid from the drs. Doesn't matter which, since
    # eventually getting the whole file, anyhow.
    hit = drs.data[0]

    data_frame = pandas_handler(store_handler=store_client, hit=hit, nrows=10)
    return data_frame.to_json()

@app.route('/clean/<general_source>/<general_target>')
def clean(general_source, general_target):
    my_dict = {
        'source': general_source,
        'target': general_target,
        'message': 'dummy response from the "clean" endpoint'}

    return jsonify(my_dict)





class InvalidUsage(Exception):
    status_code = 400

    def __init__(self, message, status_code=None, payload=None):
        Exception.__init__(self)
        self.message = message
        if status_code is not None:
            self.status_code = status_code
        self.payload = payload

    def to_dict(self):
        rv = dict(self.payload or ())
        rv['message'] = self.message
        return rv

if __name__ == '__main__':
    app.run()
