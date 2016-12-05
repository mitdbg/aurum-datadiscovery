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
from modelstore.elasticstore import StoreHandler
from knowledgerepr import fieldnetwork
from algebra import API
from modelstore.elasticstore import KWType

path_to_serialized_model = parentdir + "/test/testmodel/"
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
safe_dict['feld'] = KWType.KW_SCHEMA  # colum names/fields
safe_dict['content'] = KWType.KW_TEXT


# short names for Relations
safe_dict['schema'] = Relation.SCHEMA
safe_dict['schema_sim'] = Relation.SCHEMA_SIM
safe_dict['content_sim'] = Relation.CONTENT_SIM
safe_dict['entity_sim'] = Relation.ENTITY_SIM
safe_dict['pkfk'] = Relation.PKFK


app = Flask(__name__)
CORS(app)


@app.route('/query/<query>')
def query(query):
    try:
        res = eval(query, {"__builtins__": None}, safe_dict)
        res = jsonify(res.__dict__())

        return res
    except Exception as e:
        res = "error: " + str(e)
        return InvalidUsage(res, status_code=400)
        # return res, invalid


@app.route('/convert/<input>')
def convert(input):
    try:
        res = api._general_to_drs(input)
        res = jsonify({'data': res.data})
    except Exception as e:
        res = "error: " + str(e)
    return res


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
