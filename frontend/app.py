import os
import sys
import inspect
import json
from flask import Flask

# move to top level
currentdir = os.path.dirname(
    os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from api.apiutils import Scope, Relation

from modelstore.elasticstore import StoreHandler
from knowledgerepr import fieldnetwork
from algebra import API

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
safe_dict['db'] = Scope.DB
safe_dict['source'] = Scope.SOURCE
safe_dict['feld'] = Scope.FIELD
safe_dict['content'] = Scope.CONTENT

# short names for Relations
safe_dict['schema'] = Relation.SCHEMA
safe_dict['schema_sim'] = Relation.SCHEMA_SIM
safe_dict['content_sim'] = Relation.CONTENT_SIM
safe_dict['entity_sim'] = Relation.ENTITY_SIM
safe_dict['pkfk'] = Relation.PKFK

app = Flask(__name__)


@app.route('/query/<query>')
def query(query):
    try:
        res = eval(query, {"__builtins__": None}, safe_dict)
        res = json.dumps(res.data)
    except Exception as e:
        res = "error: " + str(e)
    return res


@app.route('/convert/<nid>')
def convert(nid):
    try:
        nid = int(nid)
        res = api._general_to_drs(nid)
        res = json.dumps(res.data)
    except Exception as e:
        res = "error: " + str(e)
    return res


if __name__ == '__main__':
    app.run()
