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

from api.reporting import Report
from api.apiutils import Scope, Relation

from modelstore.elasticstore import StoreHandler
from knowledgerepr import fieldnetwork
from algebra import API

path_to_serialized_model = parentdir + "/test/testmodel/"
network = fieldnetwork.deserialize_network(path_to_serialized_model)
store_client = StoreHandler()

api = API(network, store_client)

keyword_search = api.keyword_search
neighbor_search = api.neighbor_search
union = api.union
intersection = api.intersection
difference = api.difference

db = Scope.DB
source = Scope.SOURCE
feld = Scope.FIELD
content = Scope.CONTENT

schema = Relation.SCHEMA
schema_sim = Relation.SCHEMA_SIM
content_sim = Relation.CONTENT_SIM
entity_sim = Relation.ENTITY_SIM
pkfk = Relation.PKFK

app = Flask(__name__)

@app.route('/query/<query>')
def query(query):
    try:
        res = eval(query)
        res = json.dumps(res.data)
    except Exception as e:
        res = "error: " + str(e)
    return res


@app.route('/convert/<nid>')
def convert(nid):
    try:
        import pdb; pdb.set_trace()
        nid = int(nid)
        res = api._general_to_drs(nid)
        res = json.dumps(res.data)
    except Exception as e:
        res = "error: " + str(e)
    return res


if __name__ == '__main__':
    app.run()
