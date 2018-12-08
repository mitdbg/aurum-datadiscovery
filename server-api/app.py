import os
import sys
import argparse
import inspect
from flask import Flask, jsonify
from flask import request
from flask_cors import CORS, cross_origin
from flask import send_from_directory
import json
import pandas as pd
from DoD.dod import DoD
from DoD import data_processing_utils as dpu
import server_config as C

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


path_to_serialized_model = C.path_model
sep = C.separator
print("Configuring DoD with model: " + str(path_to_serialized_model) + " separator: " + str(sep))
network = fieldnetwork.deserialize_network(path_to_serialized_model)
store_client = StoreHandler()

global dod
dod = DoD(network=network, store_client=store_client, csv_separator=sep)

global matview
matview = None

app = Flask(__name__)
CORS(app)

# this supports exactly 1 session right now
global view_generator


@app.route('/test')
def test():
    return "received test ok"


@app.route("/testpost", methods=['POST'])
def testpost():
    # print(request.get_json())
    if request.method == 'POST':
        # print(json.loads(request.data))
        json_request = request.get_json()
        payload = json_request['payload']
        print("rcvd: " + str(payload))
        return jsonify({"myResp": "ok"})


@app.route("/findvs", methods=['POST'])
def findvs():
    if request.method == 'POST':
        json_request = request.get_json()
        payload_str = json_request['payload']
        payload = json.loads(payload_str)

        # Prepare input parameters to DoD
        list_attributes = ["" for k, v in payload.items() if k[0] == "0"]  # measure number attrs
        list_samples = ["" for el in list_attributes]  # template for samples, 1 row only for now
        for k, v in payload.items():
            row_idx = int(k[0])
            col_idx = int(k[2])
            if row_idx == 0:
                list_attributes[col_idx] = v
            else:
                list_samples[col_idx] = v

        # Obtain view - always create a new view_generator, we assume these are new views
        global dod
        global view_generator
        view_generator = iter(dod.virtual_schema_iterative_search(list_attributes, list_samples))
        mvs, attrs_to_project, view_metadata = next(view_generator)
        proj_view = dpu.project(mvs, attrs_to_project)
        analysis = obtain_view_analysis(proj_view)
        sample_view = proj_view.head(10)
        html_dataframe = sample_view.to_html()

        global matview
        matview = proj_view

        return jsonify({"view": html_dataframe, "analysis": analysis, "joingraph": view_metadata})


@app.route("/next_view", methods=['POST'])
def next_view():
    if request.method == 'POST':

        # Obtain view - always create a new view_generator, we assume these are new views
        global view_generator
        try:
            mvs, attrs_to_project, view_metadata = next(view_generator)
        except StopIteration:
            print("finished exploring views")
            return jsonify({"view": "no-more-views", "analysis": 'no'})
        proj_view = dpu.project(mvs, attrs_to_project)
        analysis = obtain_view_analysis(proj_view)
        sample_view = proj_view.head(10)
        html_dataframe = sample_view.to_html()

        global matview
        matview = proj_view

        return jsonify({"view": html_dataframe, "analysis": analysis, "joingraph": view_metadata})


@app.route("/suggest_field", methods=['POST'])
def suggest_field():
    if request.method == 'POST':
        json_request = request.get_json()
        input_text = json_request['input_text']

        suggestions = dod.aurum_api.suggest_schema(input_text)
        print(suggestions)
        output = {k: v for k, v in suggestions}

        return jsonify(output)


@app.route("/download_view", methods=['POST'])
def download_view():
    if request.method == 'POST':
        global matview
        matview.to_csv("/Users/ra-mit/development/discovery_proto/server-api/tmp/view.csv", encoding='latin1')
        return jsonify({"ok": "ok"})
        # return send_from_directory("tmp/", "test.csv")


def obtain_view_analysis(view):
    htmls = []
    for c in view.columns:
        html_repr = view[c].describe().to_frame().to_html()
        htmls.append(html_repr)
    return htmls

# @app.route('/convert/<input>')
# def convert(input):
#     try:
#         res = api._general_to_drs(input)
#         res = jsonify(res.__dict__())
#     except Exception as e:
#         res = "error: " + str(e)
#     return res

# @app.route('/inspect/<general_input>')
# def inspect(general_input):
#     drs = api._general_to_drs(general_input)
#     drs.set_table_mode()
#
#     #  if the DRS is empty, it was a bad request
#     if len(drs.data) < 1:
#         res = 'error:  The table or HIT, ' + general_input + 'does not exist';
#         return res;
#
#     # get the first nid from the drs. Doesn't matter which, since
#     # eventually getting the whole file, anyhow.
#     hit = drs.data[0]
#
#     data_frame = pandas_handler(store_handler=store_client, hit=hit, nrows=10)
#     return data_frame.to_json()


# @app.route('/clean/<general_source>/<general_target>')
# def clean(general_source, general_target):
#     my_dict = {
#         'source': general_source,
#         'target': general_target,
#         'message': 'dummy response from the "clean" endpoint'}
#
#     return jsonify(my_dict)


class Ack:
    status_code = 200

    def __init__(self):
        self.message = "ack"

    def to_dict(self):
        rv = dict()
        rv['message'] = self.message
        return rv


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

    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='nofile', help='path to aurum model')
    parser.add_argument('--separator', default=',', help='path to aurum model')

    args = parser.parse_args()

    if args.model == "nofile":
        print("Usage: --model <path_to_aurum_model>")
        exit()

    # basic test
    path_to_serialized_model = args.model
    sep = args.sep
    network = fieldnetwork.deserialize_network(path_to_serialized_model)
    store_client = StoreHandler()

    global dod
    dod = DoD(network=network, store_client=store_client, csv_separator=sep)

    app.run()
