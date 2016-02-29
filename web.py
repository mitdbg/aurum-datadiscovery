from flask import Flask
from flask import request
from flask import jsonify
from flask import send_from_directory
from flask import make_response 
from flask import current_app
from flask.ext.cors import CORS

import sys
from datetime import timedelta
from functools import update_wrapper

from api import p as API
from api import format_output_for_webclient
from api import load_precomputed_model_DBVersion

import webconfig as C

app = Flask(__name__)
app.debug = C.appdebug
CORS(app)

def cors(origin=None, methods=None, headers=None,
                max_age=21600, attach_to_all=True,
                automatic_options=True):
    if methods is not None:
        methods = ', '.join(sorted(x.upper() for x in methods))
    if headers is not None:
    # and not isinstance(headers, basestring):
        headers = ', '.join(x.upper() for x in headers)
    #if not isinstance(origin, basestring):
    #    origin = ', '.join(origin)
    if isinstance(max_age, timedelta):
        max_age = max_age.total_seconds()

    def get_methods():
        if methods is not None:
            return methods

        options_resp = current_app.make_default_options_response()
        return options_resp.headers['allow']

    def decorator(f):
        def wrapped_function(*args, **kwargs):
            if automatic_options and request.method == 'OPTIONS':
                resp = current_app.make_default_options_response()
            else:
                resp = make_response(f(*args, **kwargs))
            if not attach_to_all and request.method != 'OPTIONS':
                return resp

            h = resp.headers

            h['Access-Control-Allow-Origin'] = origin
            h['Access-Control-Allow-Methods'] = get_methods()
            h['Access-Control-Max-Age'] = str(max_age)
            if headers is not None:
                h['Access-Control-Allow-Headers'] = headers
            return resp

        f.provide_automatic_options = False
        return update_wrapper(wrapped_function, f)
    return decorator

@app.route('/')
def root():
    return 'hello world'

@app.route('/kwsearch')
def kwsearch():
    kw = request.args.get('kw')
    result = API.search_keyword(kw)
    format_result = format_output_for_webclient(result, False)
    json = {'result': format_result}
    return jsonify(json)

@app.route('/colsim')
def colsim():
    filename = request.args.get('filename')
    colname = request.args.get('colname')
    key = (filename, colname)
    result = API.columns_like(key)
    format_result = format_output_for_webclient(result, True)
    json = {'result': format_result}
    return jsonify(json)

@app.route('/colove')
def colove():
    filename = request.args.get('filename')
    colname = request.args.get('colname')
    key = (filename, colname)
    result = API.columns_joinable_with(key)
    format_result = format_output_for_webclient(result, True)
    json = {'result': format_result}
    return jsonify(json)

@app.route('/test')
def test():
    json = {'result': [
    {'files': [
      {'name': 'A',
      'schema': [
        {'name': 'id1', 'samples': ['1', '1', '1']},
        {'name': 'name1', 'samples': ['one', 'one']}
      ]},
      {'name': 'B',
      'schema': [
        {'name': 'id2', 'samples': ['2', '2', '2']},
        {'name': 'name2', 'samples': ['two', 'two', 'two']}
      ]},
      {'name': 'C',
      'schema': [
        {'name': 'id3', 'samples': ['3', '3', '3']},
        {'name': 'name3', 'samples': ['three', 'three', 'three']}
      ]},
      {'name': 'D',
      'schema': [
        {'name': 'id4', 'samples': ['0', '0', '0']},
        {'name': 'name4', 'samples': ['0', '0', '0']}
      ]}
    ]},
    {'files': [
      {'name': 'E',
      'schema': [
        {'name': 'id5', 'samples': ['0', '0', '0']},
        {'name': 'name5', 'samples': ['0', '0', '0']}
      ]},
      {'name': 'F',
      'schema': [
        {'name': 'id6', 'samples': ['0', '0', '0']},
        {'name': 'name6', 'samples': ['0', '0', '0']}
      ]},
      {'name': 'G',
      'schema': [
        {'name': 'id7', 'samples': ['0', '0', '0']},
        {'name': 'name7', 'samples': ['0', '0', '0']}
      ]},
      {'name': 'H',
      'schema': [
        {'name': 'id8', 'samples': ['0', '0', '0']},
        {'name': 'name8', 'samples': ['0', '0', '0']}
    ]}
    ]
    },
    {'files': [
      {'name': 'I',
      'schema': [
        {'name': 'id9', 'samples': ['0', '0', '0']},
        {'name': 'name9', 'samples': ['0', '0', '0']}
      ]},
      {'name': 'J',
      'schema': [
        {'name': 'id10', 'samples': ['0', '0', '0']},
        {'name': 'name10', 'samples': ['0', '0', '0']}
      ]},
      {'name': 'K',
      'schema': [
        {'name': 'id11', 'samples': ['0', '0', '0']},
        {'name': 'name11', 'samples': ['0', '0', '0']}
      ]}
    ]
    }
    ]}
    return jsonify(json)

if __name__ == '__main__':
    if len(sys.argv) >= 2:
        modelname = sys.argv[2]

    else:
        print("USAGE")
        print("db: the name of the model to serve")
        print("python web.py --db <db>")
        exit()
    modelname
    load_precomputed_model_DBVersion(modelname)
    app.run()
