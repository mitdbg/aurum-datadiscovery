from flask import Flask

import webconfig as C

app.debug = C.appdebug
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello World!'

if __name__ == '__main__':
    app.run()
