from flask import Flask, request, send_from_directory, url_for

app = Flask(__name__, static_url_path='')


@app.route('/')
def root():
    return app.send_static_file('index.html')


if __name__ == '__main__':
    print("Serving DoD's Virtual Schema interface")
    app.run(debug=True)
