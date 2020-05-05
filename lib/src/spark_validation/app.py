import json
import sys

from flask import Flask, jsonify, request, make_response

from spark_validation.dataframe_validation import file_system_validator

application = Flask(__name__, static_url_path="")


@application.route("/")
def index():
    return application.send_static_file('index.html')

@application.route("/api/validate", methods=["POST"])
def validate():
    json_input = request.get_json(force=True)
    with open('config.json', 'w') as fp:
        json.dump(json_input, fp)
    sys.argv = ["example.py", "-c", 'config.json']

    print("JSON: {}".format(json_input))

    file_system_validator.init()
    response = {"validation": "yes"}

    return make_response(jsonify(response), 200)


if __name__ == "__main__":
    application.run(port=8000, debug=True)
