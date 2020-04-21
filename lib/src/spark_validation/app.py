import sys

from flask import Flask, jsonify, request, make_response

from spark_validation.dataframe_validation import file_system_validator

application = Flask(__name__)


@application.route("/")
def index():
    return "Data Validation Framework"


@application.route("/validate", methods=["POST"])
def validate():
    json_input = request.get_json(force=True)
    sys.argv = ["example.py", "-c", json_input["config_file"]]

    file_system_validator.init()
    response = {"validation": "yes"}
    return make_response(jsonify(response), 200)


if __name__ == "__main__":
    application.run(port=8000, debug=True)
