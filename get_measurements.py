#!flask/bin/python

from flask import Flask, jsonify
from flask import request
import copy
import credential
import pprint
from py2neo import Graph
import pandas
import numpy

app = Flask(__name__)

graph = Graph(password=credential.neo4j_password)
cypher = graph

# Function to handle access control allow headers
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = 'DELETE, GET, POST, PUT'
        headers = request.headers.get('Access-Control-Request-Headers')
        if headers:
            response.headers['Access-Control-Allow-Headers'] = headers
    return response


app.after_request(add_cors_headers)
#method:measurements
#params[annotation]:["sex","hmpbodysupersite","sampcollectdevice","envmatter","bodysite","visitno","runcenter","runid","description"]
#Name

# Function for performing SQL query to retrieve measurements with filters and pagination
@app.route('/api', methods=['POST'])
def post_measurements():
    if request.method == 'OPTIONS':
        res = jsonify({})
        res.headers['Access-Control-Allow-Origin'] = '*'
        res.headers['Access-Control-Allow-Headers'] = 'origin, content-type, accept'
        return res
    print(request.method)
    print(request.values)
    print(request.values['id'])
    print(request.values['params[annotation]'])

    #in_samples = request.values['params[measurements]']

    #tick_samples = in_samples.replace("\"", "\'")
    #print(tick_samples)
    request.data = request.get_json()
    reqId = request.args.get('requestId')

    measurements = []

    qryStr = "MATCH (s:Sample) RETURN s.id"
    print(qryStr)
    result = cypher.run(qryStr)

    list_result = list(result)

    print(result)
    print(list_result)
    print("About to print results")
    for j in range(0, len(list_result)):
        measurements.append(list_result[j]['s.id'])

    print("Printed results")
    #result["cols"] = {[]}
    # session.close()
    errorStr = ""
    result = {"id": measurements, "name":measurements, "datasourceGroup": "ihmp", "datasourceId": "ihmp", "defaultChartType": "", "type": "feature", "minValue": 0.0, "maxValue": 10.0, "annotation": [], "metadata": ["label", "id", "taxonomy1", "taxonomy2","taxonomy3","taxonomy4","taxonomy5","taxonomy6","taxonomy7", "lineage"]}

    res = jsonify({"id": request.values['id'], "error": errorStr, "result": result})
    pprint.pprint(jsonify({"id": request.values['id'], "error": errorStr, "result": result}))
    return res


if __name__ == '__main__':
    app.run(debug=True, port=5001)
