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
    print(request.values['params'])

    qryStr = "MATCH (f:Feature {id:'0-0'}) RETURN  f.start, f.end"
    print(qryStr)
    result = cypher.run(qryStr)

    print(result)
    arr = []
    arr.append(None)
    for line in result:
        print(line)
        print(line['f.start'])
        arr.append(line['f.start'])
        print(line['f.end'])
        arr.append(line['f.end'])

    print([arr])
    print("Printed results")
    errorStr = None
    res = jsonify({"id": request.values['id'], "error": errorStr, "result": [arr]})
    pprint.pprint(jsonify({"id": request.values['id'], "error": errorStr, "result": ""}))
    return res


if __name__ == '__main__':
    app.run(debug=True, port=5003)