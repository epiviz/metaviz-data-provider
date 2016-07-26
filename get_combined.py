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
    print(request.values['params[end]'])
    print(request.values['params[measurements]'])
    print(request.values['params[order]'])
    print(request.values['params[start]'])
    print(request.values['params[selection]'])
    print(request.values['method'])
    print(request.values['params[selectedLevels]'])
    print(request.values['params[partition]'])

    in_params_end = request.values['params[end]']
    in_params_order = request.values['params[order]']
    in_params_start = request.values['params[start]']
    in_params_selection = request.values['params[selection]']
    in_params_method = request.values['method']
    in_params_selected_levels = request.values['params[selectedLevels]']
    in_params_partition = request.values['params[partition]']
    in_samples = request.values['params[measurements]']

    tick_samples = in_samples.replace("\"", "\'")
    print(tick_samples)
    request.data = request.get_json()
    reqId = request.args.get('requestId')

    measurements = []
    top_level_keys = ["rows", "cols"]

    dictionary = {}

    qryStr = "MATCH (f:Feature {depth:'3'}) MATCH (f2:Feature) MATCH (f3:Feature) MATCH (f4:Feature) MATCH (s:Sample) \
             MATCH (f)-[:PARENT_OF]->(f2) MATCH (f2)-[:PARENT_OF]->(f3) MATCH (f3)-[:PARENT_OF]->(f4) \
             MATCH (s)-[v:VALUE]->(f4) WHERE s.id IN " + tick_samples + " RETURN reduce(total = 0, n in collect(v.val) | total + n) AS agg, s.id, f.label, f.leafIndex, f.end, f.start, f.id, f.lineage, f.lineageLabel"
    print(qryStr)
    result = cypher.run(qryStr)

    list_result = list(result)
    df = pandas.DataFrame(list_result)
    df[0] = df[0].astype(float)
    df[3] = df[3].astype(int)
    df[4] = df[4].astype(int)
    df[5] = df[5].astype(int)

    rows_group = df.groupby([2, 3, 4, 5,6 ,7 ,8])

    gb_rows = rows_group.groups
    to_sort = []
    for key, values in gb_rows.iteritems():
        to_sort.append(key)

    to_sortDF = pandas.DataFrame(to_sort, columns = ['label', 'index', 'end', 'start', 'id', 'lineage', 'lineageLabel'])

    sorted = to_sortDF.sort('start')

    df_grouped = df.groupby([1])

    gb = df_grouped.groups
    cols = {}
    for key, values in gb.iteritems():
        col_val = numpy.zeros(len((sorted['end'].values).tolist()))
        print(key)
        print(values)
        print(df.iloc[values])
        for v in df.iloc[values].values:
            print(v)
            val_idx = pandas.Index(sorted['label']).get_loc(v[2])
            print(sorted['label'])
            print("val_idx " + str(val_idx))
            col_val[val_idx] = v[0]
        cols[key] = col_val.tolist()

    print(cols)


    #pprint.pprint(sorted)
    rows = {}

    rows['end'] =(sorted['end'].values).tolist()
    rows['index'] = (sorted['index'].values).tolist()
    rows['start'] = (sorted['start'].values).tolist()
    rows['metadata'] = dict()
    rows['metadata']['id'] = dict()
    rows['metadata']['label'] = dict()
    rows['metadata']['lineage'] = dict()
    rows['metadata']['taxonomy'] = dict()
    rows['metadata']['id'] = (sorted['id'].values).tolist()
    rows['metadata']['label'] = (sorted['label'].values).tolist()
    rows['metadata']['lineage'] = (sorted['lineage'].values).tolist()
    rows['metadata']['taxonomy'] = (sorted['lineageLabel'].values).tolist()


    z = numpy.zeros(shape = (len(tick_samples), len((sorted['end'].values).tolist())))

    print(rows)
    gb = df_grouped.groups

    print("About to print results")

    print("Printed results")

    errorStr = ""
    pageSize = str(10)
    resRowsCols = {"cols": cols, "rows": rows, "globalStartIndex": (min(rows['start']))}
    res = jsonify({"id": request.values['id'], "error": errorStr, "result": resRowsCols})
    pprint.pprint(jsonify({"id": request.values['id'], "error": errorStr, "result": ""}))
    return res


if __name__ == '__main__':
    app.run(debug=True)
