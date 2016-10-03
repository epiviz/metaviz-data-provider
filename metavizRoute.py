from flask import Flask, jsonify, request, Response
import ujson
import CombinedRequest, HierarchyRequest, MeasurementsRequest, PartitionsRequest, PCARequest, DiversityRequest, utils

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

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

@app.route('/api/', methods = ['POST', 'OPTIONS', 'GET'])
@app.route('/api', methods = ['POST', 'OPTIONS', 'GET'])
def process_api():
    if request.method == 'OPTIONS':
        res = jsonify({})
        res.headers['Access-Control-Allow-Origin'] = '*'
        res.headers['Access-Control-Allow-Headers'] = '*'
        return res

    in_params_method = request.values['method']

    if(in_params_method == "hierarchy"):
        in_params_order = eval(request.values['params[order]'])
        in_params_selection = eval(request.values['params[selection]'])
        in_params_selectedLevels = eval(request.values['params[selectedLevels]'])
        in_params_nodeId = request.values['params[nodeId]']
        in_params_depth = request.values['params[depth]']

        result = HierarchyRequest.get_data(in_params_selection, in_params_order, in_params_selectedLevels, in_params_nodeId, in_params_depth)
        errorStr = None
        # res = jsonify({"id": request.values['id'], "error": errorStr, "result": result})
    elif in_params_method == "partitions":
        result = PartitionsRequest.get_data()
        errorStr = None
    elif in_params_method == "measurements":
        errorStr = None
        result = MeasurementsRequest.get_data()
    elif in_params_method == "pca":
        errorStr = None
        in_params_selectedLevels = eval(request.values['params[selectedLevels]'])
        in_params_samples = request.values['params[measurements]']
        result = PCARequest.get_data(in_params_selectedLevels, in_params_samples)

        errorStr = None

    elif in_params_method == "diversity":
        errorStr = None
        in_params_selectedLevels = eval(request.values['params[selectedLevels]'])
        in_params_samples = request.values['params[measurements]']

        result = DiversityRequest.get_data(in_params_selectedLevels, in_params_samples)

        errorStr = None
    elif in_params_method == "combined":
        in_params_end = request.values['params[end]']
        in_params_start = request.values['params[start]']
        in_params_order = eval(request.values['params[order]'])
        in_params_selection = eval(request.values['params[selection]'])
        in_params_selectedLevels = eval(request.values['params[selectedLevels]'])
        in_params_samples = request.values['params[measurements]']

        result = CombinedRequest.get_data(in_params_start, in_params_end, in_params_order, in_params_selection, in_params_selectedLevels, in_params_samples)
        errorStr = None
    elif in_params_method == "search":
        in_param_datasource = request.values['params[datasource]']
        in_param_searchQuery = request.values['params[q]']
        in_param_maxResults = request.values['params[maxResults]']
        result = SearchRequest.get_data(in_param_datasource, in_param_searchQuery, in_param_maxResults)
        errorStr = None

    reqId = request.values['id']
    res = Response(response=ujson.dumps({"id": reqId, "error": errorStr, "result": result}), status=200,
                   mimetype="application/json")
    return res

if __name__ == '__main__':
    if(utils.check_neo4j()):
        app.run(debug=True, host="0.0.0.0")
    else:
        print("Neo4j is not running")
