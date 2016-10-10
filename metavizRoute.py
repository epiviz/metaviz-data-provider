import ujson
import utils
from flask import Flask, jsonify, request, Response
from Requests import create_request_param_dict, CombinedRequest, MeasurementsRequest, HierarchyRequest, PartitionsRequest, PCARequest, DiversityRequest


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
    print request.values
    if(in_params_method == "hierarchy"):
        param_dict = create_request_param_dict(order=eval(request.values['params[order]']),
                                               selection=eval(request.values['params[selection]']),
                                               selected_levels=eval(request.values['params[selectedLevels]']),
                                               node_id=request.values['params[nodeId]'],
                                               depth=request.values['params[depth]'])
        result = HierarchyRequest(param_dict).get_data()
        errorStr = None
        # res = jsonify({"id": request.values['id'], "error": errorStr, "result": result})

    elif in_params_method == "partitions":
        result = PartitionsRequest(create_request_param_dict()).get_data()
        errorStr = None

    elif in_params_method == "measurements":
        result = MeasurementsRequest(create_request_param_dict()).get_data()
        errorStr = None

    elif in_params_method == "pca":
        param_dict = create_request_param_dict(selected_levels=eval(request.values['params[selectedLevels]']),
                                               samples=request.values['params[measurements]'])
        result = PCARequest(param_dict).get_data()
        errorStr = None

    elif in_params_method == "diversity":
        param_dict = create_request_param_dict(selected_levels=eval(request.values['params[selectedLevels]']),
                                               samples=request.values['params[measurements]'])
        result = DiversityRequest(param_dict).get_data()
        errorStr = None

    elif in_params_method == "combined":

        param_dict = create_request_param_dict(end=request.values['params[end]'],
                                               start=request.values['params[start]'],
                                               order=eval(request.values['params[order]']),
                                               selection=eval(request.values['params[selection]']),
                                               selected_levels=eval(request.values['params[selectedLevels]']),
                                               samples=request.values['params[measurements]'])
        result = CombinedRequest(param_dict).get_data()
        errorStr = None

    reqId = request.values['id']
    res = Response(response=ujson.dumps({"id": reqId, "error": errorStr, "result": result}), status=200,
                   mimetype="application/json")
    return res


if __name__ == '__main__':
    if utils.check_neo4j():
        app.run(debug=True, host="0.0.0.0")
    else:
        print("Neo4j is not running")
