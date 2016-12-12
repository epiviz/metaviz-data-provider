from flask import Flask, jsonify, request, Response
import ujson
import CombinedRequest, HierarchyRequest, MeasurementsRequest, PartitionsRequest, PCARequest, DiversityRequest, utils

app = Flask(__name__)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

"""
.. module:: metavizRoute
   :synopsis: Routing HTTP requests to appropriate query fucntion

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""

def add_cors_headers(response):
    """
    Add access control allow headers to response

    Args:
     response: Flask response to be sent

    Returns:
     response: Flask response with access control allow headers set
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = 'DELETE, GET, POST, PUT'
        headers = request.headers.get('Access-Control-Request-Headers')
        if headers:
            response.headers['Access-Control-Allow-Headers'] = headers
    return response

app.after_request(add_cors_headers)


# Route for POST, OPTIONS, and GET requests
@app.route('/api/', methods = ['POST', 'OPTIONS', 'GET'])
@app.route('/api', methods = ['POST', 'OPTIONS', 'GET'])
def process_api():
    """
    Send the request to the appropriate cypher query generation function.

    Args:

    Returns:
     res: Flask response containing query result
    """

    # For OPTIONS request, return an emtpy response
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
        result = HierarchyRequest.get_data(in_params_selection, in_params_order, in_params_selectedLevels,
                                           in_params_nodeId, in_params_depth)
        errorStr = None

    elif in_params_method == "partitions":
        result = PartitionsRequest.get_data()
        errorStr = None

    elif in_params_method == "measurements":
        result = MeasurementsRequest.get_data()
        errorStr = None

    elif in_params_method == "pca":
        in_params_selectedLevels = eval(request.values['params[selectedLevels]'])
        in_params_samples = request.values['params[measurements]']
        result = PCARequest.get_data(in_params_selectedLevels, in_params_samples)
        errorStr = None

    elif in_params_method == "diversity":
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
        result = CombinedRequest.get_data(in_params_start, in_params_end, in_params_order, in_params_selection,
                                          in_params_selectedLevels, in_params_samples)
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
