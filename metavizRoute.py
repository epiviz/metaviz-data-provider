from flask import Flask, jsonify, request, Response, redirect
from flask_cache import Cache
import ujson
import CombinedRequest, HierarchyRequest, MeasurementsRequest, PartitionsRequest, PCARequest, DiversityRequest, utils, SearchRequest, RedirectRequest

application = Flask(__name__)
cache = Cache(application, config={'CACHE_TYPE': 'simple'})
application.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

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

application.after_request(add_cors_headers)

# Route for POST, OPTIONS, and GET requests
@application.route('/api/ihmp_redirect/', methods = ['POST', 'OPTIONS', 'GET'])
def process_redirect():
    """
    Send a request to the RedirectRequest class to lookup Metaviz workspace id of iHMP file id

    Args:

    Returns:
     res: Flask redirect containing Metaviz workspace
    """

    # For OPTIONS request, return an emtpy response
    if request.method == 'OPTIONS':
        res = jsonify({})
        res.headers['Access-Control-Allow-Origin'] = '*'
        res.headers['Access-Control-Allow-Headers'] = '*'
        return res

    in_params_id = request.values['fid']

    if(in_params_id != None):
        redirect_location = RedirectRequest.get_data(in_params_id)

    res = redirect(redirect_location, code=302)
    return res


# Route for POST, OPTIONS, and GET requests
@application.route('/api/', methods = ['POST', 'OPTIONS', 'GET'])
@application.route('/api', methods = ['POST', 'OPTIONS', 'GET'])
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

    result = None
    errorStr = None
    response_status = 200

    if utils.check_neo4j() != True:
        errorStr = "Neo4j is not running"
        reqId = request.values['id']
        response_status = 500
        res = Response(response=ujson.dumps({"id": reqId, "error": errorStr, "result": result}), status=response_status,
                   mimetype="application/json")
        return res


    if(in_params_method == "hierarchy"):
        in_params_order = eval(request.values['params[order]'])
        in_params_selection = eval(request.values['params[selection]'])
        in_params_selectedLevels = eval(request.values['params[selectedLevels]'])
        in_params_nodeId = request.values['params[nodeId]']
        in_params_depth = request.values['params[depth]']
        in_datasource = request.values['params[datasource]']
        result,errorStr, response_status = HierarchyRequest.get_data(in_params_selection, in_params_order, in_params_selectedLevels,
                                           in_params_nodeId, in_params_depth, in_datasource)

    elif in_params_method == "partitions":
        in_datasource = request.values['params[datasource]']
        @cache.memoize()
        def partition_cache_call(in_datasource):
            return PartitionsRequest.get_data(in_datasource)
        result = partition_cache_call(in_datasource)
        errorStr = None
        response_status = 200

    elif in_params_method == "measurements":
        in_datasource = request.values['params[datasource]']
        @cache.memoize()
        def measurement_cache_call(in_datasource):
            return MeasurementsRequest.get_data(in_datasource)
        result = measurement_cache_call(in_datasource)
        errorStr = None
        response_status = 200

    elif in_params_method == "pca":
        in_datasource = request.values['params[datasource]']
        in_params_selectedLevels = eval(request.values['params[selectedLevels]'])
        in_params_samples = request.values['params[measurements]']
        result, errorStr, response_status = PCARequest.get_data(in_params_selectedLevels, in_params_samples, in_datasource)

    elif in_params_method == "diversity":
        in_datasource = request.values['params[datasource]']
        in_params_selectedLevels = eval(request.values['params[selectedLevels]'])
        in_params_samples = request.values['params[measurements]']
        result, errorStr, response_status = DiversityRequest.get_data(in_params_selectedLevels, in_params_samples, in_datasource)

    elif in_params_method == "combined":
        in_datasource = request.values['params[datasource]']
        in_params_end = request.values['params[end]']
        in_params_start = request.values['params[start]']
        in_params_order = eval(request.values['params[order]'])
        in_params_selection = eval(request.values['params[selection]'])
        in_params_selectedLevels = eval(request.values['params[selectedLevels]'])
        in_params_samples = request.values['params[measurements]']
        result, errorStr, response_status = CombinedRequest.get_data(in_params_start, in_params_end, in_params_order, in_params_selection,
                                          in_params_selectedLevels, in_params_samples, in_datasource)


    elif in_params_method == "search":
        in_param_datasource = request.values['params[datasource]']
        in_param_searchQuery = request.values['params[q]']
        in_param_maxResults = request.values['params[maxResults]']
        result, errorStr, response_status = SearchRequest.get_data(in_param_datasource, in_param_searchQuery, in_param_maxResults)

    reqId = request.values['id']
    res = Response(response=ujson.dumps({"id": reqId, "error": errorStr, "result": result}), status=response_status,
                   mimetype="application/json")
    return res

if __name__ == '__main__':
    application.run(debug=True)

