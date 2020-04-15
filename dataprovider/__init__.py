from flask import Flask, jsonify, request, Response
# from flask_cache import Cache
from flask_caching import Cache
import json
import utils

from BaseRequest import BaseRequest
from CombinedRequest import CombinedRequest
from MeasurementsRequest import MeasurementsRequest
from PartitionsRequest import PartitionsRequest
from HierarchyRequest import HierarchyRequest
from DiversityRequest import DiversityRequest
from SearchRequest import SearchRequest
from RedirectRequest import RedirectRequest
from WorkspaceRequest import WorkspaceRequest
from FeatureRequest import FeatureRequest
from PCARequest import PCARequest
from PCoARequest import PCoARequest



"""
.. module:: metavizRoute
   :synopsis: Routing HTTP requests to appropriate query fucntion

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

    The following resources were referenced while designing the package and classes:
    https://github.com/jkanche/epiviz_data_provider/
    https://github.com/pallets/flask/tree/master/examples/flaskr
    http://flask.pocoo.org/docs/0.12/patterns/packages/ 
"""

application = Flask(__name__)
cache = Cache(application, config={'CACHE_TYPE': 'simple', 'CACHE_DEFAULT_TIMEOUT': 0})
application.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

def create_request(action, request):
    """

    :param action:
    :param request:
    :return:
    """

    request_mapping = {}

    request_mapping['hierarchy'] = HierarchyRequest
    request_mapping['partitions'] = PartitionsRequest
    request_mapping['measurements'] = MeasurementsRequest
    request_mapping['pca'] = PCARequest
    request_mapping['pcoa'] = PCoARequest
    request_mapping['diversity'] = DiversityRequest
    request_mapping['combined'] = CombinedRequest
    request_mapping['featureData'] = FeatureRequest
    request_mapping['search'] = SearchRequest

    new_request = request_mapping[action](request)
    return new_request

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

@application.route("/api/", methods = ["POST", "OPTIONS", "GET"])
@application.route("/api", methods = ["POST", "OPTIONS", "GET"])
@application.route("/", methods = ["POST", "OPTIONS", "GET"])

def process_request():
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

    param_method = request.values.get("method")
    req_id = request.values.get("id")

    metaviz_request = create_request(param_method, request.values)

    if (request.values.get("method") in ["partitions", "measurements"]):
        @cache.memoize()
        def partitions_measurements_cache_call(datasource, method):
            return metaviz_request.get_data()

        result, errorStr, response_status = partitions_measurements_cache_call(request.values.get("params[datasource]"), request.values.get("method"))

    else:
        result, errorStr, response_status = metaviz_request.get_data()

    res = Response(response=json.dumps({"id": int(req_id), "error": errorStr, "result": result}),
                   status=response_status, mimetype="application/json")
    return res
