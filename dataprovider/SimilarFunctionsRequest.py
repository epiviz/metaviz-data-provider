import utils
import sys
import pandas
from BaseRequest import BaseRequest
import ast

"""
.. module:: HierarchyRequest
   :synopsis: Query Neo4j Feature nodes and return hierarchy of levels

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""
class SimilarFunctionsRequest(BaseRequest):
    def __init__(self, request):
        super(SimilarFunctionsRequest, self).__init__(request)
        self.params_keys = [self.featureList_param, self.datasource_param]
        self.params = self.validate_params(request)

    def validate_params(self, request):


        params = {}
        for key in self.params_keys:
            if request.has_key(key):
                params[key] = request.get(key)
            else:
                if key not in self.params_keys:
                   raise Exception("missing params in request")

        return params


    def get_data(self):
        # in_params_selection, in_params_order, in_params_selected_levels, in_params_nodeId, in_params_depth, in_datasource):
        """
        Finds and returns the hierarchy of the taxonomic features in the database. The hierarchy is traversed starting
        at the root node by using the PARENT_OF relationships the paths to until all leaf nodes are discovered.  The
        results are formatted according the the metaviz API specification.

        Args:
            in_params_selection: The samples selected
            in_params_order: The order of the features
            in_params_selected_levels: The levels for aggregation of each feature node or all nodes by default
            in_params_nodeId: The id of the root node
            in_params_depth: level depth to query at
            in_datasource: namespace to query
        Returns:
         result: Heirachy of levels in database

        """
        result = None
        error = None
        response_status = 200
        print(self.params.get(self.featureList_param))
        features = self.params.get(self.featureList_param)

        qryStr = "MATCH (ds:Datasource {label: 'picrust'})-[:DATASOURCE_OF]->()-[:PARENT_OF*]->(f:Feature) WHERE f.label IN " + features + " MATCH (f)-[:LEAF_OF]->()<-[:KO_TERM_OF]-()<-[:LEAF_OF]-(f2:Feature {depth:1}) " \
                  "return distinct f2.id as id"
        print(qryStr)

        try:
                rq_res = utils.cypher_call(qryStr)
                df = utils.process_result(rq_res)

                result = df['id']

        except:
                error_info = sys.exc_info()
                error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
                response_status = 500

        return result, error, response_status

