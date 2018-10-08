import utils
from dataprovider.BaseRequest import BaseRequest
import sys

"""
.. module:: PartitionsRequest
   :synopsis: Query Neo4j root Feature nodes and return range of all Features

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""

class PartitionsRequest(BaseRequest):

  def __init__(self, request):
    super(PartitionsRequest, self).__init__(request)
    self.params_keys = [self.datasource_param]
    self.params = self.validate_params(request)

  #https://github.com/jkanche/epiviz_data_provider/blob/master/epivizws/requests.py
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
    """
    Returns the range of features in the database.  The cypher query finds the root of the Neo4j feature hierarchy and
    retrieves the start and end values which denote the range of features.

    Args:
     in_datasource: namspace to query

    Returns:
     arr: Feature range under root of tree
    """

    qryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(f:Feature {id:'0-0'}) RETURN f.start as start, " \
             "f.end as end" % (self.params.get(self.datasource_param))
    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)

    arr = []
    arr.append([self.params.get(self.datasource_param), df['start'][0], df['end'][0]])

    return arr, None, 200