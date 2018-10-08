import utils
import sys
from BaseRequest import BaseRequest

class SearchRequest(BaseRequest):

  def __init__(self, request):
      super(SearchRequest, self).__init__(request)
      self.params_keys = [self.datasource_param, self.search_query_param, self.max_results_param]
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
    result = None
    error = None
    response_status = 200

    qryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature) WHERE " \
             "f.label contains '%s' RETURN f.label as gene, f.start as start, f.end as end, 'neo4j' as seqName, " \
             "f.id as nodeId, f.taxonomy as level ORDER BY f.depth limit %s" % (self.params.get(self.datasource_param), \
             self.params.get(self.search_query_param), self.params.get(self.max_results_param))
    try:
        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

        result = []

        for index, row in df.iterrows():
            temp = row.to_dict()
            result.append(temp)

    except:
        error_info = sys.exc_info()
        error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
        response_status = 500

    return result, error, response_status