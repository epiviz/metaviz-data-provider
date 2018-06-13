import utils
import numpy as np
from BaseRequest import BaseRequest
import sys
"""
.. module:: MeasurementsRequest
   :synopsis: Query Neo4j Sample nodes and return node information

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""

class MeasurementsRequest(BaseRequest):

  def __init__(self, request):
      super(MeasurementsRequest, self).__init__(request)
      self.params_keys = [self.datasource_param]
      self.params = self.validate_params(request)

  def validate_params(self, request):
      params = {}

      for key in self.params_keys:
        if request.has_key(key):
            params[key] = request.get(key)
            print(params[key])
        else:
            if key not in self.params_keys:
                raise Exception("missing params in request")

      return params


  def get_data(self):
    """
    This function returns the set of all samples in the database.  The first cypher query is finding all samples in the
    database.  The second cypher query is used to find the mix and max count value for
    all features across all samples.  This is return along with data source information including name and taxonomic
    hierarchy level names.

    Args:
     in_datasource: namespace to query

    Returns:
     result: Sample nodes information in database
    """
    result = None
    error = None
    response_status = 200

    longitudinalQyrStr = "MATCH (ds:Datasource {label: '%s'}) RETURN ds.samplingType as sampling_type" \
                            % (self.params.get(self.datasource_param))
    try:
        rq_res = utils.cypher_call(longitudinalQyrStr)
        df = utils.process_result(rq_res)
        longitudinal = df['sampling_type'].values[0]
    except:
        error_info = sys.exc_info()
        error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
        response_status = 500
        return result, error, response_status

    if longitudinal:
        qryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->()-[LEAF_OF]->()<-[:COUNT]-(:Sample)-[:SAMPLE_OF]->(su:Subject) " \
                 "RETURN DISTINCT ds,su" % (self.params.get(self.datasource_param))
    else:
        qryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->()-[LEAF_OF]->()<-[:COUNT]-(s:Sample) " \
             "RETURN DISTINCT ds,s" % (self.params.get(self.datasource_param))

    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)
    measurements = []

    anno = []
    df.fillna(0, inplace=True)
    dsGroup = []
    dsId = []

    dsDescription = []
    dsSequencingType = []


    for index, row in df.iterrows():
        if longitudinal:
            temp = row['su']
            measurements.append(temp['SubjectID'])
            del temp['SubjectID']
        else:
            temp = row['s']
            measurements.append(temp['id'])
            del temp['id']
        anno.append(temp)
        dsGroup.append(row['ds']['label'])
        dsId.append(row['ds']['label'])
        dsDescription.append(row['ds']['description'])
        dsSequencingType.append(row['ds']['sequencingType'])

    rowQryStr = "MATCH ()-[r]-() WHERE EXISTS(r.val) RETURN min(r.val) as minVal, max(r.val) as maxVal"

    rq_res2 = utils.cypher_call(rowQryStr)
    df2 = utils.process_result(rq_res2)

    result = {"id": measurements, "name": measurements, "datasourceGroup": dsGroup, "datasourceId": dsId,
              "datasourceDescription": dsDescription, "defaultChartType": "", "type": "feature",
              "minValue": df2['minVal'][0], "maxValue": df2['maxVal'][0], "annotation": anno,
              "metadata": ["label", "id", "taxonomy1", "taxonomy2", "taxonomy3", "taxonomy4", "taxonomy5", "taxonomy6",
              "taxonomy7", "lineage"], "sequencingType": dsSequencingType
             }

    return result, None, 200