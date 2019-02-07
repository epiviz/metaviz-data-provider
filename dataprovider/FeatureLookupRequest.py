import utils
import pandas
import sys
import ujson
import numpy
from BaseRequest import BaseRequest
import ast
"""
.. module:: CombinedRequest
   :synopsis: Query Neo4j Sample nodes and compute aggregation function over selected Feature nodes

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""
class FeatureLookupRequest(BaseRequest):

    def __init__(self, request):
        super(FeatureLookupRequest, self).__init__(request)
        self.params_keys = [self.featureList_param, self.datasource_param]
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
        Aggregates counts to the selected nodes in the feature hierarchy and returns the counts for the samples
        selected.

        Args:
            in_params_start: Start of range for features to use during aggregation
            in_params_end: End of range for features to use during aggregation
            in_params_order: Order of features
            in_params_selection: Features nodes and the selection type of expanded, aggregated, or removed
            in_params_selectedLevels: Level of the hierarchy to use
            in_params_samples: Samples to compute aggregation with

        Returns:
            resRowsCols: Aggregated counts for the selected Features over the selected Samples
        """
        print("in get_data featureLookupRequest")
        print(self.params.get(self.featureList_param))
        result = None

        error = None

        response_status = 200
        qryStr = "MATCH (ds:Datasource {label: '%s'}) MATCH (ds)-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
                       "(f:Feature)  WHERE f.label IN %s RETURN distinct f.id as id, f.lineage as lineage, f.level as level," \
                       "f.start as start, f.taxonomy as taxonomy, f.label as label, f.leafIndex as leafIndex, f.parentId as parentId," \
                       "f.depth as depth, f.datasource as datasource, f.end as end, f.lineageLabel as lineageLabel, f.nchildren as nchildren, " \
                       "f.order as order, f.nleaves as nleaves " % (
                           self.params.get(self.datasource_param),
                           self.params.get(self.featureList_param))

        print(qryStr)
        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)
        print(df)

        colnames = df.columns.values.tolist()
        print(colnames)
        vals = []
        for index, rowsamp in df.iterrows():
            # rowsamp = rowsamp.tolist();
            print(rowsamp)
            print(rowsamp[colnames[0]])
            temp = {}

            for key in colnames:
                temp[key] = rowsamp[key]

            vals.append(temp)

        print(vals)

        result = {"data": vals}

        return result, error, response_status
