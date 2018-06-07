import utils
import pandas
import sys
import numpy
from sklearn.decomposition import PCA
from BaseRequest import BaseRequest
import ast

"""
.. module:: PCARequest
   :synopsis: Query Neo4j Samples nodes and compute PCA over Features at specified level

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""



class PCARequest(BaseRequest):

  def __init__(self, request):
    super(PCARequest, self).__init__(request)
    self.params_keys = [self.start_param, self.end_param, self.datasource_param, self.measurements_param,
                        self.selectedLevels_param]
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
    """
    Computes PCA over the selected samples and the given level of the hierarchy

    Args:
     in_params_selectedLevels:  Level of hierarchy of features to compute PCA
     in_params_samples: Samples to use to compute PCA
     in_datasource: datasource to query

    Returns:
     resRowsCols: PCA for the samples at the selected level

    """

    tick_samples = self.params.get(self.measurements_param).replace("\"", "\'")

    # get the min selected Level if aggregated at multiple levels

    result = None
    error = None
    response_status = 200

    try:
        selectedLevelsDict = ast.literal_eval(self.params.get(self.selectedLevels_param))
        minSelectedLevel = utils.find_min_level(self.params.get(self.datasource_param), selectedLevelsDict)

    except:
        error_info = sys.exc_info()
        error = "%s %s %s" % str(error_info[0]), str(error_info[1]), str(error_info[2])
        response_status = 500
        return result, error, response_status

    markerGeneOrWgs = ""

    markerGeneOrWgsQyrStr = "MATCH (ds:Datasource {label: '%s'}) RETURN ds.sequencing_type as sequencing_type" \
                            % self.params.get(self.datasource_param)

    try:
        rq_res = utils.cypher_call(markerGeneOrWgsQyrStr)
        df = utils.process_result(rq_res)
        markerGeneOrWgs = df['sequencing_type'].values[0]

    except:
        error_info = sys.exc_info()
        error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
        response_status = 500
        return result, error, response_status


    if markerGeneOrWgs == "wgs":
        qryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)<-" \
                 "[v:COUNT]-(s:Sample) WHERE (f.depth= toInt(%s)) AND (f.start >= %s AND f.end <= %s) " \
                 "AND s.id IN %s with distinct f, s, v.val as agg " \
                 "RETURN distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as end, f.start as start, " \
                 "f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order" \
                 % (self.params.get(self.datasource_param), str(minSelectedLevel),
                    str(self.params.get(self.start_param)), str(self.params.get(self.end_param)), tick_samples)
    else:
        qryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-" \
                 "[:LEAF_OF]->()<-[v:COUNT]-(s:Sample) WHERE (f.depth= toInt(%s)) AND (f.start >= %s AND f.end <= %s)" \
                 "AND s.id IN %s with distinct f, " \
                 "s, SUM(v.val) as agg RETURN distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as " \
                 "end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, " \
                 "f.order as order" % (self.params.get(self.datasource_param), str(minSelectedLevel),
                                       str(self.params.get(self.start_param)), str(self.params.get(self.end_param)),
                                       tick_samples)
    try:
        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

        forPCAmat = pandas.pivot_table(df, index=["label"], columns="s.id", values="agg", fill_value=0)
    	forPCAmat = numpy.log2(forPCAmat + 1)

        pca = PCA(n_components = 2)
        pca.fit(forPCAmat)
        variance_explained = pca.explained_variance_ratio_

        cols = {}
        cols['PC1'] = pca.components_[0]
        cols['PC2']= pca.components_[1]

        # samplesQryStr = "MATCH (ds:Datasource {label: '" + self.params.get(self.datasource_param) + "'})-" \
        #                 "[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-[:LEAF_OF]->()<-[v:COUNT]-(s:Sample) " \
        #                 "WHERE s.id IN " + tick_samples + " RETURN DISTINCT s"

        samplesQryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)" \
                        "-[:LEAF_OF]->()<-[v:COUNT]-(s:Sample) WHERE s.id IN %s RETURN DISTINCT s" \
                        % (self.params.get(self.datasource_param), tick_samples)

        samples_rq_res = utils.cypher_call(samplesQryStr)
        samples_df = utils.process_result_graph(samples_rq_res)
        vals = []

        for index, row in samples_df.iterrows():
            temp = {}
            for key in row.keys().values:
                temp[key] = row[key]
            temp['PC1'] = cols['PC1'][index]
            temp['PC2'] = cols['PC2'][index]
            temp['sample_id'] = temp['id']
            del temp['id']
            vals.append(temp)

        result = {"data": vals}

        variance_explained[0] = round(variance_explained[0]*100.0, 2)
        variance_explained[1] = round(variance_explained[1]*100.0, 2)
        result['pca_variance_explained'] = variance_explained

    except:
        error_info = sys.exc_info()
        error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
        response_status = 500

    return result, error, response_status


