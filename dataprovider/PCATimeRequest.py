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



class PCATimeRequest(BaseRequest):

  def __init__(self, request):
    super(PCATimeRequest, self).__init__(request)
    self.params_keys = [self.datasource_param, self.measurements_param, self.selectedLevels_param, self.timepoint_param]
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
    print(tick_samples)
    # get the min selected Level if aggregated at multiple levels

    result = None
    error = None
    response_status = 200

    try:
        # selectedLevelsDict = ast.literal_eval(self.params.get(self.selectedLevels_param))
        # minSelectedLevel = utils.find_min_level(self.params.get(self.datasource_param), selectedLevelsDict)
        minSelectedLevel = 2
    except:
        error_info = sys.exc_info()
        error = "%s %s %s" % str(error_info[0]), str(error_info[1]), str(error_info[2])
        response_status = 500
        return result, error, response_status

    markerGeneOrWgs = ""

    markerGeneOrWgsQyrStr = "MATCH (ds:Datasource {label: '%s'}) RETURN ds.sequencingType as sequencing_type" \
                            % self.params.get(self.datasource_param)

    dayListQryStr = "MATCH (s:Sample) WHERE s.SubjectID IN " + tick_samples + " RETURN DISTINCT(s.Day) as Day"
    dayListQryStr_res = utils.cypher_call(dayListQryStr)
    dayListQryStr_df = utils.process_result(dayListQryStr_res)

    day_list = dayListQryStr_df['Day'].values
    day_list_converted = []
    for d in day_list:
        day_list_converted.append(str(d))

    day_list_start_index = int(self.params.get(self.timepoint_param))
    if day_list_start_index < 0:
        day_list_start_index = 0

    if day_list_start_index > (len(day_list_converted) - 3):
        day_list_start_index = (len(day_list_converted) - 3)

    day_list_end_index = day_list_start_index + 3

    select_days = day_list_converted[day_list_start_index:day_list_end_index]

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
        qryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature) " \
                 "MATCH (su:Subject)-[:TIMEPOINT]->(sa:Sample) MATCH (sa)-[v:COUNT]->()<-[:LEAF_OF]-(f) " \
                 "WHERE (f.depth= toInt(%s)) AND su.SubjectID IN %s with distinct f, s, v.val as agg " \
                 "RETURN distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as end, f.start as start, " \
                 "f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order" \
                 % (self.params.get(self.datasource_param), str(minSelectedLevel), tick_samples)
    else:
        qryStr = "MATCH (ds:Datasource {label: '" + self.params.get(self.datasource_param) + "'})-[:DATASOURCE_OF]->" \
         "(:Feature)-[:PARENT_OF*]->(f:Feature)-[:LEAF_OF]->()<-[v:COUNT]-(s:Sample) WHERE (f.depth= toInt(" + str(minSelectedLevel) + ")) " \
         "AND s.SubjectID IN " + tick_samples + " AND s.Day IN " + str(select_days) + " with distinct f, s, SUM(v.val) " \
         "as agg RETURN distinct agg, s.SubjectID as SubjectID, s.Day as Day, s.AntiGiven as AntiGiven, s.AnyDayDiarrhea as AnyDayDiarrhea , " \
         "s.Diarrhea as Diarrhea, s.Dose as Dose, s.AntibPrev as AntibPrev," \
         " f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, " \
         "f.lineageLabel as lineageLabel, f.order as order"

    try:
        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)
        result = {}
        grouped_df = df.groupby('Day')
        iterator = 1
        for group, day_group in grouped_df:
            result["pcatime" + str(iterator)] = {}
            result["pcatime" + str(iterator)]["data"] = []
            forPCAmat = pandas.pivot_table(day_group, index=["label"], columns="SubjectID", values="agg", fill_value=0)
            forPCAmat = numpy.log2(forPCAmat + 1)
            pca = PCA(n_components = 2)
            pca.fit(forPCAmat)
            variance_explained = pca.explained_variance_ratio_

            cols = {}
            cols['PC1'] = pca.components_[0].tolist()
            cols['PC2']= pca.components_[1].tolist()

            variance_explained[0] = round(variance_explained[0]*100.0, 2)
            variance_explained[1] = round(variance_explained[1]*100.0, 2)
            result["pcatime" + str(iterator)]['pca_variance_explained'] = variance_explained
            subjects_group = forPCAmat.columns
            for i in range(0,len(subjects_group)):
                temp = {}
                temp['PC1'] = cols['PC1'][i]
                temp['PC2'] = cols['PC2'][i]
                subset_df = day_group.loc[day_group['SubjectID'] == str(subjects_group[i])]
                temp["AnyDayDiarrhea"] = subset_df['AnyDayDiarrhea'].values[0]
                result["pcatime" + str(iterator)]["data"].append(temp)
            iterator = iterator + 1

    except:
        error_info = sys.exc_info()
        error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
        response_status = 500

    return result, error, response_status



