import utils
import pandas
import sys
import numpy
from sklearn.metrics.pairwise import pairwise_distances
from sklearn.manifold import MDS

"""
.. module:: PCARequest
   :synopsis: Query Neo4j Samples nodes and compute PCA over Features at specified level

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""

def get_data(in_params_selectedLevels, in_params_samples, in_datasource):
    """
    Computes PCoA over the selected samples and the given level of the hierarchy

    Args:
     in_params_selectedLevels:  Level of hierarchy of features to compute PCA
     in_params_samples: Samples to use to compute PCA
     in_datasource: datasource to query

    Returns:
     resRowsCols: PCA for the samples at the selected level

    """

    tick_samples = in_params_samples.replace("\"", "\'")

    # get the min selected Level if aggregated at multiple levels

    qryStr = "MATCH (s:Sample)-[:COUNT]->(f:Feature)<-[:LEAF_OF]-(:Feature)<-[:PARENT_OF*]-(:Feature)<-[:DATASOURCE_OF]-(ds:Datasource {label: '" + in_datasource + "'}) RETURN f.depth as depth  LIMIT 1"

    result = None
    error = None
    response_status = 200

    try:
        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

    except:
        error_info = sys.exc_info()
        error = str(error_info[0]) + " " + str(error_info[1]) + " " + str(error_info[2])
        response_status = 500
        return result, error, response_status


    minSelectedLevel = int(df['depth'].values[0])
    if minSelectedLevel is None:
        minSelectedLevel = 6

    for level in in_params_selectedLevels.keys():
        if in_params_selectedLevels[level] == 2 and int(level) < minSelectedLevel:
            minSelectedLevel = int(level)

    markerGeneOrWgs = ""

    markerGeneOrWgsQyrStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'}) RETURN ds.sequencing_type as sequencing_type"

    try:
        rq_res = utils.cypher_call(markerGeneOrWgsQyrStr)
        df = utils.process_result(rq_res)
        markerGeneOrWgs = df['sequencing_type'].values[0]

    except:
        error_info = sys.exc_info()
        error = str(error_info[0]) + " " + str(error_info[1]) + " " + str(error_info[2])
        response_status = 500
        return result, error, response_status


    if markerGeneOrWgs == "wgs":
        qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)<-[v:COUNT]-(s:Sample) WHERE (f.depth= toInt(" + str(minSelectedLevel) + ")) " \
         "AND s.id IN " + tick_samples + " with distinct f, s, v.val as agg RETURN distinct agg, s.id, f.label " \
         "as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, " \
         "f.lineageLabel as lineageLabel, f.order as order"
    else:
        qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-[:LEAF_OF]->()<-[v:COUNT]-(s:Sample) WHERE (f.depth= toInt(" + str(minSelectedLevel) + ")) " \
         "AND s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, f.label " \
         "as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, " \
         "f.lineageLabel as lineageLabel, f.order as order"

    try:
        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

        forPCoAmat = pandas.pivot_table(df, index=["label"], columns="s.id", values="agg", fill_value=0)
    	
        cols = {}

        # sklearn implementation 
        # metrics - http://scikit-learn.org/stable/modules/generated/sklearn.metrics.pairwise.pairwise_distances.html
        # MDS - http://scikit-learn.org/stable/modules/generated/sklearn.manifold.MDS.html#sklearn.manifold.MDS.fit
        distMat = pairwise_distances(forPCoAmat, metric="braycurtis")
        mds = MDS(n_components = 2, dissimilarity = "precomputed", n_init = 1, max_iter = 100, random_state = 1000)
        result = mds.fit_transform(distMat)

        samplesQryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-[:LEAF_OF]->()<-[v:COUNT]-(s:Sample) WHERE s.id IN " + tick_samples + " RETURN DISTINCT s"

        samples_rq_res = utils.cypher_call(samplesQryStr)
        samples_df = utils.process_result_graph(samples_rq_res)
        vals = []

        for index, row in samples_df.iterrows():
            temp = {}
            for key in row.keys().values:
                temp[key] = row[key]
            temp['PC1'] = result[index][0]
            temp['PC2'] = result[index][1]
            temp['sample_id'] = temp['id']
            del temp['id']
            vals.append(temp)

        result = {"data": vals}
    except:
        error_info = sys.exc_info()
        error = str(error_info[0]) + " " + str(error_info[1]) + " " + str(error_info[2])
        response_status = 500

    return result, error, response_status

