import utils
import pandas
import copy
import math
import sys
import numpy


"""
.. module:: DiversityRequest
   :synopsis: Query Neo4j Sample nodes and compute Diversity over selected level of Feature nodes

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""

def get_data(in_params_selectedLevels, in_params_samples, in_datasource):
    """
    Computes Alpha Diversity using the specified samples and level of hierarchy
    :param in_params_selectedLevels: Hierarchy level to compute Alpha Diversity
    :param in_params_samples: Samples to use for computing Alpha Diversity
    :return:

    Args:
        in_params_selectedLevels: Hierarchy level to compute Alpha Diversity
        in_params_samples: Samples to use for computing Alpha Diversity
        in_datasource: datasource to query
    Returns:
        resRowsCols: Alpha diversity for the samples at the selected level
    """

    tick_samples = in_params_samples.replace("\"", "\'")
    diversity_type = "shannon"
    # get the min selected Level if aggregated at multiple levels
    result = None
    error = None
    response_status = 200

    qryStr = "MATCH (s:Sample)-[:COUNT]->(f:Feature)<-[:LEAF_OF]-(:Feature)<-[:PARENT_OF*]-(:Feature)<-[:DATASOURCE_OF]-(ds:Datasource {label: '" + in_datasource + "'}) RETURN f.depth as depth  LIMIT 1"

    try:
        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

        minSelectedLevel = int(df['depth'].values[0])
        if minSelectedLevel is None:
            minSelectedLevel = 6

        for level in in_params_selectedLevels.keys():
            if in_params_selectedLevels[level] == 2 and int(level) < minSelectedLevel:
                minSelectedLevel = int(level)

    except:
        error_info = sys.exc_info()
        error = str(error_info[0]) + " " + str(error_info[1]) + " " + str(error_info[2])
        response_status = 500
        return result, error, response_status

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
         "AND s.id IN " + tick_samples + " with distinct f, s, v.val as agg RETURN distinct agg, s.id, " \
         "f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, " \
         "f.lineageLabel as lineageLabel, f.order as order"
    else:
        qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-[:LEAF_OF]->()<-[v:COUNT]-(s:Sample) WHERE (f.depth= toInt(" + str(minSelectedLevel) + ")) " \
         "AND s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, " \
         "f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, " \
         "f.lineageLabel as lineageLabel, f.order as order"

    try:
        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

        forDiversityDF = df[["agg", "s.id", "label"]]

        forDiversityMat = pandas.pivot_table(df, index=["label"], columns="s.id", values="agg", fill_value=0)

        alphaDiversityVals = []
        cols = {}
        sample_ids = list(set(forDiversityDF["s.id"]))
        if diversity_type == "shannon":
            for i in range(0, len(sample_ids)):
                col_vals = forDiversityMat.ix[:,i].get_values()
                props = list()
                totalSum = col_vals.sum()

                for k in range(0, len(col_vals)):
                    temp_prop = float(col_vals[k]/totalSum)
                    if temp_prop != 0.0:
                        props.append(float((temp_prop * math.log(temp_prop))))
                    else:
                        props.append(0.0)

                nd_props = numpy.asarray(props, dtype=float)
                alphaDiversity = -(nd_props.sum())

                alphaDiversityVals.append(alphaDiversity)
                cols[forDiversityMat.columns.values[i]] = alphaDiversity

        sampleQryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-[:LEAF_OF]->()<-[v:COUNT]-(s:Sample) WHERE s.id IN " + tick_samples + " RETURN DISTINCT s"

        sample_rq_res = utils.cypher_call(sampleQryStr)
        sample_df = utils.process_result_graph(sample_rq_res)

        vals = []
        for index, row in sample_df.iterrows():
            temp = {}
            for key in row.keys().values:
                temp[key] = row[key]
            temp['alphaDiversity'] = cols[row['id']]
            temp['sample_id'] = temp['id']
            del temp['id']
            vals.append(temp)

        result = {"data": vals}

    except:
        error_info = sys.exc_info()
        error = str(error_info[0]) + " " + str(error_info[1]) + " " + str(error_info[2])
        response_status = 500

    return result, error, response_status

