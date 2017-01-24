import utils
import pandas
import copy
import math

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

    qryStr = "MATCH (s:Sample)-[:COUNT]->(f:Feature)<-[:LEAF_OF]-(:Feature)<-[:PARENT_OF*]-(:Feature)<-[:DATASOURCE_OF]-(ds:Datasource {label: '" + in_datasource + "'}) RETURN f.depth as depth  LIMIT 1"

    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)

    minSelectedLevel = int(df['depth'].values[0])
    if minSelectedLevel is None:
        minSelectedLevel = 6

    for level in in_params_selectedLevels.keys():
        if in_params_selectedLevels[level] == 2 and int(level) < minSelectedLevel:
            minSelectedLevel = int(level)

    qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-[:LEAF_OF]->()<-[v:COUNT]-(s:Sample) WHERE (f.depth=" + str(minSelectedLevel) + ") " \
        "AND s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, " \
        "f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, " \
        "f.lineageLabel as lineageLabel, f.order as order"

    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)

    forDiversityDF = df[["agg", "s.id", "label"]]

    forDiversityMat = pandas.pivot_table(df, index=["label"], columns="s.id", values="agg", fill_value=0)

    alphaDiversityVals = []
    cols = {}
    sample_ids = list(set(forDiversityDF["s.id"]))
    if diversity_type == "shannon":
      for i in range(0, len(sample_ids)):

        l = forDiversityMat.ix[:,i].get_values()
        props = copy.deepcopy(l)
        alphaDiversity = 0.0
        totalSum = 0.0
        for j in range(0, len(l)):
            totalSum = totalSum + l[j]

        for j in range(0, len(l)):
            props[j] = l[j]/totalSum

        for j in range(0, len(props)):
            alphaDiversity = alphaDiversity + (props[j] * math.log1p(props[j]))

        alphaDiversityVals.append(alphaDiversity)
        cols[forDiversityMat.columns.values[i]] = alphaDiversity

    sampleQryStr = "MATCH (s:Sample) WHERE s.id IN " + tick_samples + " RETURN s"

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

    resRowsCols = {"data": vals}

    return resRowsCols
