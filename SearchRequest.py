import utils

def get_data(in_param_datasource, in_param_searchQuery, in_param_maxResults):

    qryStr = "MATCH (f:Feature {namespace: '" + in_param_datasource + "'}) WHERE f.label contains '" + in_param_searchQuery + "' " \
             "RETURN f.label as gene, f.start as start, f.end as end, 'neo4j' as seqName, f.id as nodeId, f.taxonomy as level " \
             "ORDER BY f.depth limit " + in_param_maxResults

    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)

    result = []

    for index, row in df.iterrows():
        temp = row.to_dict()
        result.append(temp)

    return result
