import utils
import sys

def get_data(in_param_datasource, in_param_searchQuery, in_param_maxResults):
    result = None
    error = None

    qryStr = "MATCH (ds:Datasource {label: '" + in_param_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature) WHERE f.label contains '" + in_param_searchQuery + "' " \
             "RETURN f.label as gene, f.start as start, f.end as end, 'neo4j' as seqName, f.id as nodeId, f.taxonomy as level " \
             "ORDER BY f.depth limit " + in_param_maxResults
    try:
        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

        result = []

        for index, row in df.iterrows():
            temp = row.to_dict()
            result.append(temp)

    except:
        error_info = sys.exc_info()
        error = str(error_info[0]) + " " + str(error_info[1]) + " " + str(error_info[2])

    return result, error
