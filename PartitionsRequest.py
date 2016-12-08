import utils

def get_data():
    """
    Returns the range of features in the database.  The cypher query finds the root of the Neo4j feature hierarchy and
    retrieves the start and end values which denote the range of features.
    """
    qryStr = "MATCH (f:Feature {id:'0-0'}) RETURN  f.start as start, f.end as end"

    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)

    arr = []
    arr.append([None, df['start'][0], df['end'][0]])

    return arr