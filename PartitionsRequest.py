import utils

"""
.. module:: PartitionsRequest
   :synopsis: Query Neo4j root Feature nodes and return range of all Features

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""

def get_data(in_datasource):
    """
    Returns the range of features in the database.  The cypher query finds the root of the Neo4j feature hierarchy and
    retrieves the start and end values which denote the range of features.

    Args:
     in_datasource: namspace to query

    Returns:
     arr: Feature range under root of tree
    """

    qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(f:Feature {id:'0-0'}) RETURN  f.start as start, f.end as end"

    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)

    arr = []
    arr.append([None, df['start'][0], df['end'][0]])

    return arr