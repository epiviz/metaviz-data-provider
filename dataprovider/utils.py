import requests as rqs
import ujson
import pandas
from config import preset_configs

"""
.. module:: utils
   :synopsis: Send requests to cypher engine and process results

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""

def process_result(result):
    """
    Process result from cypher into a data frame with specified columns

    Args:
     result: Cypher query response object

    Returns:
     df: dataframe of cypher query response
    """
    rows = []

    jsResp = ujson.loads(result.text)

    for row in jsResp["results"][0]["data"]:
        rows.append(row['row'])

    df = pandas.DataFrame()

    if len(rows) > 0:
        df = pandas.DataFrame(rows, columns=jsResp['results'][0]['columns'])

    return df

def process_result_graph(result):
    """
    Process result from cypher for into a dataframe

    Args:
     result: Cypher query response object

    Returns:
     df: dataframe of cypher query response
    """
    rows = []

    jsResp = ujson.loads(result.text)

    for row in jsResp["results"][0]["data"]:
        rows.append(row['row'][0])

    df = pandas.DataFrame()

    if len(rows) > 0:
        df = pandas.DataFrame(rows)

    return df


def cypher_call(query):
    """
    Route query to the neo4j REST api.  This showed the best performance compared to py2neo and python neo4j driver

    Args:
     query: Cypher query to send to Neo4j

    Returns:
     rq_res: Cypher query response
    """
    headers = {'Content-Type': 'application/json'}
    data = {'statements': [{'statement': query, 'includeStats': False}]}

    rq_res = rqs.post(url=preset_configs.NEO4J_TRANSACTIONS_URL, headers=headers, data=ujson.dumps(data),
                  auth=(preset_configs.NEO4J_USER, preset_configs.NEO4J_PASSWORD))
    return rq_res

def check_neo4j():
    """
    On start of application, checks that neo4j is running locally

    Args:
        none

    Return:
        none
    """
    try:
        rq_res = rqs.get(url=preset_configs.NEO4J_DB_URL,
                         auth=(preset_configs.NEO4J_USER, preset_configs.NEO4J_PASSWORD))
    except rqs.exceptions.ConnectionError as err:
        return False

    return True

def workspace_request(wid, query):
    """
    Route query to the neo4j REST api.  This showed the best performance compared to py2neo and python neo4j driver

    Args:
     query: Cypher query to send to Neo4j

    Returns:
     rq_res: Cypher query response
    """
    # headers = {'Content-Type': 'application/json'}

    query_url = preset_configs.METAVIZ_WORKSPACE_URL + wid

    if query is None or query == "":
        query_url = query_url + "&q="
    else:
        query_url = query_url + "&q=" + query

    print(query_url)
    rq_res = rqs.get(url=query_url)
    return rq_res

def find_min_level(datasource_param, selectedLevels_param):
    qryStr = "MATCH (s:Sample)-[:COUNT]->(f:Feature)<-[:LEAF_OF]-(:Feature)<-[:PARENT_OF*]-(:Feature)<-" \
             "[:DATASOURCE_OF]-(ds:Datasource {label: '%s'}) " \
             "RETURN f.depth as depth  LIMIT 1" % datasource_param

    rq_res = cypher_call(qryStr)
    df = process_result(rq_res)


    minSelectedLevel = int(df['depth'].values[0])
    if minSelectedLevel is None:
        minSelectedLevel = 6

    if selectedLevels_param is None:
        return minSelectedLevel

    selectedLevelsDict = selectedLevels_param

    if (hasattr(selectedLevelsDict, "keys")):
        for level in selectedLevelsDict.keys():
            if selectedLevelsDict[level] == 2 and int(level) < minSelectedLevel:
                minSelectedLevel = int(level)

    return minSelectedLevel