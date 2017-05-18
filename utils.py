import credential
import requests as rqs
import ujson
import pandas

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

    rq_res = rqs.post(url='http://localhost:7474/db/data/transaction/commit', headers=headers, data=ujson.dumps(data),
                  auth=(credential.neo4j_username, credential.neo4j_password))
    return rq_res

def workspace_request(wid, query):
    """
    Route query to the neo4j REST api.  This showed the best performance compared to py2neo and python neo4j driver

    Args:
     query: Cypher query to send to Neo4j

    Returns:
     rq_res: Cypher query response
    """
    # headers = {'Content-Type': 'application/json'}

    query_url = "http://metaviz.cbcb.umd.edu/data/main.php?requestId=8&version=4&action=getWorkspaces&ws=" + wid
    if query is None or query == "":
	    query_url = query_url + "&q="
    else:
	    query_url = query_url + "&q=" + query

    print(query_url)
    rq_res = rqs.get(url=query_url)
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
        rq_res = rqs.get(url='http://localhost:7474/db/data',
                         auth=(credential.neo4j_username, credential.neo4j_password))
    except rqs.exceptions.ConnectionError as err:
        return False

    return True
