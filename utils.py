import credential
import requests as rqs
import ujson
import pandas

def process_result(result):
    """
    Process result from cypher into a data frame with specified columns
    :param Cypher query response object
    :return: dataframe of cypher query response
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
    :param Cypher query response object
    :return: dataframe of cypher query response
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
    :param query: The cypher query to send to Neo4j
    :return: The cypher query response
    """
    headers = {'Content-Type': 'application/json'}
    data = {'statements': [{'statement': query, 'includeStats': False}]}

    rq_res = rqs.post(url='http://localhost:7474/db/data/transaction/commit', headers=headers, data=ujson.dumps(data),
                  auth=(credential.neo4j_username, credential.neo4j_password))
    return rq_res

def check_neo4j():
    """
    On start of application, checks that neo4j is running locally
    """
    try:
        rq_res = rqs.get(url='http://localhost:7474/db/data',
                         auth=(credential.neo4j_username, credential.neo4j_password))
    except rqs.exceptions.ConnectionError as err:
        return False

    return True
