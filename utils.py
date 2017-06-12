import credential
import ujson
import pandas
from neo4j.v1 import GraphDatabase

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
    df = pandas.DataFrame([r.values() for r in result], columns=result.keys())

    return df

def process_result_graph(result):
    """
    Process result from cypher for into a dataframe

    Args:
     result: Cypher query response object

    Returns:
     df: dataframe of cypher query response
    """
    first_row = result.records().next()['s']

    first_keys = first_row.keys()
    first_vals = first_row.values()

    first_df = pandas.DataFrame([first_vals], columns = first_keys)
    df = pandas.DataFrame([r['s'].values() for r in result], columns = first_keys)
    df = df.append(first_df)

    return df


def cypher_call(query):
    """
    Route query to the neo4j REST api.  This showed the best performance compared to py2neo and python neo4j driver

    Args:
     query: Cypher query to send to Neo4j

    Returns:
     rq_res: Cypher query response
    """
    
    driver = GraphDatabase.driver("bolt://localhost", auth=(credential.neo4j_username, credential.neo4j_password))
    result = driver.session().run(query)

    return result

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
