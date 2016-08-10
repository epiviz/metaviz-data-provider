import credential
import requests as rqs
import ujson
import pandas

# process result from cypher into a data frame
def process_result(result):
    rows = []

    jsResp = ujson.loads(result.text)

    for row in jsResp["results"][0]["data"]:
        rows.append(row['row'])

    df = pandas.DataFrame(rows, columns=jsResp['results'][0]['columns'])
    return df

# make cypher qyery calls.
def cypher_call(query):
    headers = {'Content-Type': 'application/json'}
    data = {'statements': [{'statement': query, 'includeStats': False}]}

    rq_res = rqs.post(url='http://localhost:7474/db/data/transaction/commit', headers=headers, data=ujson.dumps(data),
                  auth=(credential.neo4j_username, credential.neo4j_password))
    return rq_res

# for every instance of Flask, check if neo4j is running.
def check_neo4j():

    try:
        rq_res = rqs.get(url='http://localhost:7474/db/data',
                         auth=(credential.neo4j_username, credential.neo4j_password))
    except rqs.exceptions.ConnectionError as err:
        return False

    return True
