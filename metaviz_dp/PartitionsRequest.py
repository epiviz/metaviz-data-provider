import utils
import ujson
import pandas

def get_data():
    qryStr = "MATCH (f:Feature {id:'0-0'}) RETURN  f.start as start, f.end as end"

    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)

    arr = []
    arr.append([None, df['start'][0], df['end'][0]])

    errorStr = None

    return arr