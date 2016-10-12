import utils

def get_data():
    measurements = []

    qryStr = "MATCH (s:Sample) RETURN s"

    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result_graph(rq_res)

    anno = []

    for index, row in df.iterrows():
        temp = row.to_dict()
        del temp['id']
        anno.append(temp)

    measurements = df['id'].values
    rowQryStr = "MATCH ()-[r]-() WHERE EXISTS(r.val) RETURN min(r.val) as minVal, max(r.val) as maxVal"

    rq_res2 = utils.cypher_call(rowQryStr)
    df2 = utils.process_result(rq_res2)

    errorStr = ""
    result = {"id": measurements, "name": measurements, "datasourceGroup": "msd16s", "datasourceId": "msd16s",
              "defaultChartType": "", "type": "feature", "minValue": df2['minVal'][0], "maxValue": df2['maxVal'][0], "annotation": anno,
              "metadata": ["label", "id", "taxonomy1", "taxonomy2", "taxonomy3", "taxonomy4", "taxonomy5",
                           "taxonomy6",
                           "taxonomy7", "lineage"]}

    return result
