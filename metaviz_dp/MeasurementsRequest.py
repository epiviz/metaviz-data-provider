import utils
import pandas


def get_data():
    measurements = []

    qryStr = "MATCH (s:Sample) RETURN s.id as id"

    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)

    measurements = df['id'].values

    errorStr = ""
    result = {"id": measurements, "name": measurements, "datasourceGroup": "ihmp", "datasourceId": "ihmp",
              "defaultChartType": "", "type": "feature", "minValue": 0.0, "maxValue": 10.0, "annotation": [],
              "metadata": ["label", "id", "taxonomy1", "taxonomy2", "taxonomy3", "taxonomy4", "taxonomy5",
                           "taxonomy6",
                           "taxonomy7", "lineage"]}

    return result