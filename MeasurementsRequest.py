import utils

"""
.. module:: MeasurementsRequest
   :synopsis: Query Neo4j Sample nodes and return node information

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""

def get_data(in_datasource):
    """
    This function returns the set of all samples in the database.  The first cypher query is finding all samples in the
    database.  The second cypher query is used to find the mix and max count value for
    all features across all samples.  This is return along with data source information including name and taxonomic
    hierarchy level names.

    Args:
     in_datasource: namespace to query

    Returns:
     result: Sample nodes information in database
    """
    qryStr = "MATCH (n:Namespace {label: '" + in_datasource + "'})-[NAMESPACE_OF]->()-[LEAF_OF]->()<-[VALUE]-(s:Sample)" \
             "RETURN DISTINCT n,s"

    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result_graph(rq_res)

    measurements = []

    anno = []
    df.fillna(0, inplace=True)
    dsGroup = []
    dsId = []

    for index, row in df.iterrows():
        temp = row['s']
        measurements.append(temp['id'])
        del temp['id']
        anno.append(temp)
        dsGroup.append(row['n']['label'])
        dsId.append(row['n']['label'])

    rowQryStr = "MATCH ()-[r]-() WHERE EXISTS(r.val) RETURN min(r.val) as minVal, max(r.val) as maxVal"

    rq_res2 = utils.cypher_call(rowQryStr)
    df2 = utils.process_result(rq_res2)

    result = {"id": measurements, "name": measurements, "datasourceGroup": dsGroup, "datasourceId": dsId,
              "defaultChartType": "", "type": "feature", "minValue": df2['minVal'][0], "maxValue": df2['maxVal'][0],
              "annotation": anno,
              "metadata": ["label", "id", "taxonomy1", "taxonomy2", "taxonomy3", "taxonomy4", "taxonomy5", "taxonomy6","taxonomy7", "lineage"]}

    return result
