import utils
import sys

"""
.. module:: HierarchyRequest
   :synopsis: Query Neo4j Feature nodes and return hierarchy of levels

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""

def get_data(in_params_selection, in_params_order, in_params_selected_levels, in_params_nodeId, in_params_depth, in_datasource):
    """
    Finds and returns the hierarchy of the taxonomic features in the database. The hierarchy is traversed starting
    at the root node by using the PARENT_OF relationships the paths to until all leaf nodes are discovered.  The
    results are formatted according the the metaviz API specification.

    Args:
        in_params_selection: The samples selected
        in_params_order: The order of the features
        in_params_selected_levels: The levels for aggregation of each feature node or all nodes by default
        in_params_nodeId: The id of the root node
        in_params_depth: level depth to query at
        in_datasource: namespace to query
    Returns:
     result: Heirachy of levels in database

    """
    root_node = in_params_nodeId
    root_node = root_node.replace('"', "")

    taxonomy = False
    result = None
    error = None
    response_status = 200

    if len(root_node) == 0 or root_node == "0-0":
        root_node = "0-0"
        qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(f:Feature {id:'" + root_node + "'})-[:PARENT_OF*0..3]->(f2:Feature) " \
                 "with collect(f2) + f as nodesFeat unwind nodesFeat as ff " \
                 "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
                 "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as partition, " \
                 "ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as nchildren, " \
                 "ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order ORDER by ff.depth, ff.leafIndex, ff.order"

        tQryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(f:Feature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth"
        taxonomy = True
    else:
        qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature {id:'" + root_node + "'})-[:PARENT_OF*0..3]->(f2:Feature) " \
                 "OPTIONAL MATCH (f)<-[:PARENT_OF]-(fParent:Feature) with collect(f2) + f + fParent as nodesFeat " \
                 "unwind nodesFeat as ff return distinct ff.lineage as lineage, ff.start as start, " \
                 "ff.label as label, ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, " \
                 "ff.partition as partition, ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, " \
                 "ff.nchildren as nchildren, ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order " \
                 "ORDER by ff.depth, ff.leafIndex, ff.order"
    try:
        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

        if len(df) > 0:
            # convert columns to int
            df['start'] = df['start'].astype(int)
            df['end'] = df['end'].astype(int)
            df['order'] = df['order'].astype(int)
            df['leafIndex'] = df['leafIndex'].astype(int)
            df['nchildren'] = df['nchildren'].astype(int)
            df['nleaves'] = df['nleaves'].astype(int)
            df['depth'] = df['depth'].astype(int)
            df['depth'] = df['depth'].astype(int)

            # restore current order, selection and levels from input params
            for key in in_params_order.keys():
                df.loc[df['id'] == key, 'order'] = in_params_order[key]

            for key in in_params_selection.keys():
                df.loc[df['id'] == key, 'selectionType'] = in_params_selection[key]

            for key in in_params_selected_levels.keys():
                df.loc[df['depth'] == int(key), 'selectionType'] = in_params_selected_levels[key]

            root = df.iloc[0]
            other = df.loc[1:,]

            rootDict = row_to_dict(root)
            result = df_to_tree(rootDict, other)

            if taxonomy:
                trq_res = utils.cypher_call(tQryStr)
                tdf = utils.process_result(trq_res)

                result['rootTaxonomies'] = tdf['taxonomy'].values.tolist()

    except:
        error_info = sys.exc_info()
        error = str(error_info[0]) + " " + str(error_info[1]) + " " + str(error_info[2])
        response_status = 500

    return result, error, response_status

def row_to_dict(row):
    """
    Helper function to format the response.

    Args:
        row: A row from the cypher response

    Returns:
        toRet: Dictionary to be loaded into a JSON response
    """
    toRet = {}
    toRet['end'] = row['end']
    toRet['partition'] = None
    toRet['leafIndex'] = row['leafIndex']
    toRet['nchildren'] = row['nchildren']
    toRet['label'] = row['label']
    toRet['name'] = row['label']
    toRet['start'] = row['start']
    toRet['depth'] = row['depth']
    toRet['globalDepth'] = row['depth']
    toRet['nleaves'] = row['nleaves']
    toRet['parentId'] = row['parentId']
    toRet['order'] = row['order']
    toRet['id'] = row['id']
    toRet['selectionType'] = 1
    toRet['taxonomy'] = row['taxonomy']
    toRet['size'] = 1
    toRet['children'] = []
    return toRet

def df_to_tree(root, df):
    """
    Helper function to convert dataframe to a tree formatted in JSON

    Args:
        root: The id of the root node of tree
        df: The cypher response object for query

    Returns:
        root: Tree at current step
    """
    children = df[df['parentId'] == root['id']]

    if len(children) == 0:
        root['children'] = None
        return root

    otherChildren = df[~(df['parentId'] == root['id'])]

    # children.sort_values('order')
    # for old version of pandas
    children.sort('order')

    for index,row in children.iterrows():
        childDict = row_to_dict(row)
        subDict = df_to_tree(childDict, otherChildren)
        if subDict is None:
            root['children'] = None
        else:
            root['children'].append(subDict)

    return root
