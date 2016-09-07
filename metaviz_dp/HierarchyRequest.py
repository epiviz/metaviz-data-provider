import utils

def get_data(in_params_selection, in_params_order, in_params_selected_levels, in_params_nodeId, in_params_depth):

    root_node = in_params_nodeId
    root_node = root_node.replace('"', "")

    taxonomy = False

    if len(root_node) == 0 or root_node == "0-0":
        root_node = "0-0"
        qryStr = "MATCH (f:Feature {id:'" + root_node + "'})-[:PARENT_OF*0..3]->(f2:Feature) " \
                                                        "with collect(f2) + f as nodesFeat unwind nodesFeat as ff " \
                                                        "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
                                                        "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as partition, " \
                                                        "ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as nchildren, ff.nleaves as nleaves, " \
                                                        "ff.order as order " \
                                                        "order by ff.depth, ff.leafIndex, ff.order"

        tQryStr = "MATCH (f:Feature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth"
        taxonomy = True
    else:
        qryStr = "MATCH (f:Feature {id:'" + root_node + "'})-[:PARENT_OF*0..3]->(f2:Feature) OPTIONAL MATCH (f)<-[:PARENT_OF]-(fParent:Feature) " \
                                                        "with collect(f2) + f + fParent as nodesFeat unwind nodesFeat as ff " \
                                                        "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
                                                        "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as partition, " \
                                                        "ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as nchildren, ff.nleaves as nleaves, " \
                                                        "ff.order as order " \
                                                        "order by ff.depth, ff.leafIndex, ff.order"

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

            result.taxonomy = tdf['taxonomy'].values.to_list()

        return result

def row_to_dict(row):
    toRet = {}
    # toRet['lineage'] = row['lineage']
    toRet['end'] = row['end']
    toRet['partition'] = None
    toRet['leafIndex'] = row['leafIndex']
    toRet['nchildren'] = row['nchildren']
    toRet['label'] = row['label']
    toRet['name'] = row['label']
    toRet['start'] = row['start']
    toRet['depth'] = row['depth']
    toRet['globalDepth'] = row['depth']
    # toRet['lineageLabel'] = row['lineageLabel']
    toRet['nleaves'] = row['nleaves']
    toRet['parentId'] = row['parentId']
    toRet['order'] = row['order']
    toRet['id'] = row['id']
    toRet['selectionType'] = 1
    toRet['taxonomy'] = 'taxonomy' + (str(int(toRet['depth']) + 1))
    toRet['size'] = 1
    toRet['children'] = []
    return toRet

def df_to_tree(root, df):

    children = df[df['parentId'] == root['id']]

    if len(children) == 0:
        root['children'] = None
        return root

    otherChildren = df[~(df['parentId'] == root['id'])]
    # children.sort_values('order')
    # for old version of pandas
    children.sort('order')
    # root['size'] = len(children)

    for index,row in children.iterrows():
        childDict = row_to_dict(row)
        subDict = df_to_tree(childDict, otherChildren)
        if subDict is None:
            root['children'] = None
        else:
            root['children'].append(subDict)

    return root
