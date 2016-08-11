import utils
import ujson
import pandas

def get_data(in_params_selection, in_params_order, in_params_selected_levels, in_params_nodeId, in_params_depth):

    rootNode = "0-0"
    qryStr = "MATCH (f:Feature {id:'0-0'})-[:PARENT_OF*0..3]->(f2:Feature) " \
                "with collect(f2) + f as nodesFeat unwind nodesFeat as ff " \
                "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
                "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as partition, " \
                "ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as nchildren, ff.nleaves as nleaves, " \
                "ff.order as order " \
                "order by ff.leafIndex, ff.order"


    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)

    root = df[df['id'].str.contains(rootNode)].iloc[0]
    other = df[~df['id'].str.contains(rootNode)]

    rootDict = row_to_dict(root)
    resRows = df_to_tree(rootDict, other)

    print(ujson.dumps(resRows))

def row_to_dict(row):
    toRet = {}
    toRet['lineage'] = row['lineage']
    toRet['end'] = row['end']
    toRet['partition'] = row['partition']
    toRet['leafIndex'] = row['leafIndex']
    toRet['nchildren'] = row['nchildren']
    toRet['label'] = row['label']
    toRet['name'] = row['label']
    toRet['start'] = row['start']
    toRet['depth'] = row['depth']
    toRet['globalDepth'] = row['depth']
    toRet['lineageLabel'] = row['lineageLabel']
    toRet['nleaves'] = row['nleaves']
    toRet['parentId'] = row['parentId']
    toRet['order'] = row['order']
    toRet['id'] = row['id']
    toRet['selectionType'] = 1
    toRet['size'] = 1
    toRet['children'] = []
    return toRet

def df_to_tree(root, df):

    if len(df) == 0:
        root['children'] = None
        return

    children = df[df['parentId'].str.contains(root['id'])]
    otherChildren = df[~df['parentId'].str.contains(root['id'])]
    children.sort_values('order')

    for index,row in children.iterrows():
        childDict = row_to_dict(row)
        subDict = df_to_tree(childDict, otherChildren)
        root['children'].append(subDict)

    return root
