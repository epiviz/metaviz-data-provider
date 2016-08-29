import utils
import pandas

def get_data(in_params_start, in_params_end, in_params_order, in_params_selection, in_params_selectedLevels, in_params_samples):

    #default aggregation
    # if len(in_params_selectedLevels) == 0:
    #     in_params_selectedLevels = {'3': 2}

    tick_samples = in_params_samples.replace("\"", "\'")

    # get the min selected Level if aggregated at multiple levels
    minSelectedLevel = 6
    for level in in_params_selectedLevels.keys():
        if in_params_selectedLevels[level] == 2 and int(level) < minSelectedLevel:
            minSelectedLevel = int(level)

    # user selection nodes for custom aggregation - decides the cut
    selNodes = "["
    selFlag = 0
    for node in in_params_selection.keys():
        if in_params_selection[node] == 2:
            selNodes += "'" +  node + "',"
            selFlag = 1

    if selFlag == 1:
        selNodes = selNodes[:-1]
    selNodes += "]"

    # if minSelectedLevel == 6:
    #     qryStr = "MATCH (f:Feature)<-[v:VALUE]-(s:Sample) USING INDEX s:Sample(id) WHERE (f.depth=" + str(
    #     minSelectedLevel) + " OR f.id IN " + selNodes + ") AND (f.start >= " + in_params_start + " AND f.end < " + in_params_end + ") AND s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order"
    # else:
    qryStr = "MATCH (f:Feature)-[:LEAF_OF]->()<-[v:VALUE]-(s:Sample) USING INDEX s:Sample(id) WHERE (f.depth=" + str(
        minSelectedLevel) + " OR f.id IN " + selNodes + ") AND (f.start >= " + in_params_start + " AND f.end <= " + in_params_end + ") AND s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order"

    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)

    if len(df) > 0:
        # change column type
        df['index'] = df['index'].astype(int)
        df['start'] = df['start'].astype(int)
        df['end'] = df['end'].astype(int)
        df['order'] = df['order'].astype(int)

        # update order based on req
        for key in in_params_order.keys():
            df.loc[df['id'] == key, 'order'] = in_params_order[key]

        for key in in_params_selection.keys():
            lKey = key.split('-')
            if int(lKey[0]) <= minSelectedLevel:
                if in_params_selection[key] == 0:
                    # user selected nodes to ignore!
                    df = df[~df['lineage'].str.contains(key)]
                elif in_params_selection[key] == 2:
                    df = df[~(df['lineage'].str.contains(key) & ~df['id'].str.contains(key))]

        # create a pivot_table where columns are samples and rows are features

        df_pivot = pandas.pivot_table(df, rows=["id", "label", "index", "lineage", "lineageLabel", "start", "end", "order"],
                                      cols="s.id", values="agg", fill_value=0).sortlevel("index")

        # for pandas > 0.17
        # df_pivot = pandas.pivot_table(df, index=["id", "label", "index", "lineage", "lineageLabel", "start", "end", "order"],
        #                               columns="s.id", values="agg", fill_value=0).sortlevel("index")

        cols = {}

        for col in df_pivot:
            cols[col] = df_pivot[col].values.tolist()

        rows = {}
        rows['metadata'] = {}

        metadata_row = ["end", "start", "index"]

        for row in df_pivot.index.names:
            if row in metadata_row:
                rows[row] = df_pivot.index.get_level_values(row).values.tolist()
            else:
                rows['metadata'][row] = df_pivot.index.get_level_values(row).values.tolist()

        errorStr = ""
        resRowsCols = {"cols": cols, "rows": rows, "globalStartIndex": (min(rows['start']))}

    else:
        cols = {}

        samples = eval(in_params_samples)

        for sa in samples:
            cols[sa] = []

        rows = { "end": [], "start": [], "index": [], "metadata": {} }
        resRowsCols = {"cols": cols, "rows": rows, "globalStartIndex": None}

    return resRowsCols
