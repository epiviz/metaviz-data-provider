from flask import Flask, jsonify, request
from py2neo import Graph
import pandas
import credential
# import ujson

app = Flask(__name__)
# app.json_encoder = MiniJSONEncoder
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

graph = Graph(password=credential.neo4j_password)
cypher = graph

# Function to handle access control allow headers
def add_cors_headers(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    if request.method == 'OPTIONS':
        response.headers['Access-Control-Allow-Methods'] = 'DELETE, GET, POST, PUT'
        headers = request.headers.get('Access-Control-Request-Headers')
        if headers:
            response.headers['Access-Control-Allow-Headers'] = headers
    return response

app.after_request(add_cors_headers)

# handle combined requests
@app.route('/api', methods=['POST', 'OPTIONS', 'GET'])
def post_combined():
    if request.method == 'OPTIONS':
        res = jsonify({})
        res.headers['Access-Control-Allow-Origin'] = '*'
        res.headers['Access-Control-Allow-Headers'] = '*'
        return res

    in_params_end = request.values['params[end]']
    in_params_start = request.values['params[start]']
    in_params_method = request.values['method']
    in_params_order = eval(request.values['params[order]'])
    in_params_selection = eval(request.values['params[selection]'])
    in_params_selectedLevels = eval(request.values['params[selectedLevels]'])
    in_samples = request.values['params[measurements]']
    reqId = request.values['id']

    tick_samples = in_samples.replace("\"", "\'")

    # get the min selected Level if aggregated at multiple levels 
    minSelectedLevel = 100
    for level in in_params_selectedLevels.keys():
        if in_params_selectedLevels[level] == 2 and int(level) < minSelectedLevel:
            minSelectedLevel = int(level)

    # user selection nodes for custom aggregation - decides the cut 
    selNodes = "["
    selFlag = 0
    for node in in_params_selection.keys():
        if in_params_selection[node] == 2:
            selNodes += "'" + node + "'"
            selFlag = 1

    if selFlag ==1:
        selNodes = selNodes[:-1]

    selNodes += "]"

    qryStr = "MATCH (f:Feature)-[:LEAF_OF]->()<-[v:VALUE]-(s:Sample) WHERE (f.depth='" + str(minSelectedLevel) + "' OR f.id IN " + selNodes + ") AND s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order"
    result = cypher.data(qryStr)

    # convert result to dataFrame
    df = pandas.DataFrame(result)

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
                for other in in_params_selection.keys():
                    if key != other:
                        if parent(key, other):
                            # if two nodes are aggregated within a subtree.
                            # choose the parent, remove the child nodes
                            df = df[~df['id'].str.contains(key)]
                            # look for level duplicates
                            df = df[~df['lineage'].str.contains(key)]
                        else:
                            # if aggregated node is at a higher level than minSelectedLevel
                            # remove child nodes at the minSelectedLevel
                            df = df[~(df['lineage'].str.contains(key) & ~df['id'].str.contains(key))]

    # create a pivot_table where columns are samples and rows are features
    df_pivot = pandas.pivot_table(df,rows=["id", "label", "index", "lineage", "lineageLabel", "start", "end", "order"], cols="s.id", values="agg", fill_value=0).sortlevel("index")

    # convert the pivot matrix into metaviz json format

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
    # res = ujson.dumps({"id":reqId, "error": errorStr, "result": resRowsCols})
    # return res
    return jsonify({"id":reqId, "error": errorStr, "result": resRowsCols})

# if child of parent (node id's), return true
def parent(child, parent):
    qryStr = "MATCH (f:Feature {id:'" + child + "'}) RETURN f.lineage as lineage"
    result = cypher.data(qryStr)
    if parent in result[0]['lineage']:
        # print parent, child, result[0]['lineage']
        return True

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5005)
