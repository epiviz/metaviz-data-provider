from flask import Flask, jsonify, request, Response
import pandas
import credential
import requests as rqs
import ujson

app = Flask(__name__)
# app.json_encoder = MiniJSONEncoder
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

# graph = Graph(password=credential.neo4j_password)
# cypher = graph

def returnChildren(diction):
    toRet = {}
    toRet['lineage'] = diction['lineage']
    toRet['end'] = int(diction['end'])
    toRet['partition'] = diction['partition']
    toRet['leafIndex'] = int(diction['leafIndex'])
    toRet['nchildren'] = int(diction['nchildren'])
    toRet['label'] = diction['label']
    toRet['name'] = diction['label']
    toRet['start'] = int(diction['start'])
    toRet['depth'] = int(diction['depth'])
    toRet['globalDepth'] = int(diction['depth'])
    toRet['lineageLabel'] = diction['lineageLabel']
    toRet['nleaves'] = int(diction['nleaves'])
    toRet['parentId'] = diction['parentId']
    toRet['order'] = int(diction['order'])
    toRet['id'] = diction['id']
    toRet['selectionType'] = 1
    toRet['size'] = 1
    toRet['taxonomy'] = 'taxonomy' + (str(int(toRet['depth'])+1))
    toRet['children'] = None
    return toRet


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

@app.route('/api', methods = ['POST', 'OPTIONS', 'GET'])
def process_api():
    if request.method == 'OPTIONS':
        res = jsonify({})
        res.headers['Access-Control-Allow-Origin'] = '*'
        res.headers['Access-Control-Allow-Headers'] = '*'
        return res

    in_params_method = request.values['method']

    if(in_params_method == "hierarchy"):
        in_params_order = request.values['params[order]']
        in_params_selection = request.values['params[selection]']
        in_params_method = request.values['method']
        in_params_selected_levels = request.values['params[selectedLevels]']
        in_params_nodeId = request.values['params[nodeId]']
        in_params_depth = request.values['params[depth]']
        nonRoot = False
        if '-' not in in_params_nodeId or '0-0' in in_params_nodeId:
            in_params_nodeId = "0-0"
            root_depth = 0
        else:
            root_depth = int(in_params_nodeId.split('-')[0].replace("\"", ""))
            nonRoot = True

        depth = str(int(in_params_depth) + root_depth)
        if int(depth) > 6:
            depth = str(6)
        request.data = request.get_json()
        reqId = request.args.get('requestId')

        measurements = []

        dictionary = {}

        if nonRoot:
            qryStr = "MATCH p=(fParent:Feature)-[:PARENT_OF]->(f:Feature {id:'" + in_params_nodeId.replace("\"",
                                                                                                           "") + "'})-[*]->(f2:Feature {depth: " + depth + "}) RETURN f AS root, COLLECT(f2) AS leaf, nodes(p) AS path, fParent"
        else:
            qryStr = "MATCH p=(f:Feature {id:'" + in_params_nodeId + "'})-[*]->(f2:Feature {depth: " + depth + "}) RETURN f AS root, COLLECT(f2) AS leaf, nodes(p) AS path"

        vals = ['lineage', 'start', 'label', 'leafIndex', 'parentId', 'depth', 'partition', 'end', 'id', 'lineageLabel',
                'nchildren', 'nleaves', 'order']

        # print(qryStr)
        # result = cypher.run(qryStr)
        roots = []
        leaves = []
        paths = []
        counter = 0
        parent = []
        result = cypher_call(qryStr)
        jsResp = ujson.loads(result.text)

        print(qryStr)
        print(result)
        print(jsResp)

        for row in jsResp["results"][0]["data"]:
            print(row['row'])
            if counter == 0 and nonRoot:
                parent.append(row['row'][3])
                counter = counter + 1
            roots.append(row['row'][0])
            leaves.append(row['row'][1])
            paths.append(row['row'][2])

        print(roots)

        tree = []

        result = {}
        result['id'] = in_params_nodeId
        depth_1_children = {}
        depth_1_children_ret = []
        depth_1_children_index = {}
        depth_2_children = {}
        depth_2_children_index = {}
        depth_3_children = {}
        depth_3_children_index = {}
        for j in range(0, len(paths)):
            if paths[j][1]['id'] not in depth_1_children.keys():
                depth_1_children[(paths[j][1]['id'])] = [returnChildren(paths[j][1])]
                depth_1_children_ret.append(returnChildren(paths[j][1]))
                depth_1_children_index[paths[j][1]['id']] = len(depth_1_children_ret) - 1
            else:
                depth_1_children[(paths[j][1]['id'])].append(returnChildren(paths[j][1]))

        for j in range(0, len(paths)):
            parent_id = paths[j][2]['parentId']
            if depth_1_children[parent_id][0]['children'] is not None:
                to_iter = depth_1_children[parent_id][0]['children']
                in_iter = False
                for l in range(0, len(to_iter)):
                    if paths[j][2]['id'] in to_iter[l].values():
                        in_iter = True
                        break
                if not in_iter:
                    depth_1_children[parent_id][0]['children'].append(returnChildren(paths[j][2]))
                    depth_1_children_ret[depth_1_children_index[parent_id]]['children'].append(
                        returnChildren(paths[j][2]))
                    depth_2_children[paths[j][2]['id']] = len(
                        depth_1_children_ret[depth_1_children_index[parent_id]]['children']) - 1
            else:
                depth_1_children[parent_id][0]['children'] = [returnChildren(paths[j][2])]
                depth_1_children_ret[depth_1_children_index[parent_id]]['children'] = [returnChildren(paths[j][2])]
                depth_2_children[paths[j][2]['id']] = 0
            if paths[j][2]['id'] in depth_2_children_index:
                continue
            else:
                depth_2_children_index[paths[j][2]['id']] = parent_id

        for j in range(0, len(paths)):
            parent_id = paths[j][3]['parentId']
            depth_1_parent_id = depth_2_children_index[parent_id]
            to_find_index = depth_1_children[depth_1_parent_id][0]['children']
            for z in range(0, len(to_find_index)):
                if parent_id == to_find_index[z]['id']:
                    break
            depth_2_child_list = depth_1_children[depth_1_parent_id][0]['children'][z]['children']
            if depth_2_child_list is not None:
                to_iter = depth_2_child_list
                in_iter = False
                # print(to_iter)
                for l in range(0, len(to_iter)):
                    # print(to_iter[l])
                    if paths[j][3]['id'] in to_iter[l].values():
                        in_iter = True
                        break
                if not in_iter:
                    if parent_id in depth_2_children.keys():
                        print(depth_2_children.keys())
                    depth_1_children[depth_1_parent_id][0]['children'][z]['children'].append(
                        returnChildren(paths[j][3]))
                    child_index = depth_2_children[parent_id]
                    depth_1_children_ret[depth_1_children_index[depth_1_parent_id]]['children'][child_index][
                        'children'].append(returnChildren(paths[j][3]))
                    # print("appened child for " + paths[j][3]['parentId'])
                    depth_3_children[paths[j][3]['id']] = len(
                        depth_1_children_ret[depth_1_children_index[depth_1_parent_id]]['children'][child_index][
                            'children']) - 1
            else:
                # if parent_id in depth_2_children.keys():
                    # print("key present")
                depth_1_children[depth_1_parent_id][0]['children'][z]['children'] = [returnChildren(paths[j][3])]
                child_index = depth_2_children[parent_id]
                depth_1_children_ret[depth_1_children_index[depth_1_parent_id]]['children'][child_index]['children'] = [
                    returnChildren(paths[j][3])]
                depth_3_children[paths[j][3]['id']] = 0
            if paths[j][3]['id'] in depth_3_children_index:
                continue
            else:
                depth_3_children_index[paths[j][3]['id']] = parent_id

        # print(depth_1_children)

        if nonRoot:
            for j in range(0, len(paths)):
                parent_id = paths[j][4]['parentId']
                depth_2_parent_id = depth_3_children_index[parent_id]
                depth_1_parent_id = depth_2_children_index[depth_2_parent_id]
                to_find_index = depth_1_children[depth_1_parent_id][0]['children']
                for z in range(0, len(to_find_index)):
                    if depth_2_parent_id == to_find_index[z]['id']:
                        break
                depth_2_child_list = depth_1_children[depth_1_parent_id][0]['children'][z]['children']
                for y in range(0, len(depth_2_child_list)):
                    if parent_id == depth_2_child_list[y]['id']:
                        break
                depth_3_child_list = depth_1_children[depth_1_parent_id][0]['children'][z]['children'][y]['children']
                if depth_3_child_list is not None:
                    to_iter = depth_3_child_list
                    in_iter = False
                    # print(to_iter)
                    for l in range(0, len(to_iter)):
                        print(to_iter[l])
                        if paths[j][4]['id'] in to_iter[l].values():
                            in_iter = True
                            break
                    if not in_iter:
                        if parent_id in depth_3_children.keys():
                            print(depth_3_children.keys())
                        depth_1_children[depth_1_parent_id][0]['children'][z]['children'][y]['children'].append(
                            returnChildren(paths[j][4]))
                        depth_2_child_index = depth_2_children[depth_2_parent_id]
                        child_index = depth_3_children[parent_id]
                        depth_1_children_ret[depth_1_children_index[depth_1_parent_id]]['children'][
                            depth_2_child_index]['children'][child_index]['children'].append(
                            returnChildren(paths[j][4]))
                else:
                    if parent_id in depth_3_children.keys():
                        print(depth_3_children.keys())
                    depth_1_children[depth_1_parent_id][0]['children'][z]['children'][y]['children'] = [
                        returnChildren(paths[j][4])]
                    depth_2_child_index = depth_2_children[depth_2_parent_id]
                    child_index = depth_3_children[parent_id]
                    depth_1_children_ret[depth_1_children_index[depth_1_parent_id]]['children'][depth_2_child_index][
                        'children'][child_index]['children'] = [returnChildren(paths[j][4])]
                    # print("added child for " + paths[j][3]['parentId'])

        result['lineage'] = roots[0]['lineage']
        result['end'] = int(roots[0]['end'])
        # result['partition'] = roots[0]['partition']
        result['partition'] = None
        result['leafIndex'] = int(roots[0]['leafIndex'])
        result['nchildren'] = int(roots[0]['nchildren'])
        result['label'] = roots[0]['label']
        result['name'] = roots[0]['label']
        result['start'] = int(roots[0]['start'])
        result['depth'] = int(roots[0]['depth'])
        result['globalDepth'] = int(roots[0]['depth'])
        result['lineageLabel'] = roots[0]['lineageLabel']
        result['nleaves'] = int(roots[0]['nleaves'])
        result['parentId'] = roots[0]['parentId']
        result['order'] = int(roots[0]['order'])
        result['id'] = roots[0]['id']
        result['selectionType'] = 1
        result['size'] = 1
        result['taxonomy'] = 'taxonomy' + (str(int(result['depth']) + 1))

        result['children'] = depth_1_children_ret

        finalResult = {}

        if nonRoot:
            result['lineage'] = parent[0]['lineage']
            result['end'] = int(parent[0]['end'])
            # result['partition'] = roots[0]['partition']
            result['partition'] = None
            result['leafIndex'] = int(parent[0]['leafIndex'])
            result['nchildren'] = int(parent[0]['nchildren'])
            result['label'] = parent[0]['label']
            result['name'] = parent[0]['label']
            result['start'] = int(parent[0]['start'])
            result['depth'] = int(parent[0]['depth'])
            result['globalDepth'] = int(parent[0]['depth'])
            result['lineageLabel'] = parent[0]['lineageLabel']
            result['nleaves'] = int(parent[0]['nleaves'])
            result['parentId'] = parent[0]['parentId']
            result['order'] = int(parent[0]['order'])
            result['id'] = parent[0]['id']
            result['selectionType'] = 1
            result['size'] = 1
            result['taxonomy'] = 'taxonomy' + (str(int(result['depth']) + 1))

            result['children'] = depth_1_children_ret

        errorStr = ""
        pageSize = str(10)
        res = jsonify({"id": request.values['id'], "error": errorStr, "result": result})
    elif in_params_method == "partitions":
        qryStr = "MATCH (f:Feature {id:'0-0'}) RETURN  f.start as start, f.end as end"

        rq_res = cypher_call(qryStr)
        df = process_result(rq_res)

        print(df)

        arr = []
        arr.append([None, df['start'][0], df['end'][0]])

        errorStr = None
        # res = jsonify({"id": request.values['id'], "error": errorStr, "result": [arr]})
        res = Response(response=ujson.dumps({"id": request.values['id'], "error": errorStr, "result": arr}),
                       status=200,
                       mimetype="application/json")
    elif in_params_method == "measurements":
        request.data = request.get_json()
        reqId = request.args.get('requestId')

        measurements = []

        qryStr = "MATCH (s:Sample) RETURN s.id as id"

        rq_res = cypher_call(qryStr)
        df = process_result(rq_res)

        print(df)

        measurements = df['id'].values

        errorStr = ""
        result = {"id": measurements, "name": measurements, "datasourceGroup": "ihmp", "datasourceId": "ihmp",
                  "defaultChartType": "", "type": "feature", "minValue": 0.0, "maxValue": 10.0, "annotation": [],
                  "metadata": ["label", "id", "taxonomy1", "taxonomy2", "taxonomy3", "taxonomy4", "taxonomy5",
                               "taxonomy6",
                               "taxonomy7", "lineage"]}

        # res = jsonify({"id": reqId, "error": errorStr, "result": result})
        res = Response(response=ujson.dumps({"id": reqId, "error": errorStr, "result": result}), status=200,
                       mimetype="application/json")
    elif in_params_method == "combined":
        in_params_method = request.values['method']
        in_params_end = request.values['params[end]']
        in_params_start = request.values['params[start]']
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
                selNodes += "'" + node + "',"
                selFlag = 1

        if selFlag == 1:
            selNodes = selNodes[:-1]
        selNodes += "]"

        qryStr = "MATCH (f:Feature)-[:LEAF_OF]->()<-[v:VALUE]-(s:Sample) USING INDEX s:Sample(id) WHERE (f.depth=" + str(
            minSelectedLevel) + " OR f.id IN " + selNodes + ") AND (f.start >= " + in_params_start + " AND f.end < " + in_params_end + ") AND s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order"

        rq_res = cypher_call(qryStr)
        df = process_result(rq_res)

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
        df_pivot = pandas.pivot_table(df,
                                      rows=["id", "label", "index", "lineage", "lineageLabel", "start", "end", "order"],
                                      cols="s.id", values="agg", fill_value=0).sortlevel("index")

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
        res = Response(response=ujson.dumps({"id": reqId, "error": errorStr, "result": resRowsCols}), status=200,
                       mimetype="application/json")
    return res

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
    return True

if __name__ == '__main__':
    app.run(debug=True, host="0.0.0.0", port=5006)
