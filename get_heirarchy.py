#!flask/bin/python

from flask import Flask, jsonify
from flask import request
import copy
import credential
import pprint
from py2neo import Graph
import pandas
import numpy

app = Flask(__name__)

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

def returnChildren(diction):
    toRet = {}
    toRet['lineage'] = diction['lineage']
    toRet['end'] = diction['end']
    toRet['partition'] = diction['partition']
    toRet['leafIndex'] = diction['leafIndex']
    toRet['nchildren'] = diction['nchildren']
    toRet['label'] = diction['label']
    toRet['name'] = diction['label']
    toRet['start'] = diction['start']
    toRet['depth'] = diction['depth']
    toRet['globalDepth'] = diction['depth']
    toRet['lineageLabel'] = diction['lineageLabel']
    toRet['nleaves'] = diction['nleaves']
    toRet['parentId'] = diction['parentId']
    toRet['order'] = diction['order']
    toRet['id'] = diction['id']
    toRet['selectionType'] = 1
    toRet['size'] = 1
    toRet['taxonomy'] = 'taxonomy' + (str(int(toRet['depth'])+1))
    toRet['children'] = None
    return toRet

app.after_request(add_cors_headers)

# Function for performing SQL query to retrieve measurements with filters and pagination
@app.route('/api', methods=['POST'])
def post_measurements():
    if request.method == 'OPTIONS':
        res = jsonify({})
        res.headers['Access-Control-Allow-Origin'] = '*'
        res.headers['Access-Control-Allow-Headers'] = 'origin, content-type, accept'
        return res
    print(request.method)
    print(request.values)
    print(request.values['id'])
    print(request.values['params[depth]'])
    print(request.values['params[nodeId]'])
    print(request.values['params[selection]'])
    print(request.values['method'])
    print(request.values['params[selectedLevels]'])
    print(request.values['params[order]'])

    in_params_order = request.values['params[order]']
    in_params_selection = request.values['params[selection]']
    in_params_method = request.values['method']
    in_params_selected_levels = request.values['params[selectedLevels]']
    in_params_nodeId = request.values['params[nodeId]']
    in_params_depth = request.values['params[depth]']

    if '-' not in in_params_nodeId:
        in_params_nodeId = "0-0"
        root_depth = 0
    else:
        root_depth = int(in_params_nodeId.split('-')[0].replace("\"", ""))

    depth = str(int(in_params_depth)+root_depth)
    if int(depth) > 6:
        depth = str(6)
    request.data = request.get_json()
    reqId = request.args.get('requestId')

    measurements = []

    dictionary = {}

    qryStr = "MATCH p=(f:Feature {id:'" + in_params_nodeId.replace("\"", "") + "'})-[*]->(f2:Feature {depth: '"+ depth + "'}) RETURN f AS root, COLLECT(f2) AS leaf, nodes(p) AS path"

    vals = ['lineage', 'start', 'label', 'leafIndex', 'parentId', 'depth', 'partition', 'end', 'id', 'lineageLabel', 'nchildren', 'nleaves', 'order']

    print(qryStr)
    result = cypher.run(qryStr)

    tree = []

    roots = []
    leaves = []
    paths = []
    for line in result:
        #print(line)
        #print(line.keys())
        roots.append(line['root'])
        leaves.append(line['leaf'][0])
        paths.append(line['path'])

    result = {}
    result['id'] = in_params_nodeId
    depth_1_children = {}
    depth_1_children_ret = []
    depth_1_children_index = {}
    depth_2_children = {}
    depth_2_children_index = {}
    depth_3_children_index = {}
    for j in range(0, len(paths)):
        if paths[j][1]['id'] not in depth_1_children.keys():
            depth_1_children[(paths[j][1]['id'])] = [returnChildren(paths[j][1])]
            depth_1_children_ret.append(returnChildren(paths[j][1]))
            depth_1_children_index[paths[j][1]['id']] = len(depth_1_children_ret)-1
        else:
            depth_1_children[(paths[j][1]['id'])].append(returnChildren(paths[j][1]))

    print(depth_1_children)
    print(depth_1_children_index)

    for j in range(0, len(paths)):
        parent_id = paths[j][2]['parentId']
        if depth_1_children[parent_id][0]['children'] is not None:
          to_iter = depth_1_children[parent_id][0]['children']
          in_iter = False
          for l in range(0, len (to_iter)):
            print(to_iter[l])
            if  paths[j][2]['id'] in to_iter[l].values():
                in_iter = True
                break
          if not in_iter:
            depth_1_children[parent_id][0]['children'].append(returnChildren(paths[j][2]))
            depth_1_children_ret[depth_1_children_index[parent_id]]['children'].append(returnChildren(paths[j][2]))
            depth_2_children[paths[j][2]['id']] = len(depth_1_children_ret[depth_1_children_index[parent_id]]['children'])-1
        else:
          depth_1_children[parent_id][0]['children'] = [returnChildren(paths[j][2])]
          depth_1_children_ret[depth_1_children_index[parent_id]]['children'] = [returnChildren(paths[j][2])]
          depth_2_children[paths[j][2]['id']] = 0
        if paths[j][2]['id'] in depth_2_children_index:
          continue
        else:
          depth_2_children_index[paths[j][2]['id']] = parent_id


    print(depth_2_children)

    for j in range(0, len(paths)):
        parent_id = paths[j][3]['parentId']
        depth_1_parent_id = depth_2_children_index[parent_id]
        to_find_index = depth_1_children[depth_1_parent_id][0]['children']
        for z in range(0, len(to_find_index)):
            if parent_id == to_find_index[z]['id']:
                break
        depth_2_child_list = depth_1_children[depth_1_parent_id][0]['children'][z]['children']
        if  depth_2_child_list is not None:
            to_iter = depth_2_child_list
            in_iter = False
            print(to_iter)
            for l in range(0, len (to_iter)):
              print(to_iter[l])
              if  paths[j][3]['id'] in to_iter[l].values():
                  in_iter = True
                  break
            if not in_iter:
                if parent_id in depth_2_children.keys():
                    print(depth_2_children.keys())
                depth_1_children[depth_1_parent_id][0]['children'][z]['children'].append(returnChildren(paths[j][3]))
                child_index = depth_2_children[parent_id]
                depth_1_children_ret[depth_1_children_index[parent_id]]['children'][child_index]['children'].append(returnChildren(paths[j][3]))
                print("appened child for " + paths[j][3]['parentId'])
        else:
            if parent_id in depth_2_children.keys():
                print(depth_2_children.keys())
            depth_1_children[depth_1_parent_id][0]['children'][z]['children'] = [returnChildren(paths[j][3])]
            child_index = depth_2_children[parent_id]
            depth_1_children_ret[depth_1_children_index[parent_id]]['children'][child_index]['children'] = [returnChildren(paths[j][3])]
            print("added child for " + paths[j][3]['parentId'])

    print(depth_1_children)

    result['lineage'] = roots[0]['lineage']
    result['end'] = roots[0]['end']
    result['partition'] = roots[0]['partition']
    result['leafIndex'] = roots[0]['leafIndex']
    result['nchildren'] = roots[0]['nchildren']
    result['label'] = roots[0]['label']
    result['name'] = roots[0]['label']
    result['start'] = roots[0]['start']
    result['depth'] = roots[0]['depth']
    result['globalDepth'] = roots[0]['depth']
    result['lineageLabel'] = roots[0]['lineageLabel']
    result['nleaves'] = roots[0]['nleaves']
    result['parentId'] = roots[0]['parentId']
    result['order'] = roots[0]['order']
    result['id'] = roots[0]['id']
    result['selectionType'] = 1
    result['size'] = 1
    result['taxonomy'] = 'taxonomy' + (str(int(result['depth'])+1))

    result['children'] = depth_1_children_ret

    errorStr = ""
    pageSize = str(10)
    #resRowsCols = {"cols": cols, "rows": rows, "globalStartIndex": (min(rows['start']))}
    res = jsonify({"id": request.values['id'], "error": errorStr, "result": result})
    #pprint.pprint(jsonify({"id": request.values['id'], "error": errorStr, "result": result}))
    return res


if __name__ == '__main__':
    app.run(debug=True, port=5002)

