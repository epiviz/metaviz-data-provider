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


app.after_request(add_cors_headers)

  # public function getCombined(array $measurements, $start, $end, $partition=null, array $metadata=null, $retrieve_index=true, $retrieve_end=true,
  #                             $offset_location=false, array $selection=null, array $order=null,
  #                             $aggregation_function=null, array $selected_levels=null) {
  #   if ($selection === null) { $selection = array(); }
  #   if ($order === null) { $order = array(); }
  #   $location_cols = array_flip(array('index', 'partition', 'start', 'end'));
  #   $columns = $this->getTableColumns(EpivizApiController::ROWS_TABLE);
  #   if ($metadata != null) {
  #     $safe_metadata_cols = array();
  #     foreach ($metadata as $col) {
  #       if (array_key_exists($col, $columns) && !array_key_exists($col, $location_cols)) {
  #         $safe_metadata_cols[] = $col;
  #       }
  #     }
  #     $metadata = $safe_metadata_cols;
  #   } else {
  #     $metadata = array();
  #     foreach ($columns as $col => $_) {
  #       if (!array_key_exists($col, $location_cols)) {
  #         $metadata[] = $col;
  #       }
  #     }
  #   }
  #   $levels = $this->getLevels();
  #   if ($aggregation_function == null) { $aggregation_function = 'average'; }
  #   $agg_func = $this->aggregatorFactory->get($aggregation_function);
  #   if (count($measurements) == 0) {
  #     return DatasourceTable::createEmpty($measurements, $metadata, $levels, $offset_location, $retrieve_index, $retrieve_end);
  #   }
  #   $node_ids = array_keys($selection + $order);
  #   $nodes = $this->getSiblings($node_ids, $selected_levels);
  #   $selection_nodes = $this->extractSelectionNodes($nodes, $selection, $selected_levels);
  #   $in_range_selection_nodes = $this->filterOutOfRangeSelectionNodes($selection_nodes, $start, $end);
  #   foreach ($in_range_selection_nodes as $node) {
  #     if ($node->selectionType == SelectionType::NODE) {
  #       if ($node->start < $start) {
  #         $start = $node->start;
  #       }
  #       if ($node->end > $end) {
  #         $end = $node->end;
  #       }
  #     }
  #   }
  #   list($selection_nodes_indexes, $start_index_collapse) = $this->calcSelectionNodeIndexes($selection_nodes, $selection, $selected_levels, $start, $end);
  #   $cond_selection_nodes = array_filter($in_range_selection_nodes, function(Node $node) { return $node->selectionType === SelectionType::NONE; });
  #   // Build correct select intervals
  #   $cond = implode(' OR ', array_fill(0, 1+count($cond_selection_nodes),
  #     sprintf($this->intervalQueryFormat, EpivizApiController::ROWS_TABLE, $partition == null ? '`partition` IS NULL' : '`partition` = ?')));
  #   $params = array();
  #   if ($partition != null) {
  #     $params[] = $partition;
  #   }
  #   $params[] = $start;
  #   $last_end = $start;
  #   foreach ($cond_selection_nodes as $node) {
  #     $params[] = $node->start;
  #     if ($partition != null) {
  #       $params[] = $partition;
  #     }
  #     $params[] = $last_end;
  #     $params[] = $node->start;
  #     if ($partition != null) {
  #       $params[] = $partition;
  #     }
  #     $params[] = $node->end;
  #     $last_end = $node->end;
  #   }
  #   $params[] = $end;
  #   if ($partition != null) {
  #     $params[] = $partition;
  #   }
  #   $params[] = $last_end;
  #   $params[] = $end;
  #   $db = $this->db;
  #   $db->beginTransaction();
  #   $db->query(sprintf('DROP TABLE IF EXISTS `%1$s`', EpivizApiController::TEMP_ROWS));
  #   $db->prepare(sprintf('CREATE TEMPORARY TABLE `%1$s` (PRIMARY KEY (`id`)) ENGINE=MEMORY AS (SELECT * FROM `%2$s` WHERE %3$s ORDER BY `index` ASC) ',
  #     EpivizApiController::TEMP_ROWS, EpivizApiController::ROWS_TABLE, $cond))->execute($params);
  #   $db->query(sprintf('DROP TABLE IF EXISTS `%1$s`', EpivizApiController::TEMP_HIERARCHY));
  #   // TODO: Test which is the faster one on the production database; currently, on machine running MySQL 5.6.12, the first runs faster
  #   // TODO:   On the machine running MySQL 5.1.73, the second one is faster.
  #   // $db->query(sprintf(
  #   //   'CREATE TEMPORARY TABLE `%1$s` (PRIMARY KEY (`id`)) ENGINE=MEMORY AS (SELECT * FROM `%2$s` WHERE `id` IN (SELECT `id` FROM `%3$s`) %4$s) ',
  #   //   EpivizApiController::TEMP_HIERARCHY, EpivizApiController::HIERARCHY_TABLE, EpivizApiController::TEMP_ROWS, $this->nodesOrderBy));
  #   $db->prepare(sprintf(
  #     'CREATE TEMPORARY TABLE `%1$s` (PRIMARY KEY (`id`)) ENGINE=MEMORY AS (SELECT * FROM `%2$s` WHERE `id` IN (SELECT `id` FROM `%3$s` WHERE %5$s) %4$s) ',
  #     EpivizApiController::TEMP_HIERARCHY, EpivizApiController::HIERARCHY_TABLE, EpivizApiController::ROWS_TABLE, $this->nodesOrderBy, $cond))
  #     ->execute($params);
  #   $db->query(sprintf('DROP TABLE IF EXISTS `%1$s`', EpivizApiController::TEMP_COLS));
  #   $db->prepare(sprintf(
  #     'CREATE TEMPORARY TABLE `%1$s` ENGINE=MEMORY AS (SELECT * FROM `%2$s` WHERE `id` IN (%3$s)) ',
  #     EpivizApiController::TEMP_COLS, EpivizApiController::COLS_TABLE, implode(',', array_fill(0, count($measurements), '?'))))
  #     ->execute($measurements);
  #   $db->commit();
  #   $fields = '`%1$s`.`index` AS `row`, `%1$s`.`start`';
  #   $fields .= ', `%4$s`.`lineagelabel`, `%4$s`.`lineage`, `%4$s`.`depth`';
  #   $fields .= ', `%3$s`.`val`, `%2$s`.`index` AS `col`, `%2$s`.`id` AS `measurement`';
  #   $fields .= ', `%1$s`.`end`';
  #   foreach ($metadata as $col) {
  #     $fields .= ', `%1$s`.`' . $col . '`';
  #   }
  #   $sql = sprintf(
  #     'SELECT '.$fields.' '
  #     .'FROM (`%1$s` JOIN `%4$s` ON `%1$s`.`id` = `%4$s`.`id`) JOIN `%2$s` '
  #     .'JOIN `%3$s` ON  (`%1$s`.`index` = `%3$s`.`row` AND `%2$s`.`index` = `%3$s`.`col`) '
  #     .'ORDER BY `%1$s`.`index` ASC ',
  #     EpivizApiController::TEMP_ROWS, EpivizApiController::TEMP_COLS, EpivizApiController::VALUES_TABLE, EpivizApiController::TEMP_HIERARCHY);
  #   $stmt = $db->prepare($sql);
  #   $stmt->execute();
  #   $unprocessed_tbl = DatasourceTable::createEmpty($measurements, $metadata, $levels, $offset_location, $retrieve_index, $retrieve_end);
  #   while (!empty($stmt) && ($r = ($stmt->fetch(PDO::FETCH_NUM))) != false) {
  #     $unprocessed_tbl->addDbRecord($r);
  #   }
  #   $unprocessed_tbl->finalize();
  #   $ret = $unprocessed_tbl->aggregate($selection_nodes, $agg_func, $start, $end);
  #   // Apply ordering
  #   if (!empty($order)) {
  #     $parent_ids = array_flip(array_map(function($node_id) use ($nodes) { return $nodes[$node_id]->parentId; }, array_keys($order)));
  #     $order_nodes = array_filter($nodes, function(Node $node) use ($parent_ids) { return array_key_exists($node->parentId, $parent_ids); });
  #     array_walk($order_nodes, function(Node &$node) use ($order) { $node->order = idx($order, $node->id, $node->order); });
  #     $ordered_interval_tree = new OrderedIntervalTree($order_nodes);
  #     $ret = $ret->reorder($ordered_interval_tree->orderIntervals($ret));
  #   }
  #   return $ret;
  # }

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

    request.data = request.get_json()
    reqId = request.args.get('requestId')

    #id:5
    # method:hierarchy
    # params[depth]:3
    # params[nodeId]:""
    # params[selection]:{}
    # params[order]:{}
    # params[selectedLevels]:{"3":2,"4":2}

# result : {id: "0-0", name: "k__Bacteria", label: "k__Bacteria", globalDepth: 0, depth: 0, taxonomy: "taxonomy1
# children : [{id: "1-0", name: "p__Actinobacteria", label: "p__Actinobacteria", globalDepth: 1, depth: 1
# depth : 0
# end : 953
# globalDepth : 0
# id : "0-0"
# label : "k__Bacteria"
# leafIndex : 0
# name : "k__Bacteria"
# nchildren : 9
# nleaves : 953
# order : 0
# parentId : null
# partition : null
# selectionType : 1
# size : 1
# start : 0
# taxonomy : "taxonomy1"

    measurements = []

    dictionary = {}

    #if in_params_nodeId=="" :
    #qryStr = "MATCH p=(fParent:Feature {id:'0-0'})-[*]->(f:Feature {depth : '" + str(1) +"'}) RETURN nodes(p) AS nodes"
    qryStr = "MATCH p=(f:Feature {id:'0-0'})-[*]->(f2:Feature {depth: '3'}) RETURN f AS root, COLLECT(f2) AS leaf, nodes(p) AS path"
    #qryStr = "MATCH (fParent:Feature {id:'0-0'}) \
    #MATCH (f:Feature {depth : '" + in_params_depth +"'}) MATCH (fParent)-[*]->(f)  \
    #RETURN f.lineage, f.start, f.label, f.leafIndex, f.parentId, f.depth, f.partition, f.end, f.id, f.lineageLabel, f.nchildren, f.nleaves, f.order"
    #else:
    #    qryStr = "MATCH (fParent:Feature {id:' " + in_params_nodeId + " '}) \
    #    MATCH (f:Feature {depth : '" + in_params_depth +"'}) MATCH (fParent)-[*]->(f)  \
    #    RETURN f.lineage, f.start, f.label, f.leafIndex, f.parentId, f.depth, f.partition, f.end, f.id, f.lineageLabel, f.nchildren, f.nleaves, f.order"

    vals = ['lineage', 'start', 'label', 'leafIndex', 'parentId', 'depth', 'partition', 'end', 'id', 'lineageLabel', 'nchildren', 'nleaves', 'order']
#     lineage	0-0
# start	0
# label	k__Bacteria
# leafIndex	0
# parentId	NULL
# depth	0
# partition	NULL
# end	953
# id	0-0
# lineageLabel	k__Bacteria
# nchildren	9
# nleaves	953
# order
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

    print(roots)
    print(leaves)
    print(paths)

    result = {}
    result['id'] = '0-0'
    depth_1_children = []
    depth_1_children_index = []
    depth_2_children = []
    depth_3_children = []
    for j in range(0, len(paths)):
        if paths[j][1]['id'] not in depth_1_children_index:
            depth_1_children.append({'id':paths[j][1]['id']})
            depth_1_children_index.append(paths[j][1]['id'])

    print(depth_1_children_index)
    depth_2_index = {}
    for j in range(0, len(paths)):
        print(paths[j][2]['parentId'])
        if paths[j][2]['parentId'] in depth_1_children_index:
            print(depth_1_children_index.index(paths[j][2]['parentId']))
            depth_1_children[depth_1_children_index.index(paths[j][2]['parentId'])]['children'] = [{'id': paths[j][2]['id']}]
            if paths[j][2]['id'] in depth_2_index:
                depth_2_index[paths[j][2]['id']].append(str(j))
            else:
                depth_2_index[paths[j][2]['id']] = [str(j)]
            #depth_1_children[depth_1_children_index.index(paths[j][2]['parentId'])]['children'].append({'id': paths[j][2]['id']})
            #depth_2_children.append({'id' : paths[j][2]['id'], 'parentId': paths[j][2]['parentId']})


    for j in range(0, len(paths)):
        if paths[j][3]['id'] not in depth_3_children:
            depth_3_children.append({'id' : paths[j][3]['id'], 'parentId': paths[j][3]['parentId']})

    print(depth_1_children)
    print(depth_2_children)
    print(depth_3_children)

# depth:0
# end:953
# globalDepth:0
# id:"0-0"
# label:"k__Bacteria"
# leafIndex:0
# name:"k__Bacteria"
# nchildren:9
# nleaves:953
# order:0
# parentId:null
# partition:null
# selectionType:1
# size:1
# start:0
# taxonomy:"taxonomy1"
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
    result['taxonomy'] = 'taxonomy1'

    result['children'] = depth_1_children

    # , 'end': to_iter[j]['end'], 'partition': to_iter[j]['partition'], 'leafIndex': to_iter[j]['leafIndex'], 'nchildren': to_iter[j]['nchildren'],
    # 'label': to_iter[j]['label'], 'start' : to_iter[j]['start'], 'depth': to_iter[j]['depth'], 'lineageLabel' : to_iter[j]['lineageLabel'], 'nleaves' : to_iter[j]['nleaves'],
    # 'parentId' : to_iter[j]['parentId'], 'order' : to_iter[j]['order'], 'id' : to_iter[j]['id']

        # path = line['path']
        # for j in range(0, len(path)):
        #     if path[j]['parentId'] == tree['root']:

        #print(root)
        #print(leaf[0])
        #print(path)
        #print(to_iter)
        #for j in range(0, len(to_iter)):
        #    children_child = {'lineage': to_iter[j]['lineage'], 'end': to_iter[j]['end'], 'partition': to_iter[j]['partition'], 'leafIndex': to_iter[j]['leafIndex'], 'nchildren': to_iter[j]['nchildren'],
        #                      'label': to_iter[j]['label'], 'start' : to_iter[j]['start'], 'depth': to_iter[j]['depth'], 'lineageLabel' : to_iter[j]['lineageLabel'], 'nleaves' : to_iter[j]['nleaves'],
        #                      'parentId' : to_iter[j]['parentId'], 'order' : to_iter[j]['order'], 'id' : to_iter[j]['id']}
        #    children.append(children_child)
        #measurements.append(children)

    #print(measurements)
            # print(to_iter[j]['lineage'])
            # print(to_iter[j]['end'])
            # print(to_iter[j]['partition'])
            # print(to_iter[j]['leafIndex'])
            # print(to_iter[j]['nchildren'])
            # print(to_iter[j]['label'])
            # print(to_iter[j]['start'])
            # print(to_iter[j]['depth'])
            # print(to_iter[j]['lineageLabel'])
            # print(to_iter[j]['nleaves'])
            # print(to_iter[j]['parentId'])
            # print(to_iter[j]['order'])
            # print(to_iter[j]['id'])


    # list_result = list(result)
    # print(result)
    # print(list_result)
    # print(list_result[0]['nodes'])
    # to_iter = list_result[0]['nodes']
    # #print(list(list_result))
    # print("About to print results")
    # for j in range(0, len(to_iter)):
    #     measurements.append({"lineage": to_iter[j]['lineage'], "start": to_iter[j]['start'],
    #             "label": to_iter[j]['label'], "leafIndex": to_iter[j]['leafIndex'],
    #             "parentId": list_result[j]['f.parentId'], "f.depth": list_result[j]['depth'],
    #             "partition": list_result[j]['f.partition'], "end": list_result[j]['end'],
    #             "id":list_result[j]['f.id'], "lineageLabel": list_result[j]['lineageLabel'],
    #             "nchildren": list_result[j]['nchildren'], "nleaves": list_result[j]['nleaves'],
    #             "order": list_result[j]['order']})

        # measurements.append(list_result[j]['f.lineage'])
        # measurements.append(list_result[j]['f.start'])
        # measurements.append(list_result[j]['f.label'])
        # measurements.append(list_result[j]['f.leafIndex'])
        # measurements.append(list_result[j]['f.parentId'])
        # measurements.append(list_result[j]['f.depth'])
        # measurements.append(list_result[j]['f.partition'])
        # measurements.append(list_result[j]['f.end'])
        # measurements.append(list_result[j]['f.id'])
        # measurements.append(list_result[j]['f.lineageLabel'])
        # measurements.append(list_result[j]['f.nchildren'])
        # measurements.append(list_result[j]['f.nleaves'])
        # measurements.append(list_result[j]['f.order'])


    #print(measurements)


    # depth_1 = []
    # depth_2 = []
    # depth_3 = []
    # for k in range(0, len(measurements)):
    #     if measurements[k]['f.depth'] == '1':
    #         depth_1.append(measurements[k])
    #
    # print(depth_1)

    # df = pandas.DataFrame(list_result)
    # df[0] = df[0].astype(float)
    # df[3] = df[3].astype(int)
    # df[4] = df[4].astype(int)
    # df[5] = df[5].astype(int)
    #
    # rows_group = df.groupby([2, 3, 4, 5,6 ,7 ,8])
    #
    # gb_rows = rows_group.groups
    # to_sort = []
    # for key, values in gb_rows.iteritems():
    #     to_sort.append(key)
    #
    # to_sortDF = pandas.DataFrame(to_sort, columns = ['label', 'index', 'end', 'start', 'id', 'lineage', 'lineageLabel'])
    #
    # sorted = to_sortDF.sort('start')
    #
    # df_grouped = df.groupby([1])
    #
    # gb = df_grouped.groups
    # cols = {}
    # for key, values in gb.iteritems():
    #     col_val = numpy.zeros(len((sorted['end'].values).tolist()))
    #     print(key)
    #     print(values)
    #     print(df.iloc[values])
    #     for v in df.iloc[values].values:
    #         print(v)
    #         val_idx = pandas.Index(sorted['label']).get_loc(v[2])
    #         print(sorted['label'])
    #         print("val_idx " + str(val_idx))
    #         col_val[val_idx] = v[0]
    #     cols[key] = col_val.tolist()
    #
    # print(cols)
    #
    #
    # #pprint.pprint(sorted)
    # rows = {}
    #
    # rows['end'] =(sorted['end'].values).tolist()
    # rows['index'] = (sorted['index'].values).tolist()
    # rows['start'] = (sorted['start'].values).tolist()
    # rows['metadata'] = dict()
    # rows['metadata']['id'] = dict()
    # rows['metadata']['label'] = dict()
    # rows['metadata']['lineage'] = dict()
    # rows['metadata']['taxonomy'] = dict()
    # rows['metadata']['id'] = (sorted['id'].values).tolist()
    # rows['metadata']['label'] = (sorted['label'].values).tolist()
    # rows['metadata']['lineage'] = (sorted['lineage'].values).tolist()
    # rows['metadata']['taxonomy'] = (sorted['lineageLabel'].values).tolist()
    #
    #
    # z = numpy.zeros(shape = (len(tick_samples), len((sorted['end'].values).tolist())))
    #
    # print(rows)
    # gb = df_grouped.groups
    #
    # #for key, values in gb.iteritems():
    #    #print df.ix[values], "\n\n"
    #    #break
    #
    # #print(df)
    # #print(df_grouped)
    # #print(result)
    # #print(list_result)
    # print("About to print results")
    # #for j in range(0, len(list_result)):
    #     #print(list_result[j])
    #     #measurements.append(copy.deepcopy(dictionary))
    #     #for k in range(0, len(list_result[j])):
    #     #     measurements[j][keys[k]] = list_result[j][k]
    #
    # print("Printed results")
    #result["cols"] = {[]}
    # session.close()
    errorStr = ""
    pageSize = str(10)
    #resRowsCols = {"cols": cols, "rows": rows, "globalStartIndex": (min(rows['start']))}
    res = jsonify({"id": request.values['id'], "error": errorStr, "result": result})
    pprint.pprint(jsonify({"id": request.values['id'], "error": errorStr, "result": result}))
    return res


if __name__ == '__main__':
    app.run(debug=True, port=5002)

