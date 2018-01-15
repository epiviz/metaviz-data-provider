import utils
import sys
import pandas
from BaseRequest import BaseRequest
import ast

"""
.. module:: HierarchyRequest
   :synopsis: Query Neo4j Feature nodes and return hierarchy of levels

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""
class HierarchyRequest(BaseRequest):
    def __init__(self, request):
        super(HierarchyRequest, self).__init__(request)
        self.params_keys = [self.selection_param, self.order_param, self.selectedLevels_param, self.root_node_param,
                       self.depth_param, self.datasource_param]
        self.params = self.validate_params(request)

    def validate_params(self, request):


        params = {}
        for key in self.params_keys:
            if request.has_key(key):
                params[key] = request.get(key)
            else:
                if key not in self.params_keys:
                   raise Exception("missing params in request")

        return params

    def row_to_dict(self, row):
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


    def df_to_tree(self,root, df):
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
        children.sort_values('order')

        for index, row in children.iterrows():
            childDict = self.row_to_dict(row)
            subDict = self.df_to_tree(childDict, otherChildren)
            if subDict is None:
                root['children'] = None
            else:
                root['children'].append(subDict)

        return root

    def get_data(self):
        # in_params_selection, in_params_order, in_params_selected_levels, in_params_nodeId, in_params_depth, in_datasource):
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
        root_node = self.params.get(self.root_node_param)

        if root_node != None:
            root_node = root_node.replace('"', "")

        if root_node == None or len(root_node) == 0 or root_node == "0-0" or root_node == "0-1":
            root_node = "0-0"

        taxonomy = False
        result = None
        error = None
        response_status = 200

        otu_expand_request = False
        if "_" in root_node:
            otu_expand_request = True

        if otu_expand_request:
            split = root_node.split("_")
            all_nodes = "','".join(split)
            all_nodes = "['" + all_nodes + "']"

            qryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-" \
                     "[:PARENT_OF*0..3]->(f2:Feature) WHERE f.id IN %s OPTIONAL MATCH (f)<-[:PARENT_OF]-" \
                     "(fParent:Feature) with collect(f2) + f + fParent as nodesFeat unwind nodesFeat as ff " \
                     "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
                     "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, " \
                     "ff.partition as partition, ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, " \
                     "ff.nchildren as nchildren, ff.taxonomy as taxonomy, ff.nleaves as nleaves, " \
                     "ff.order as order ORDER by ff.depth, ff.leafIndex, ff.order" \
                     % (self.params.get(self.datasource_param), all_nodes)

            tQryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(f:Feature) RETURN DISTINCT " \
                      "f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth UNION MATCH (ds:Datasource " \
                      "{label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature) RETURN DISTINCT " \
                      "f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth" \
                      % (self.params.get(self.datasource_param), self.params.get(self.datasource_param))
            taxonomy = True

            nodesPerLevelQryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]" \
                                  "->(f:Feature)-[:PARENT_OF*0..3]->(f2:Feature) return f2.depth as level, " \
                                  "count(*) as nodesPerLevel ORDER BY level" % (self.params.get(self.datasource_param))
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

                    trq_res = utils.cypher_call(tQryStr)
                    tdf = utils.process_result(trq_res)

                    last_depth = int(tdf['depth'].values.tolist()[-1])
                    last_taxonomy = tdf['taxonomy'].values.tolist()[-1]
                    last_taxa_minus = tdf['taxonomy'].values.tolist()[-2]

                    # restore current order, selection and levels from input params
                    for key in self.params.get(self.order_param).keys():
                        df.loc[df['id'] == key, 'order'] = self.params.get(self.order_param)[key]

                    selectionDict = ast.literal_eval(self.params.get(self.selection_param))
                    for key in selectionDict.keys():
                        df.loc[df['id'] == key, 'selectionType'] = selectionDict[key]

                    for key in self.params.get(self.selectedLevels_param).keys():
                        df.loc[df['depth'] == int(key), 'selectionType'] = self.params.get(self.selectedLevels_param)[
                            key]

                    root = df.iloc[0]
                    other = df.loc[1:, ]

                    rootDict = self.row_to_dict(root)
                    rootDict['lineageLabel'] = root['lineageLabel']
                    result = self.df_to_tree(rootDict, other)

                    if taxonomy:
                        result['rootTaxonomies'] = tdf['taxonomy'].values.tolist()

                    npl_res = utils.cypher_call(nodesPerLevelQryStr)
                    npl_df = utils.process_result(npl_res)

                    result['nodesPerLevel'] = npl_df['nodesPerLevel'].values.tolist()

            except:
                error_info = sys.exc_info()
                error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
                response_status = 500

            return result, error, response_status

        root_split = root_node.split("-")

        otu_request = False
        if root_split[0] == "7":
            otu_request = True

        if not otu_request:
            if len(root_node) == 0 or root_node == "0-0" or root_node == "0-1":
                qryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(f:Feature {id:'%s'})-" \
                         "[:PARENT_OF*0..3]->(f2:Feature) with collect(f2) + f as nodesFeat unwind nodesFeat " \
                         "as ff return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
                         "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as " \
                         "partition, ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as " \
                         "nchildren, ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order ORDER by " \
                         "ff.depth, ff.leafIndex, ff.order" % (self.params.get(self.datasource_param), root_node)

                tQryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(f:Feature) RETURN DISTINCT " \
                          "f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth UNION MATCH (ds:Datasource " \
                          "{label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature) RETURN DISTINCT " \
                          "f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth" \
                          % (self.params.get(self.datasource_param), self.params.get(self.datasource_param))
                taxonomy = True

                nodesPerLevelQryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(f:Feature {id:'%s'})-" \
                                  "[:PARENT_OF*0..3]->(f2:Feature) return f2.depth as level, count(*) as " \
                                  "nodesPerLevel ORDER BY level" % (self.params.get(self.datasource_param), root_node)

            else:
                qryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
                         "(f:Feature {id:'%s'})-[:PARENT_OF*0..3]->(f2:Feature) OPTIONAL MATCH (f)<-[:PARENT_OF]-" \
                         "(fParent:Feature) with collect(f2) + f + fParent as nodesFeat unwind nodesFeat as ff " \
                         "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
                         "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as " \
                         "partition, ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as " \
                         "nchildren, ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order ORDER by " \
                         "ff.depth, ff.leafIndex, ff.order" % (self.params.get(self.datasource_param), root_node)

                tQryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(f:Feature) RETURN DISTINCT " \
                          "f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth UNION MATCH (ds:Datasource " \
                          "{label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature) RETURN DISTINCT " \
                          "f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth" \
                          % (self.params.get(self.datasource_param), self.params.get(self.datasource_param))
                taxonomy = True

                nodesPerLevelQryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-" \
                                      "[:PARENT_OF*]->(f:Feature {id:'%s'})-[:PARENT_OF*0..3]->(f2:Feature) " \
                                      "return f2.depth as level, count(*) as nodesPerLevel ORDER BY level" \
                                      % (self.params.get(self.datasource_param), root_node)
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

                    trq_res = utils.cypher_call(tQryStr)

                    tdf = utils.process_result(trq_res)


                    last_depth = int(tdf['depth'].values.tolist()[-1])
                    last_taxonomy = tdf['taxonomy'].values.tolist()[-1]
                    last_taxa_minus = tdf['taxonomy'].values.tolist()[-2]

                    # restore current order, selection and levels from input params
                    if (hasattr(self.params.get(self.order_param), "keys")):
                      for key in self.params.get(self.order_param).keys():
                        df.loc[df['id'] == key, 'order'] = self.params.get(self.order_param)[key]


                    selectionDict = ast.literal_eval(self.params.get(self.selection_param))
                    if (hasattr(selectionDict, "keys")):
                      for key in selectionDict.keys():
                        df.loc[df['id'] == key, 'selectionType'] = selectionDict[key]

                    if (hasattr(self.params.get(self.selectedLevels_param), "keys")):
                      for key in self.params.get(self.selectedLevels_param).keys():
                        df.loc[df['depth'] == int(key), 'selectionType'] = self.params.get(self.selectedLevels_param)[
                            key]

                    df_otu = df[(df['depth'] == last_depth)]
                    if len(df_otu) > 0:
                        otu_nodes = df[(df['depth'] == last_depth - 1)]
                        otu_nodes["depth"].replace(last_depth - 1, last_depth, inplace=True)
                        otu_nodes["taxonomy"].replace(last_taxa_minus, last_taxonomy, inplace=True)
                        otu_nodes["nleaves"] = 1
                        for index, row in otu_nodes.iterrows():
                            otu_nodes.at[index, 'label'] = row['label'] + ", " + str(row["nchildren"]) + " OTUs"
                            otu_nodes.at[index, 'parentId'] = row['id']
                            otu_nodes.at[index, 'id'] = str(last_depth) + "-" + row['id']
                        otu_nodes["nchildren"] = 0
                        df = df[~(df['depth'] == last_depth)]
                        df = pandas.concat([df, otu_nodes])

                    root = df.iloc[0]
                    other = df.loc[1:, ]

                    rootDict = self.row_to_dict(root)
                    rootDict['lineageLabel'] = root['lineageLabel']
                    result = self.df_to_tree(rootDict, other)

                    if taxonomy:
                        result['rootTaxonomies'] = tdf['taxonomy'].values.tolist()

                    npl_res = utils.cypher_call(nodesPerLevelQryStr)
                    npl_df = utils.process_result(npl_res)

                    result['nodesPerLevel'] = npl_df['nodesPerLevel'].values.tolist()

            except:
                error_info = sys.exc_info()
                error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
                response_status = 500

            return result, error, response_status

        else:
            otu_parent_id = root_split[1] + "-" + root_split[2]

            qryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
                     "(f:Feature {id:'%s'})-[:PARENT_OF*0..3]->(f2:Feature) OPTIONAL MATCH (f)<-[:PARENT_OF]-" \
                     "(fParent:Feature) with collect(f2) + f + fParent as nodesFeat unwind nodesFeat as ff return " \
                     "distinct ff.lineage as lineage, ff.start as start, ff.label as label, ff.leafIndex as " \
                     "leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as partition, ff.end as " \
                     "end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as nchildren, ff.taxonomy as " \
                     "taxonomy, ff.nleaves as nleaves, ff.order as order ORDER by ff.depth, ff.leafIndex, ff.order" \
                     % (self.params.get(self.datasource_param), otu_parent_id)

            tQryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(f:Feature) RETURN DISTINCT " \
                      "f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth UNION MATCH (ds:Datasource " \
                      "{label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature) RETURN DISTINCT " \
                      "f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth" \
                      % (self.params.get(self.datasource_param), self.params.get(self.datasource_param))
            taxonomy = True

            nodesPerLevelQryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]" \
                                  "->(f:Feature {id:'%s'})-[:PARENT_OF*0..3]->(f2:Feature) return f2.depth as level, " \
                                  "count(*) as nodesPerLevel ORDER BY level" \
                                  % (self.params.get(self.datasource_param),otu_parent_id)
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

                    trq_res = utils.cypher_call(tQryStr)
                    tdf = utils.process_result(trq_res)

                    last_depth = int(tdf['depth'].values.tolist()[-1])
                    last_taxonomy = tdf['taxonomy'].values.tolist()[-1]
                    last_taxa_minus = tdf['taxonomy'].values.tolist()[-2]

                    # restore current order, selection and levels from input params
                    for key in self.params.get(self.order_param).keys():
                        df.loc[df['id'] == key, 'order'] = self.params.get(self.order_param)[key]

                    selectionDict = ast.literal_eval(self.params.get(self.selection_param))
                    for key in selectionDict.keys():
                        df.loc[df['id'] == key, 'selectionType'] = selectionDict[key]

                    for key in self.params.get(self.selectedLevels_param).keys():
                        df.loc[df['depth'] == int(key), 'selectionType'] = self.params.get(self.selectedLevels_param)[
                            key]

                    df_otu = df[(df['depth'] == last_depth)]
                    if len(df_otu) > 100:
                        df_otu.loc[:, 'index_group'] = range(1, len(df_otu) + 1)
                        df_otu.loc[:, 'groupField'] = df_otu['index_group'] / 100
                        df_otu['groupField'] = df_otu['groupField'].astype(int)

                        def f(x):
                            return pandas.Series(dict(lineage=x['lineage'].iloc[0],
                                                      start=x['start'].min(),
                                                      label="%s" % ', '.join(x['label']),
                                                      leafIndex=x['leafIndex'].min(),
                                                      parentId=x['parentId'].iloc[0],
                                                      depth=x['depth'].iloc[0],
                                                      partition=x['partition'].iloc[0],
                                                      end=x['end'].max(),
                                                      id="%s" % '_'.join(x['id']),
                                                      lineageLabel=x['lineageLabel'].iloc[0],
                                                      nchildren=len(x['nchildren']),
                                                      taxonomy=x['taxonomy'].iloc[0],
                                                      nleaves=x['nleaves'].sum(),
                                                      order=x['order'].sum(),
                                                      selectionType=x['selectionType'].iloc[0]
                                                      )
                                                 )

                        df_otu = df_otu.groupby("groupField").apply(f)
                        df_otu = df_otu.reset_index()
                        del df_otu["groupField"]
                        df = df[~(df['depth'] == last_depth)]
                        df = pandas.concat([df, df_otu], ignore_index=True)

                    root = df.iloc[0]
                    other = df.loc[1:, ]
                    rootDict = self.row_to_dict(root)
                    rootDict['lineageLabel'] = root['lineageLabel']
                    result = self.df_to_tree(rootDict, other)

                    if taxonomy:
                        result['rootTaxonomies'] = tdf['taxonomy'].values.tolist()

                    npl_res = utils.cypher_call(nodesPerLevelQryStr)
                    npl_df = utils.process_result(npl_res)

                    result['nodesPerLevel'] = npl_df['nodesPerLevel'].values.tolist()

            except:
                error_info = sys.exc_info()
                error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
                response_status = 500

            return result, error, response_status

