import utils
import pandas
import sys
import ujson
import numpy
from BaseRequest import BaseRequest
import ast
import numpy

"""
.. module:: CombinedRequest
   :synopsis: Query Neo4j Sample nodes and compute aggregation function over selected Feature nodes

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""
class CombinedTimeRequest(BaseRequest):

    def __init__(self, request):
        super(CombinedTimeRequest, self).__init__(request)
        self.params_keys = [self.start_param, self.end_param, self.order_param, self.selection_param,
                       self.selectedLevels_param, self.measurements_param, self.datasource_param]
        self.params = self.validate_params(request)


    #https://github.com/jkanche/epiviz_data_provider/blob/master/epivizws/requests.py
    def validate_params(self, request):


        params = {}

        for key in self.params_keys:
            if request.has_key(key):
                params[key] = request.get(key)
            else:
                if key not in self.params_keys:
                    raise Exception("missing params in request")
        return params

    def get_data(self):

        """
        Aggregates counts to the selected nodes in the feature hierarchy and returns the counts for the samples
        selected.

        orgs:
            in_params_start: Start of range for features to use during aggregation
            in_params_end: End of range for features to use during aggregation
            in_params_order: Order of features
            in_params_selection: Features nodes and the selection type of expanded, aggregated, or removed
            in_params_selectedLevels: Level of the hierarchy to use
            in_params_samples: Samples to compute aggregation with

        Returns:
            resRowsCols: Aggregated counts for the selected Features over the selected Samples
        """

        tick_samples = self.params.get(self.measurements_param)
        #tick_samples = tick_samples.replace("\"", "\'")

        result = None

        error = None

        response_status = 200

        # get the min selected Level if aggregated at multiple levels
        try:
            selectedLevelsDict = ast.literal_eval(self.params.get(self.selectedLevels_param))
            minSelectedLevel = utils.find_min_level(self.params.get(self.datasource_param), selectedLevelsDict)

        except:
            error_info = sys.exc_info()
            error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
            response_status = 500
            return result, error, response_status

        try:
            # user selection nodes for custom aggregation - decides the cut
            selNodes = "["
            selFlag = 0
            selectionDict = ast.literal_eval(self.params.get(self.selection_param))
            if (hasattr(selectionDict, "keys")):
              for node in selectionDict.keys():
                if "_" in node:
                    # its an OTU group node
                    group_nodes = node.split("_")
                    for gNode in group_nodes:
                      if (hasattr(selectionDict, "keys")):
                        if gNode not in selectionDict.keys():
                            selectionDict[gNode] = selectionDict[node]
                            selNodes += "'" + gNode + "',"
                            selFlag = 1
                elif len(node.split("-")) > 2:
                    # its an OTU grouped node.
                    # apply selection to every node in this group
                    node_split = node.split("-")
                    sNode = node_split[1] + "-" + node_split[2]
                    childQryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
                                  "(fParent:Feature {id: '%s'})-[:PARENT_OF]->(f:Feature) RETURN f.id as id" \
                                  % (self.params.get(self.datasource_param),sNode)
                    child_rq_res = utils.cypher_call(childQryStr)
                    child_df = utils.process_result(child_rq_res)
                    children_ids = child_df['id'].values
                    for child_node in children_ids:
                      if (hasattr(selectionDict, "keys")):
                        if child_node not in selectionDict.keys():
                            selNodes += "'" + child_node + "',"
                            selectionDict[child_node] = 2
                            selFlag = 1

                elif selectionDict[node] == 2:
                    selNodes += "'" +  node + "',"
                    selFlag = 1
                elif selectionDict[node] == 1:
                    childQryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
                                  "(fParent:Feature {id: '%s'})-[:PARENT_OF]->(f:Feature) RETURN f.id as id" \
                                  % (self.params.get(self.datasource_param), node)
                    child_rq_res = utils.cypher_call(childQryStr)
                    child_df = utils.process_result(child_rq_res)
                    children_ids = child_df['id'].values
                    for child_node in children_ids:
                      if (hasattr(selectionDict, "keys")):
                        if child_node not in selectionDict.keys():
                            selNodes += "'" + child_node + "',"
                            selectionDict[child_node] = 2
                            selFlag = 1

            if selFlag == 1:
                selNodes = selNodes[:-1]
            selNodes += "]"

        except:
            error_info = sys.exc_info()
            error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
            response_status = 500
            return result, error, response_status

        markerGeneOrWgsQyrStr = "MATCH (ds:Datasource {label: '%s'}) RETURN ds.sequencingType as sequencing_type" \
                                % (self.params.get(self.datasource_param))
        try:
            rq_res = utils.cypher_call(markerGeneOrWgsQyrStr)
            df = utils.process_result(rq_res)
            markerGeneOrWgs = df['sequencing_type'].values[0]

        except:
            error_info = sys.exc_info()
            error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
            response_status = 500
            return result, error, response_status

        try:
            if markerGeneOrWgs == "wgs":
              qryStr = "MATCH (ds:Datasource {label: '%s'}) MATCH (ds)-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
                       "(f:Feature)<-[v:COUNT]-(s:Sample) USING INDEX s:Sample(id) WHERE (f.depth=%s OR f.id IN %s) " \
                       "AND (f.start >= %s AND f.end <= %s) AND s.id IN %s with distinct f, s, v.val as agg RETURN " \
                       "distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as end, f.start as start, " \
                       "f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order" \
                       % (str(self.params.get(self.datasource_param)), str(minSelectedLevel), selNodes,
                       str(self.params.get(self.start_param)), str(self.params.get(self.end_param)), tick_samples)

            else:
              # if (self.params.get(self.start_param) != "" and self.params.get(self.end_param) != ""):
              #   qryStr = "MATCH (ds:Datasource {label: '%s'}) MATCH (ds)-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
              #          "(f:Feature) MATCH (su:Subject)-[:TIMEPOINT]->(sa:Sample) MATCH (sa)-[v:COUNT]->()<-[:LEAF_OF]-(f) WHERE " \
              #          "(f.depth=%s OR f.id IN %s) AND (f.start >=%s AND f.end <= %s) AND su.SubjectID IN %s with distinct " \
              #          "f, sa, SUM(v.val) as agg RETURN distinct agg, sa.id as sid, sa.SubjectID as SubjectID, f.label as label, f.leafIndex as index, " \
              #          "f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as " \
              #          "lineageLabel, f.order as order, sa.Day as timePoint" % (self.params.get(self.datasource_param),
              #                                                                   str(minSelectedLevel), selNodes,
              #                                                                   self.params.get(self.start_param),
              #                                                                   self.params.get(self.end_param),
              #                                                                   tick_samples)
              # else:
              #     qryStr = "MATCH (ds:Datasource {label: '%s'}) MATCH (ds)-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
              #              "(f:Feature) MATCH (su:Subject)-[:TIMEPOINT]->(sa:Sample) MATCH (sa)-[v:COUNT]->()<-[:LEAF_OF]-(f) WHERE " \
              #              "(f.depth=%s OR f.id IN %s) AND su.SubjectID IN %s with distinct " \
              #              "f, sa, SUM(v.val) as agg RETURN distinct agg, sa.id as sid, sa.SubjectID as SubjectID, f.label as label, f.leafIndex as index, " \
              #              "f.id as id, f.lineage as lineage, f.lineageLabel as " \
              #              "lineageLabel, f.order as order, sa.Day as timePoint" % (
              #              self.params.get(self.datasource_param),
              #              str(minSelectedLevel), selNodes,
              #              tick_samples)

                if (self.params.get(self.start_param) != "" and self.params.get(self.end_param) != ""):
                  # qryStr = "MATCH (ds:Datasource {label: '%s'}) MATCH (ds)-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
                  #        "(f:Feature) MATCH (su:Subject)-[:TIMEPOINT]->(sa:Sample) MATCH (sa)-[v:COUNT]->()<-[:LEAF_OF]-(f) WHERE " \
                  #        "(f.depth=%s OR f.id IN %s) AND (f.start >=%s AND f.end <= %s) AND su.SubjectID IN %s with distinct " \
                  #        "f, sa, SUM(v.val) as agg RETURN distinct agg, sa.id as sid, sa.SubjectID as SubjectID, f.label as label, f.leafIndex as index, " \
                  #        "f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as " \
                  #        "lineageLabel, f.order as order, sa.Day as timePoint" % (self.params.get(self.datasource_param),
                  #                                                                 str(minSelectedLevel), selNodes,
                  #                                                                 self.params.get(self.start_param),
                  #                                                                 self.params.get(self.end_param),
                  #                                                                 tick_samples)

					qryStr = "MATCH(ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-" \
                      "[:LEAF_OF]->()<-[v:COUNT]-(sa:Sample) WHERE(f.depth = %s OR f.id IN %s) AND " \
                      "(f.start >= %s AND f.end <= %s) AND sa.SubjectID IN %s with distinct f, sa, SUM(v.val) as " \
                      "agg RETURN distinct agg, sa.id as sid, sa.SubjectID as SubjectID, f.label as label, f.leafIndex " \
                      "as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as " \
                      "lineageLabel, f.order as order, sa.Day as timePoint" % (self.params.get(self.datasource_param),
                                                                                  str(minSelectedLevel), selNodes,
                                                                                  self.params.get(self.start_param),
                                                                                  self.params.get(self.end_param),
                                                                                  tick_samples)
                else:
                    # qryStr = "MATCH (ds:Datasource {label: '%s'}) MATCH (ds)-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
                    #          "(f:Feature) MATCH (su:Subject)-[:TIMEPOINT]->(sa:Sample) MATCH (sa)-[v:COUNT]->()<-[:LEAF_OF]-(f) WHERE " \
                    #          "(f.depth=%s OR f.id IN %s) AND su.SubjectID IN %s with distinct " \
                    #          "f, sa, SUM(v.val) as agg RETURN distinct agg, sa.id as sid, sa.SubjectID as SubjectID, f.label as label, f.leafIndex as index, " \
                    #          "f.id as id, f.lineage as lineage, f.lineageLabel as " \
                    #          "lineageLabel, f.order as order, sa.Day as timePoint" % (
                    #          self.params.get(self.datasource_param),
                    #          str(minSelectedLevel), selNodes,
                    #          tick_samples)

              		qryStr = "MATCH(ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-" \
                      "[:LEAF_OF]->()<-[v:COUNT]-(sa:Sample) WHERE(f.depth = %s OR f.id IN %s) AND " \
                      "sa.SubjectID IN %s with distinct f, sa, SUM(v.val) as " \
                      "agg RETURN distinct agg, sa.id as sid, sa.SubjectID as SubjectID, f.label as label, f.leafIndex " \
                      "as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as " \
                      "lineageLabel, f.order as order, sa.Day as timePoint" % (self.params.get(self.datasource_param),
                                                                                  str(minSelectedLevel), selNodes,
                                                                                  self.params.get(self.start_param),
                                                                                  self.params.get(self.end_param),
                                                                                  tick_samples)

            rq_res = utils.cypher_call(qryStr)
            df = utils.process_result(rq_res)

            df["agg"] = numpy.log2(df["agg"] + 1)

            grouped_df = df.groupby(["SubjectID", "lineage"])

            timePointQryStr = "MATCH (su:Subject)-[:TIMEPOINT]->(sa:Sample) WHERE su.SubjectID IN %s with distinct " \
                     "sa RETURN distinct sa.id as sid, sa.SubjectID as SubjectID, sa.Day as timePoint ORDER BY timePoint" % (tick_samples)
            timePointQryStr_rq_res = utils.cypher_call(timePointQryStr)
            timePoint_df = utils.process_result(timePointQryStr_rq_res)

            timepoints = numpy.unique(timePoint_df["timePoint"].values)
            sample_ids = timePoint_df["sid"].values

            df_time_points_temp = []
            empty_array = []
            group_size = 0

            for name, group in grouped_df:
                group_size = len(group["timePoint"].values)
                if group_size == 0:
                    continue
                sample_name = name[0]
                lineage_temp = name[1]

                group = group.sort_values("timePoint")
                temp_dict = {}
                temp_dict["agg"] = group["agg"].values.tolist()
                temp_dict["s.id"] = group["sid"].values[0]
                temp_dict["SubjectID"] = group["SubjectID"].values[0]
                temp_dict["label"] = group["label"].values[0]
                temp_dict["index"] = group["index"].values[0]
                if (hasattr(group, "end")):
                    temp_dict["end"] = group["end"].values[0]
                else:
                    temp_dict["end"] = 100000
                if (hasattr(group, "start")):
                    temp_dict["start"] = group["start"].values[0]
                else:
                    temp_dict["start"] = 1
                temp_dict["id"] = group["id"].values[0]
                temp_dict["lineage"] = group["lineage"].values[0]
                temp_dict["lineageLabel"] = group["lineageLabel"].values[0]
                temp_dict["order"] = group["order"].values[0]
                temp_dict["timePoint"] = group["timePoint"].values.tolist()

                group_timepoints = group["timePoint"].values
                timepoint_idx = 0
                for t in timepoints:
                    if t not in group_timepoints:
                        if temp_dict["agg"] is 0.0:
                            temp_dict["agg"] = [None]
                        else:
                            temp_dict["agg"].insert(timepoint_idx,None)
                        if temp_dict["timePoint"] is 0.0:
                            temp_dict["timePoint"] = [t]
                        else:
                            temp_dict["timePoint"].insert(timepoint_idx, t)

                    timepoint_idx = timepoint_idx + 1

                temp_dict["agg"] = ujson.dumps(temp_dict["agg"])
                temp_dict["timePoint"] = ujson.dumps(temp_dict["timePoint"])

                df_time_points_temp.append(group["agg"].values.tolist())
                empty_array.append(temp_dict)

            unique_timepoints = df['timePoint'].unique()
            num_unique_timepoints = len(unique_timepoints)
            unique_labels = df['label'].unique()
            averages = {}
            std_devs = {}
            for u in unique_labels:
                averages[u] = {}
                std_devs[u] = {}

            print(df.head())
            # timepoint_grouped_df = df.groupby(["lineage", "SubjectID"])

            # print(timepoint_grouped_df)

            # for key, timepoint_group in timepoint_grouped_df:
            #     print(key)
            #     subjectId = key[1]
            #     print(timepoint_group)
            #     temp_timepoint_group = timepoint_group.sort_values(by='timePoint')

            #     label = timepoint_group['label'].values
            #     print(label)
            #     label = label[0]
            #     timepoint = timepoint_group['timePoint'].values
            #     print(timepoint)
            #     timepoint = timepoint[0]
            #     temp_average = timepoint_group['agg'].mean()
            #     temp_std_dev = timepoint_group['agg'].std()
            #     if numpy.isnan(temp_average):
            #         temp_average = 0.0
            #     if numpy.isnan(temp_std_dev):
            #         temp_std_dev = 0.0
                
            #     averages[label][subjectId] = temp_average
            #     std_devs[label][subjectId] = temp_std_dev

            timepoint_grouped_df = df.groupby(["lineage"])
            print(timepoint_grouped_df)

            for key, timepoint_group in timepoint_grouped_df:
                # print(key)
                # subjectId = key[1]
                # print(timepoint_group)
                temp_timepoint_group = timepoint_group.sort_values(by='timePoint')

                label = timepoint_group['label'].values
                # print(label)
                label = label[0]
                # timepoint = timepoint_group['timePoint'].values
                # print(timepoint)
                # timepoint = timepoint[0]
                temp_average = timepoint_group['agg'].mean()
                temp_std_dev = timepoint_group['agg'].std()
                if numpy.isnan(temp_average):
                    temp_average = 0.0
                if numpy.isnan(temp_std_dev):
                    temp_std_dev = 0.0
                
                averages[label] = temp_average
                std_devs[label] = temp_std_dev

            # print(averages)
            # print(std_devs)
            # sorted_list_averages = {}

            # for k in averages.keys():
            #     if k not in sorted_list_averages.keys():
            #         sorted_list_averages[k] = []
            #     sorted_keys = sorted(averages[k].keys())
            #     for m in sorted_keys:
            #         sorted_list_averages[k].append(averages[k][m])

            # sorted_list_std_devs = {}

            # for k in std_devs.keys():
            #     if k not in sorted_list_std_devs.keys():
            #         sorted_list_std_devs[k] = []
            #     sorted_keys = sorted(std_devs[k].keys())
            #     for m in sorted_keys:
            #         sorted_list_std_devs[k].append(std_devs[k][m])

            df = pandas.DataFrame(empty_array)

            root_is_present = True
            if (hasattr(selectionDict, "keys")):
              for key in selectionDict.keys():
                lKey = key.split('-')
                if int(lKey[0]) <= minSelectedLevel:
                    if selectionDict[key] == 0 and df.shape == df[df['lineage'].str.contains(key)].shape:
                        root_is_present = False

            if len(df) > 0 and root_is_present:
                # change column type
                df['index'] = df['index'].astype(int)
                df['start'] = df['start'].astype(int)
                df['end'] = df['end'].astype(int)
                df['order'] = df['order'].astype(int)

                # update order based on req
                if (hasattr(self.params.get(self.order_param), "keys")):
                  for key in self.params.get(self.order_param).keys():
                    df.loc[df['id'] == key, 'order'] = self.params.get(self.order_param)[key]

                if (hasattr(selectionDict, "keys")):
                  for key in selectionDict.keys():
                    if selectionDict[key] == 0:
                       # user selected nodes to ignore!
                       df = df[~(df['lineage'].str.contains(key))]
                    elif selectionDict[key] == 2:
                       df = df[~(df['lineage'].str.contains(key) & ~df['id'].str.contains(key))]

                if (hasattr(selectionDict, "keys")):
                  for key in selectionDict.keys():
                    if selectionDict[key] == 2:
                       if df[df['id'].str.contains(key)].shape[0] > 0:
                        node_lineage = df[df['id'].str.contains(key)]['lineage'].head(1).values[0].split(",")
                        for i in range(minSelectedLevel, len(node_lineage)-1):
                           df = df[~df['id'].str.contains(str(node_lineage[i]))]

                # create a pivot_table where columns are samples and rows are features
                # for pandas > 0.17
                df_pivot = pandas.pivot_table(df, index=["id", "label", "index", "lineage", "lineageLabel", "start",
                                                         "end", "order"],
                                              columns="SubjectID", values="agg", fill_value=0, aggfunc='first').sortlevel("index")

                cols = {}

                for col in df_pivot:
                    cols[col] = df_pivot[col].values.tolist()

                    for idx in range(len(cols[col])):
                        if cols[col][idx] == None:
                            # tempStr = "["
                            # for i in len(timepoints):
                            #     tempStr = tempStr + "0.0, "
                            # tempStr = tempStr.slice(0, len(tempStr) -1)
                            # tempStr = tempStr + "]"
                            # cols[col][idx] = tempStr
                            # cols[col][idx] = ujson.dumps(repeat(0.0, len(timepoints)))
                            cols[col][idx] = ujson.dumps(numpy.repeat(None, len(timepoints)))

                samplesQryStr = "MATCH (su:Subject)-[:TIMEPOINT]->(sa:Sample) WHERE su.SubjectID IN %s " \
                                "RETURN distinct su.SubjectID as SubjectID, sa, sa.Day as timePoint ORDER BY timePoint" % (tick_samples)
                samplesQryStr_rq_res = utils.cypher_call(samplesQryStr)
                samples_df = utils.process_result(samplesQryStr_rq_res)

                subject_metadata = {}

                for k in samples_df.itertuples():
                    if k[1] not in subject_metadata:
                        subject_metadata[k[1]] = {}
                        subject_metadata[k[1]]['timepoints'] = []
                        for j in k[2].keys():
                            subject_metadata[k[1]][j] = []

                for k in samples_df.itertuples():
                    subject_metadata[k[1]]['timepoints'].append(k[3])
                    for j in k[2].keys():
                        subject_metadata[k[1]][j].append(k[2][j])

                rows = {}
                rows['metadata'] = {}

                metadata_row = ["end", "start", "index"]

                for row in df_pivot.index.names:
                    if row in metadata_row:
                        rows[row] = df_pivot.index.get_level_values(row).values.tolist()
                    else:
                        rows['metadata'][row] = df_pivot.index.get_level_values(row).values.tolist()

                rows['metadata']['means'] = ujson.dumps(averages)
                rows['metadata']['std_devs'] = ujson.dumps(std_devs)
                rows['metadata']['subject_metadata'] = ujson.dumps(subject_metadata)

                result = {"cols": cols, "rows": rows, "globalStartIndex": (min(rows['start']))}
            else:
                cols = {}

                samples = eval(self.params.get(self.measurements_param))

                for sa in samples:
                    cols[sa] = []

                rows = { "end": [], "start": [], "index": [], "metadata": {} }
                result = {"cols": cols, "rows": rows, "globalStartIndex": None}
        except:
            error_info = sys.exc_info()
            error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
            response_status = 500

        return result, error, response_status