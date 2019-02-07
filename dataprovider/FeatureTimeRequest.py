import utils
import pandas
import sys
import ujson
import numpy
from BaseRequest import BaseRequest
import ast
"""
.. module:: CombinedRequest
   :synopsis: Query Neo4j Sample nodes and compute aggregation function over selected Feature nodes

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""
class FeatureTimeRequest(BaseRequest):

    def __init__(self, request):
        super(FeatureTimeRequest, self).__init__(request)
        self.params_keys = [self.feature_param, self.measurements_param, self.datasource_param]
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

        Args:
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
        qryStr = "MATCH (ds:Datasource {label: '%s'}) MATCH (ds)-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
                        "(f:Feature) MATCH (su:Subject)-[:TIMEPOINT]->(sa:Sample) MATCH (sa)-[v:COUNT]->()<-[:LEAF_OF]-(f) WHERE " \
                        "(f.id ='%s') AND su.SubjectID IN %s with distinct " \
                        "f, sa, SUM(v.val) as agg RETURN distinct agg, sa.id as sid, sa.SubjectID as SubjectID, f.label as label, f.leafIndex as index, " \
                        "f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as " \
                        "lineageLabel, f.order as order, sa.Day as timePoint" % (
                            self.params.get(self.datasource_param),
                            self.params.get(self.feature_param),
                            tick_samples)

        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

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

            group = group.sort("timePoint")
            temp_dict = {}
            temp_dict["agg"] = group["agg"].values.tolist()
            temp_dict["sid"] = group["sid"].values[0]
            temp_dict["SubjectID"] = group["SubjectID"].values[0]
            temp_dict["label"] = group["label"].values[0]
            temp_dict["index"] = group["index"].values[0]
            temp_dict["lineage"] = group["lineage"].values[0]
            temp_dict["lineageLabel"] = group["lineageLabel"].values[0]
            temp_dict["id"] = group["id"].values[0]
            if (hasattr(group, "end")):
                temp_dict["end"] = group["end"].values[0]
            else:
                temp_dict["end"] = 100000
            if (hasattr(group, "start")):
                temp_dict["start"] = group["start"].values[0]
            else:
                temp_dict["start"] = 1
            temp_dict["order"] = group["order"].values[0]

            temp_dict["timePoint"] = group["timePoint"].values.tolist()

            group_timepoints = group["timePoint"].values
            timepoint_idx = 0
            for t in timepoints:
                if t not in group_timepoints:
                    if temp_dict["agg"] is None:
                        temp_dict["agg"] = [0.0]
                    else:
                        temp_dict["agg"].insert(timepoint_idx,0.0)
                    if temp_dict["timePoint"] is None:
                        temp_dict["timePoint"] = [t]
                    else:
                        temp_dict["timePoint"].insert(timepoint_idx, t)

                timepoint_idx = timepoint_idx + 1

            temp_dict["agg"] = ujson.dumps(temp_dict["agg"])
            temp_dict["timePoint"] = ujson.dumps(temp_dict["timePoint"])

            df_time_points_temp.append(group["agg"].values.tolist())
            empty_array.append(temp_dict)

        # df_pivot = pandas.pivot_table(df, index=["id", "label"],
        #                               columns="SubjectID", values="agg", fill_value=0, aggfunc='first')
        df = pandas.DataFrame(empty_array)
        if len(df) > 0:

            df_pivot = pandas.pivot_table(df, index=["id", "label", "index", "lineage", "lineageLabel", "start",
                                                     "end", "order"],
                                          columns="SubjectID", values="agg", fill_value=0, aggfunc='first').sortlevel("index")

            cols = {}


            for col in df_pivot:
                cols[col] = df_pivot[col].values.tolist()
                for idx in range(len(cols[col])):
                    if cols[col][idx] == 0.0:
                        cols[col][idx] = ujson.dumps(numpy.zeros(len(timepoints)))


            rows = {}
            rows['metadata'] = {}

            metadata_row = ["end", "start", "index"]

            for row in df_pivot.index.names:
                if row in metadata_row:
                    rows[row] = df_pivot.index.get_level_values(row).values.tolist()
                else:
                    rows['metadata'][row] = df_pivot.index.get_level_values(row).values.tolist()

            sampleQryStr = "MATCH (su:Subject)-[:TIMEPOINT]->(sa:Sample {Day: '9'}) WHERE su.SubjectID IN %s with distinct " \
                 "sa RETURN distinct sa as s" % (tick_samples)

            sample_rq_res = utils.cypher_call(sampleQryStr)
            sample_df = utils.process_result_graph(sample_rq_res)

            vals = []
            for index, rowsamp in sample_df.iterrows():
                temp = {}
                for key in rowsamp.keys().values:
                    temp[key] = rowsamp[key]
                if rowsamp['SubjectID'] in cols:
                    temp['count'] = cols[rowsamp['SubjectID']][0]
                else:
                    temp['count'] = 0

                temp['sample_id'] = temp['id']
                del temp['id']
                vals.append(temp)
            result = {"data": vals}
        else:
            cols = {}

            samples = eval(self.params.get(self.measurements_param))

            for sa in samples:
                cols[sa] = []

            rows = { "end": [], "start": [], "index": [], "metadata": {} }
            result = {"cols": cols, "rows": rows, "globalStartIndex": None}

        return result, error, response_status
