import utils
import pandas
import sys
from BaseRequest import BaseRequest

"""
.. module:: FeatureRequest
   :synopsis: Query Neo4j Sample nodes and compute aggregation function over selected Feature nodes

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""

class FeatureRequest(BaseRequest):
    def __init__(self, request):
        super(FeatureRequest, self).__init__(request)
        self.params_keys = [self.datasource_param, self.feature_param, self.measurements_param]
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

    def get_data(self):

        tick_samples = self.params.get(self.measurements_param).replace("\"", "\'")
        result = None
        error = None
        response_status = 200

        # qryStr = "MATCH (ds:Datasource {label: '" + self.params.get(self.datasource_param) + "'}) " \
        #          "MATCH (ds)-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature) MATCH (f)-[:LEAF_OF]->()<-[v:COUNT]-(s:Sample)" \
        #          "USING INDEX s:Sample(id) WHERE (f.id='" + self.params.get(self.feature_param) + "') AND " \
        #          "s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg " \
        #          "RETURN distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as end, f.start as start, " \
        #          "f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order"
        qryStr = "MATCH (ds:Datasource {label: '%s'}) MATCH (ds)-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
                 "(f:Feature) MATCH (f)-[:LEAF_OF]->()<-[v:COUNT]-(s:Sample) USING INDEX s:Sample(id) WHERE " \
                 "(f.id='%s') AND s.id IN %s with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, " \
                 "f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, " \
                 "f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order" \
                 % (self.params.get(self.datasource_param), self.params.get(self.feature_param), tick_samples)

        try:
            rq_res = utils.cypher_call(qryStr)
            df = utils.process_result(rq_res)

            if len(df) > 0:
                # change column type
                df['index'] = df['index'].astype(int)
                df['start'] = df['start'].astype(int)
                df['end'] = df['end'].astype(int)
                df['order'] = df['order'].astype(int)

                # create a pivot_table where columns are samples and rows are features
                # for pandas > 0.17
                df_pivot = pandas.pivot_table(df, index=["id", "label", "index", "lineage", "lineageLabel", "start",
                                                         "end", "order"],
                                              columns="s.id", values="agg", fill_value=0).sortlevel("index")

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

                # sampleQryStr = "MATCH (ds:Datasource {label: '" + self.params.get(self.datasource_param) + "'})-" \
                #                "[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-[:LEAF_OF]->" \
                #                "()<-[v:COUNT]-(s:Sample) WHERE s.id IN " + tick_samples + " RETURN DISTINCT s"
                sampleQryStr = "MATCH (ds:Datasource {label: '%s'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->" \
                               "(f:Feature)-[:LEAF_OF]->()<-[v:COUNT]-(s:Sample) WHERE s.id IN %s RETURN DISTINCT s" \
                               % (self.params.get(self.datasource_param), tick_samples)

                sample_rq_res = utils.cypher_call(sampleQryStr)
                sample_df = utils.process_result_graph(sample_rq_res)

                vals = []
                for index, rowsamp in sample_df.iterrows():
                    temp = {}
                    for key in rowsamp.keys().values:
                        temp[key] = rowsamp[key]
                    if rowsamp['id'] in cols:
                        temp['count'] = cols[rowsamp['id']][0]
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

                rows = {"end": [], "start": [], "index": [], "metadata": {}}
                result = {"cols": cols, "rows": rows, "globalStartIndex": None}
        except:
            error_info = sys.exc_info()
            error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
            response_status = 500

        return result, error, response_status

