import tornado.ioloop
import tornado.web
import ujson
import pandas, numpy
import credential
import requests as rqs

class BaseHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header("Access-Control-Allow-Origin", "*")

class MeasurementsHandler(tornado.web.RequestHandler):
    def post(self):
        reqId = self.get_argument('id')

        qryStr = "MATCH (s:Sample) RETURN s.id as id"

        rq_res = cypher_call(qryStr)
        df = process_result(rq_res)

        measurements = df['id'].values.to_list()

        errorStr = ""
        result = {"id": measurements, "name": measurements, "datasourceGroup": "ihmp", "datasourceId": "ihmp",
                  "defaultChartType": "", "type": "feature", "minValue": 0.0, "maxValue": 10.0, "annotation": [],
                  "metadata": ["label", "id", "taxonomy1", "taxonomy2", "taxonomy3", "taxonomy4", "taxonomy5",
                               "taxonomy6",
                               "taxonomy7", "lineage"]}

        self.set_header("Access-Control-Allow-Origin", "*")
        self.write({"id": reqId, "error": errorStr, "result": result})

class HierarchyHandler(tornado.web.RequestHandler):
    def post(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.write("Hierarchy")

class PartitionHandler(tornado.web.RequestHandler):
    def post(self):

        qryStr = "MATCH (f:Feature {id:'0-0'}) RETURN  f.start as start, f.end as end"

        rq_res = cypher_call(qryStr)
        df = process_result(rq_res)

        arr = []
        arr.append([None, df[0]['start'], df[0]['end']])

        errorStr = None
        self.set_header("Access-Control-Allow-Origin", "*")
        self.write({"id": request.values['id'], "error": errorStr, "result": [arr]})

class CombinedHandler(tornado.web.RequestHandler):
    def post(self):
        # self.write("Combined")
        in_params_method = self.get_argument('method')
        in_params_end = self.get_argument('params[end]')
        in_params_start = self.get_argument('params[start]')
        in_params_order = eval(self.get_argument('params[order]'))
        in_params_selection = eval(self.get_argument('params[selection]'))
        in_params_selectedLevels = eval(self.get_argument('params[selectedLevels]'))
        in_samples = self.get_argument('params[measurements]')
        reqId = self.get_argument('id')

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
        # df_pivot = pandas.pivot_table(df,
          #                             rows=["id", "label", "index", "lineage", "lineageLabel", "start", "end", "order"],
            #                           cols="s.id", values="agg", fill_value=0).sortlevel("index")

        df_pivot = pandas.pivot_table(df,
                       index=["id", "label", "index", "lineage", "lineageLabel", "start", "end", "order"],
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

        errorStr = ""
        resRowsCols = {"cols": cols, "rows": rows, "globalStartIndex": (min(rows['start']))}
        self.set_header("Access-Control-Allow-Origin", "*")
        self.write({"id": reqId, "error": errorStr, "result": resRowsCols})

def make_app():
    return tornado.web.Application([
        (r"/measurements", MeasurementsHandler),
        (r"/partition", PartitionHandler),
        (r"/hierarchy", HierarchyHandler),
        (r"/combined", CombinedHandler),
    ])

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

if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
