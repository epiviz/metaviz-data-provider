import utils, pandas, sys
import ujson
from BaseRequest import BaseRequest

class WorkspaceRequest(BaseRequest):

  def __init__(self, request):
        super(WorkspaceRequest, self).__init__(request)
        self.params_keys = [self.workspace_id, self.query_string]
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
    result = None
    error = None
    response_status = 200
    if "-" in self.params.get(self.workspace_id):
        ws = self.params.get(self.workspace_id).split("-")
        qryStr = "MATCH (s:Sample {id: '" + ws[1] + "'}) RETURN s"
        try:
            rq_res = utils.cypher_call(qryStr)
            df = utils.process_result(rq_res)
            rowdf = df.head(1).to_dict()
            resTemplate = {"range":{"seqName":ws[0],"start":0,"width":953},"measurements":[{"id":ws[1],"name":ws[1],"type":"feature","datasourceId":ws[0],"datasourceGroup":ws[0],"dataprovider":ws[0],"formula":None,"defaultChartType":None,"annotation":rowdf['s'][0],"minValue":0.0577300543,"maxValue":161571.428571433,"metadata":["label","id","taxonomy1","taxonomy2","taxonomy3","taxonomy4","taxonomy5","taxonomy6","taxonomy7","lineage"],"description":"ihmp test data"},{"id":ws[1],"name":ws[1],"type":"feature","datasourceId":ws[0],"datasourceGroup":ws[0],"dataprovider":ws[0],"formula":None,"defaultChartType":None,"annotation":rowdf['s'][0],"minValue":0.4278795615226172,"maxValue":1.201675165902977,"metadata":["label","id","taxonomy1","taxonomy2","taxonomy3","taxonomy4","taxonomy5","taxonomy6","taxonomy7","lineage"],"description":"ihmp test data"}],"charts":{"data-structure":[{"id":"data-structure-icicle-Kc08V","type":"epiviz.ui.charts.tree.Icicle","properties":{"width":800,"height":350,"margins":{"top":50,"left":10,"bottom":10,"right":10},"visConfigSelection":{"measurements":[0],"datasourceGroup":ws[0],"annotation":{},"defaultChartType":"Navigation Control","minSelectedMeasurements":1},"colors":{"id":"d3-category20"},"modifiedMethods":{},"customSettings":{},"chartMarkers":[]}}],"plot":[{"id":"plot-heatmap-XhHot","type":"epiviz.plugins.charts.HeatmapPlot","properties":{"width":800,"height":400,"margins":{"top":120,"left":60,"bottom":20,"right":40},"visConfigSelection":{"measurements":[1],"annotation":{},"defaultChartType":"Heatmap","minSelectedMeasurements":1},"colors":{"id":"heatmap-default"},"modifiedMethods":{},"customSettings":{"colLabel":"label","maxColumns":120,"clusteringAlg":"agglomerative"},"chartMarkers":[]}}]}}
            result = ujson.dumps(resTemplate)
            return result, error, response_status

        except:
            error_info = sys.exc_info()
            error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
            response_status = 500
            return result, error, response_status
    else:
        try:
            rq_res = utils.workspace_request(self.params.get(self.workspace_id), self.params.get(self.query_string))
            jsResp = ujson.loads(rq_res.text)
            return jsResp['data'][0]['content'], error, response_status

        except:
            error_info = sys.exc_info()
            error = "%s %s %s" % (str(error_info[0]), str(error_info[1]), str(error_info[2]))
            response_status = 500
            return result, error, response_status
