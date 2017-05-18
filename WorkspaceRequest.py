import utils, pandas, sys
import ujson

def get_data(workspace_id, query_string):
	result = None
	if "-" in workspace_id:
		ws = workspace_id.split("-")
		print ws[1]
		qryStr = "MATCH (s:Sample {id: '" + ws[1] + "'}) RETURN s"
		try:
			rq_res = utils.cypher_call(qryStr)
			df = utils.process_result(rq_res)
			print(df)
			print("first row")
			rowdf = df.head(1).to_dict()
			# print(df.head(1).to_dict())
			print rowdf['s'][0]
			resTemplate = {"range":{"seqName":ws[0],"start":0,"width":953},"measurements":[{"id":ws[1],"name":ws[1],"type":"feature","datasourceId":ws[0],"datasourceGroup":ws[0],"dataprovider":ws[0],"formula":None,"defaultChartType":None,"annotation":rowdf['s'][0],"minValue":0.0577300543,"maxValue":161571.428571433,"metadata":["label","id","taxonomy1","taxonomy2","taxonomy3","taxonomy4","taxonomy5","taxonomy6","taxonomy7","lineage"],"description":"ihmp test data"},{"id":"ESM5GEZ8","name":"ESM5GEZ8","type":"feature","datasourceId":ws[0],"datasourceGroup":ws[0],"dataprovider":ws[0],"formula":None,"defaultChartType":None,"annotation":rowdf['s'][0],"minValue":0.4278795615226172,"maxValue":1.201675165902977,"metadata":["label","id","taxonomy1","taxonomy2","taxonomy3","taxonomy4","taxonomy5","taxonomy6","taxonomy7","lineage"],"description":"ihmp test data"}],"charts":{"data-structure":[{"id":"data-structure-icicle-Kc08V","type":"epiviz.ui.charts.tree.Icicle","properties":{"width":800,"height":350,"margins":{"top":50,"left":10,"bottom":10,"right":10},"visConfigSelection":{"measurements":[0],"datasourceGroup":ws[0],"annotation":{},"defaultChartType":"Navigation Control","minSelectedMeasurements":1},"colors":{"id":"d3-category20"},"modifiedMethods":{},"customSettings":{},"chartMarkers":[]}}],"plot":[{"id":"plot-heatmap-XhHot","type":"epiviz.plugins.charts.HeatmapPlot","properties":{"width":800,"height":400,"margins":{"top":120,"left":60,"bottom":20,"right":40},"visConfigSelection":{"measurements":[1],"annotation":{},"defaultChartType":"Heatmap","minSelectedMeasurements":1},"colors":{"id":"heatmap-default"},"modifiedMethods":{},"customSettings":{"colLabel":"label","maxColumns":120,"clusteringAlg":"agglomerative"},"chartMarkers":[]}}]}}

			return ujson.dumps(resTemplate)
		except:
			error_info = sys.exc_info()
			error = str(error_info[0]) + " " + str(error_info[1]) + " " + str(error_info[2])
			response_status = 500
			return result, error, response_status
	else:
		try:
			rq_res = utils.workspace_request(workspace_id, query_string)
			jsResp = ujson.loads(rq_res.text)
			print(jsResp['data'][0]['content'])
			return jsResp['data'][0]['content']
		except:
			error_info = sys.exc_info()
			error = str(error_info[0]) + " " + str(error_info[1]) + " " + str(error_info[2])
			response_status = 500
			return result, error, response_status
