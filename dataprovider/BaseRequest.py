class BaseRequest(object):
    """
    Request base class
    """

    def __init__(self, request):
        self.request = request
        self.params = None
        self.params_keys = None
        self.start_param = "params[start]"
        self.end_param = "params[end]"
        self.order_param = "params[order]"
        self.selection_param = "params[selection]"
        self.selectedLevels_param = "params[selectedLevels]"
        self.measurements_param = "params[measurements]"
        self.datasource_param = "params[datasource]"
        self.feature_param = "params[feature]"
        self.root_node_param = "params[nodeId]"
        self.depth_param = "params[depth]"
        self.filter = "params[filter]"
        self.featureList_param = "params[featureList]"
        self.file_id_param = "in_file_id"
        self.max_results_param = "params[maxResults]"
        self.search_query_param = "params[q]"
        self.workspace_id = "workspace_id"
        self.query_string = "query_string"
        self.timepoint_param = "params[timepoint]"

    def validate_params(self, request):
        """

        :param request:
        :return:
        """
        raise Exception("NotImplementedException")

    def get_data(self):
        """

        :return:
        """

        raise Exception("NotImplementedException")





