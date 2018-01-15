import utils, pandas
from BaseRequest import BaseRequest
import sys
from config import preset_configs

"""
.. module:: RedirectRequest
   :synopsis: Lookup Metaviz workspace id for an iHMP file node id
.. moduleauthor:: Justin Wagner and Jayaram Kancherla
"""

class RedirectRequest(BaseRequest):

  def __init__(self, request):
    super(RedirectRequest, self).__init__(request)
    self.params_keys = [self.file_id_param]
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
    """
    Returns the Metaviz workspace id of an iHMP file id
    Args:
     in_file_id: iHMP file id
    Returns:
     redirect_url: Metaviz URL for workspace
    """

    # lookup_table_file = "./data/igs_workspace_lookup_table.csv"
    lookup_table_file = preset_configs.METAVIZ_REDIRECT_FILE_PATH

    df = pandas.read_csv(lookup_table_file)

    ds = df.loc[df['n.id'] == self.params.get(self.file_id_param)]['datasource'].values[0]
    sample_name = df.loc[df['n.id'] == self.params.get(self.file_id_param)]['sample_name'].values[0]

    redirect_url = preset_configs.METAVIZ_REDIRECT_URL + ds + "-" + sample_name

    return redirect_url
