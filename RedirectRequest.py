import utils, pandas

"""
.. module:: RedirectRequest
   :synopsis: Lookup Metaviz workspace id for an iHMP file node id

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""

def get_data(in_file_id):
    """
    Returns the Metaviz workspace id of an iHMP file id

    Args:
     in_file_id: iHMP file id

    Returns:
     redirect_url: Metaviz URL for workspace
    """

    lookup_table_file = "igs_workspace_lookup_table.csv"
    df = pandas.read_csv(lookup_table_file)
    ws_id = df.loc[df['n.id'] == in_file_id]['metaviz_ws_id'].values[0]

    redirect_url = "http://metaviz.cbcb.umd.edu/?ws=" + ws_id

    return redirect_url