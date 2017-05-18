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

    lookup_table_file = "./data/igs_workspace_lookup_table.csv"
    df = pandas.read_csv(lookup_table_file)

    ds = df.loc[df['n.id'] == in_file_id]['datasource'].values[0]
    sample_name = df.loc[df['n.id'] == in_file_id]['sample_name'].values[0]

    redirect_url = "http://metaviz.cbcb.umd.edu/?ws=" + ds + "-" + sample_name

    return redirect_url
