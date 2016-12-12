import utils
import pandas
from sklearn.decomposition import PCA

"""
.. module:: PCARequest
   :synopsis: Query Neo4j Samples nodes and compute PCA over Features at specified level

.. moduleauthor:: Justin Wagner and Jayaram Kancherla

"""

def get_data(in_params_selectedLevels, in_params_samples):
    """
    Computes PCA over the selected samples and the given level of the hierarchy

    Args:
     in_params_selectedLevels:  Level of hierarchy of features to compute PCA
     in_params_samples: Samples to use to compute PCA

    Returns:
     resRowsCols: PCA for the samples at the selected level

    """

    tick_samples = in_params_samples.replace("\"", "\'")

    # get the min selected Level if aggregated at multiple levels
    minSelectedLevel = 6
    for level in in_params_selectedLevels.keys():
        if in_params_selectedLevels[level] == 2 and int(level) < minSelectedLevel:
            minSelectedLevel = int(level)

    qryStr = "MATCH (f:Feature)-[:LEAF_OF]->()<-[v:VALUE]-(s:Sample) WHERE (f.depth=" + str(minSelectedLevel) + ") " \
         "AND s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, f.label " \
         "as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, " \
         "f.lineageLabel as lineageLabel, f.order as order"


    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)

    forPCAmat = pandas.pivot_table(df, index=["label"], columns="s.id", values="agg", fill_value=0)
    
    pca = PCA(n_components = 2)
    pca.fit(forPCAmat)
    variance_explained = pca.explained_variance_ratio_

    cols = {}
    cols['PC1'] = pca.components_[0]
    cols['PC2']= pca.components_[1]

    samplesQryStr = "MATCH (s:Sample) WHERE s.id IN " + tick_samples + " RETURN s"

    samples_rq_res = utils.cypher_call(samplesQryStr)
    samples_df = utils.process_result_graph(samples_rq_res)
    vals = []

    for index, row in samples_df.iterrows():
        temp = {}
        for key in row.keys().values:
            temp[key] = row[key]
        temp['PC1'] = cols['PC1'][index]
        temp['PC2'] = cols['PC2'][index]
        temp['sample_id'] = temp['id']
        del temp['id']
        vals.append(temp)

    resRowsCols = {"data": vals}

    variance_explained[0] = round(variance_explained[0]*100.0, 2)
    variance_explained[1] = round(variance_explained[1]*100.0, 2)
    resRowsCols['pca_variance_explained'] = variance_explained

    return resRowsCols
