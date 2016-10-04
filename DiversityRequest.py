import utils
import pandas
import copy
import math
from sklearn.decomposition import PCA

def get_data(in_params_selectedLevels, in_params_samples):

    tick_samples = in_params_samples.replace("\"", "\'")
    diversity_type = "shannon"
    # get the min selected Level if aggregated at multiple levels
    minSelectedLevel = 6
    for level in in_params_selectedLevels.keys():
        if in_params_selectedLevels[level] == 2 and int(level) < minSelectedLevel:
            minSelectedLevel = int(level)

    qryStr = "MATCH (f:Feature)-[:LEAF_OF]->()<-[v:VALUE]-(s:Sample) WHERE (f.depth=" + str(minSelectedLevel) + ") AND s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order"

    rq_res = utils.cypher_call(qryStr)
    df = utils.process_result(rq_res)

    print(df)

    forDiversityDF = df[["agg", "s.id", "label"]]

    forDiversityMat = pandas.pivot_table(df, index=["label"], columns="s.id", values="agg", fill_value=0)

    alphaDiversityVals = []
    cols = {}
    sample_ids = list(set(forDiversityDF["s.id"]))
    if diversity_type == "shannon":
      for i in range(0, len(sample_ids)):

        l = forDiversityMat.ix[:,i].get_values()
        props = copy.deepcopy(l)
        alphaDiversity = 0.0
        totalSum = 0.0
        for j in range(0, len(l)):
            totalSum = totalSum + l[j]

        for j in range(0, len(l)):
            props[j] = l[j]/totalSum

        for j in range(0, len(props)):
            alphaDiversity = alphaDiversity + (props[j] * math.log1p(props[j]))

        alphaDiversityVals.append(alphaDiversity)
        print(forDiversityMat.columns.values[i])
        print(alphaDiversity)

        cols[forDiversityMat.columns.values[i]] = alphaDiversity

    print alphaDiversityVals
    print(cols)
    
    count = 0

    vals = []

    qryStr2 = "MATCH (s:Sample) WHERE s.id IN " + tick_samples + " RETURN s"

    rq_res2 = utils.cypher_call(qryStr2)
    df2 = utils.process_result_graph(rq_res2)
    print(df2)
    vals = []

    for index, row in df2.iterrows():
        temp = {}
        print(index)
        print(row)
        print(row.keys())
        print(row.keys().values)
        for key in row.keys().values:
            temp[key] = row[key]
        temp['alphaDiversity'] = cols[row['id']]
        print(temp)
        temp['sample_id'] = temp['id']
        del temp['id']
        vals.append(temp)

    print(vals)

    resRowsCols = {"data": vals}

    return resRowsCols