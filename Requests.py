import copy
import math
import pandas
from sklearn.decomposition import PCA
import utils


def create_request_param_dict(order=None, selection=None, selected_levels=None, node_id=None, depth=None, samples=None, end=None, start=None):
    param_dict = {'order': order,
                  'selection': selection,
                  'selected_levels': selected_levels,
                  'node_id': node_id,
                  'depth': depth,
                  'samples': samples,
                  'end': end,
                  'start': start}

    return param_dict


class Request(object):

    def __init__(self, params):
        self.params = params
        self.result = None

    def get_data(self):
        raise Exception("NotImplementedException")

    def get_measurements(self):
        raise Exception("NotImplementedException")


class PartitionsRequest(Request):

    def __init__(self, params):
        super(PartitionsRequest, self).__init__(params)

    def get_data(self):
        qryStr = "MATCH (f:Feature {id:'0-0'}) RETURN  f.start as start, f.end as end"

        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

        arr = []
        arr.append([None, df['start'][0], df['end'][0]])

        errorStr = None

        return arr


class MeasurementsRequest(Request):

    def __init__(self, params):
        super(MeasurementsRequest, self).__init__(params)

    def get_data(self):
        measurements = []

        qryStr = "MATCH (s:Sample) RETURN s"

        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result_graph(rq_res)

        anno = []

        for index, row in df.iterrows():
            temp = row.to_dict()
            del temp['id']
            anno.append(temp)

        measurements = df['id'].values
        rowQryStr = "MATCH ()-[r]-() WHERE EXISTS(r.val) RETURN min(r.val) as minVal, max(r.val) as maxVal"

        rq_res2 = utils.cypher_call(rowQryStr)
        df2 = utils.process_result(rq_res2)

        errorStr = ""
        result = {"id": measurements, "name": measurements, "datasourceGroup": "ihmp", "datasourceId": "ihmp",
                  "defaultChartType": "", "type": "feature", "minValue": df2['minVal'][0], "maxValue": df2['maxVal'][0], "annotation": anno,
                  "metadata": ["label", "id", "taxonomy1", "taxonomy2", "taxonomy3", "taxonomy4", "taxonomy5",
                               "taxonomy6",
                               "taxonomy7", "lineage"]}

        return result


class CombinedRequest(Request):

    def __init__(self, params):
        super(CombinedRequest, self).__init__(params)

    def get_data(self):
        in_params_start = self.params.get('start')
        in_params_end = self.params.get('end')
        in_params_order = self.params.get('order')
        in_params_selection = self.params.get('selection')
        in_params_selectedLevels = self.params.get('selected_levels')
        in_params_samples = self.params.get('samples')

        #default aggregation
        # if len(in_params_selectedLevels) == 0:
        #     in_params_selectedLevels = {'3': 2}

        tick_samples = in_params_samples.replace("\"", "\'")

        # get the min selected Level if aggregated at multiple levels
        minSelectedLevel = 6
        for level in in_params_selectedLevels.keys():
            if in_params_selectedLevels[level] == 2 and int(level) < minSelectedLevel:
                minSelectedLevel = int(level)

        # user selection nodes for custom aggregation - decides the cut
        selNodes = "["
        selFlag = 0
        for node in in_params_selection.keys():
            if in_params_selection[node] == 2:
                selNodes += "'" +  node + "',"
                selFlag = 1

        if selFlag == 1:
            selNodes = selNodes[:-1]
        selNodes += "]"

        # if minSelectedLevel == 6:
        #     qryStr = "MATCH (f:Feature)<-[v:VALUE]-(s:Sample) USING INDEX s:Sample(id) WHERE (f.depth=" + str(
        #     minSelectedLevel) + " OR f.id IN " + selNodes + ") AND (f.start >= " + in_params_start + " AND f.end < " + in_params_end + ") AND s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order"
        # else:
        qryStr = "MATCH (f:Feature)-[:LEAF_OF]->()<-[v:VALUE]-(s:Sample) USING INDEX s:Sample(id) WHERE (f.depth=" + str(
            minSelectedLevel) + " OR f.id IN " + selNodes + ") AND (f.start >= " + in_params_start + " AND f.end <= " + in_params_end + ") AND s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order"

        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

        if len(df) > 0:
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

            #df_pivot = pandas.pivot_table(df, rows=["id", "label", "index", "lineage", "lineageLabel", "start", "end", "order"],
            #                              cols="s.id", values="agg", fill_value=0).sortlevel("index")

            # for pandas > 0.17
            df_pivot = pandas.pivot_table(df, index=["id", "label", "index", "lineage", "lineageLabel", "start", "end", "order"],
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

        else:
            cols = {}

            samples = eval(in_params_samples)

            for sa in samples:
                cols[sa] = []

            rows = { "end": [], "start": [], "index": [], "metadata": {} }
            resRowsCols = {"cols": cols, "rows": rows, "globalStartIndex": None}

        resRowsCols['measurements'] = {'cols': resRowsCols.get('cols'),
                                       'rows': resRowsCols.get('rows')}
        self.result = resRowsCols
        return resRowsCols


class PCARequest(Request):

    def __init__(self, params):
        super(PCARequest, self).__init__(params)

    def get_data(self):

        in_params_samples = self.params.get('samples')
        in_params_selectedLevels = self.params.get('selected_levels')

        tick_samples = in_params_samples.replace("\"", "\'")

        # get the min selected Level if aggregated at multiple levels
        minSelectedLevel = 6
        for level in in_params_selectedLevels.keys():
            if in_params_selectedLevels[level] == 2 and int(level) < minSelectedLevel:
                minSelectedLevel = int(level)

        qryStr = "MATCH (f:Feature)-[:LEAF_OF]->()<-[v:VALUE]-(s:Sample) WHERE (f.depth=" + str(minSelectedLevel) + ") AND s.id IN " + tick_samples + " with distinct f, s, SUM(v.val) as agg RETURN distinct agg, s.id, f.label as label, f.leafIndex as index, f.end as end, f.start as start, f.id as id, f.lineage as lineage, f.lineageLabel as lineageLabel, f.order as order"


        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

        print(df)

        forPCAdf = df[["agg", "s.id", "label"]]

        forPCAmat = pandas.pivot_table(df, index=["label"], columns="s.id", values="agg", fill_value=0)

        print(forPCAmat)
        pca = PCA(n_components = 2)
        pca.fit(forPCAmat)
        print(pca.components_)

        cols = {}
        cols['pca1'] = pca.components_[0]
        cols['pca2']= pca.components_[1]

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
            temp['pca1'] = cols['pca1'][index]
            temp['pca2'] = cols['pca2'][index]
            print(temp)
            temp['sample_id'] = temp['id']
            del temp['id']
            vals.append(temp)


        # for col in forPCAmat:
        #     row = {}
        #     row['pca1'] = cols['pca1'][count]
        #     row['pca2'] = cols['pca2'][count]
        #     row['sample_id'] = col
        #     vals.append(row)
        #     count = count+1

        print(vals)

        measurements = {}
        for s in vals:
            sample_id = s.get('sample_id')
            pca1 = s.get('pca1')
            pca2 = s.get('pca2')
            measurements[sample_id] = {'pca1': pca1,
                                       'pca2': pca2 }

        resRowsCols = {"data": vals,
                       "measurements": measurements}
        self.result = resRowsCols
        return resRowsCols

    def get_measurements(self):
        if self.result:
            return self.result.get('measurements')
        return self.get_data().get('measurements')


class HierarchyRequest(Request):

    def __init__(self, params):
        super(HierarchyRequest, self).__init__(params)

    def get_data(self):
        in_params_selection = self.params.get('selection')
        in_params_order = self.params.get('order')
        in_params_selected_levels = self.params.get('selected_levels')
        in_params_nodeId = self.params.get('node_id')

        root_node = in_params_nodeId
        root_node = root_node.replace('"', "")

        taxonomy = False

        if len(root_node) == 0 or root_node == "0-0":
            root_node = "0-0"
            qryStr = "MATCH (f:Feature {id:'" + root_node + "'})-[:PARENT_OF*0..3]->(f2:Feature) " \
                                                            "with collect(f2) + f as nodesFeat unwind nodesFeat as ff " \
                                                            "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
                                                            "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as partition, " \
                                                            "ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as nchildren, ff.nleaves as nleaves, " \
                                                            "ff.order as order " \
                                                            "order by ff.depth, ff.leafIndex, ff.order"

            tQryStr = "MATCH (f:Feature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth"
            taxonomy = True
        else:
            qryStr = "MATCH (f:Feature {id:'" + root_node + "'})-[:PARENT_OF*0..3]->(f2:Feature) OPTIONAL MATCH (f)<-[:PARENT_OF]-(fParent:Feature) " \
                                                            "with collect(f2) + f + fParent as nodesFeat unwind nodesFeat as ff " \
                                                            "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
                                                            "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as partition, " \
                                                            "ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as nchildren, ff.nleaves as nleaves, " \
                                                            "ff.order as order " \
                                                            "order by ff.depth, ff.leafIndex, ff.order"

        rq_res = utils.cypher_call(qryStr)
        df = utils.process_result(rq_res)

        if len(df) > 0:
            # convert columns to int
            df['start'] = df['start'].astype(int)
            df['end'] = df['end'].astype(int)
            df['order'] = df['order'].astype(int)
            df['leafIndex'] = df['leafIndex'].astype(int)
            df['nchildren'] = df['nchildren'].astype(int)
            df['nleaves'] = df['nleaves'].astype(int)
            df['depth'] = df['depth'].astype(int)
            df['depth'] = df['depth'].astype(int)

            # restore current order, selection and levels from input params
            for key in in_params_order.keys():
                df.loc[df['id'] == key, 'order'] = in_params_order[key]

            for key in in_params_selection.keys():
                df.loc[df['id'] == key, 'selectionType'] = in_params_selection[key]

            for key in in_params_selected_levels.keys():
                df.loc[df['depth'] == int(key), 'selectionType'] = in_params_selected_levels[key]

            root = df.iloc[0]
            other = df.loc[1:,]

            rootDict = HierarchyRequest.row_to_dict(root)
            result = HierarchyRequest.df_to_tree(rootDict, other)

            if taxonomy:
                trq_res = utils.cypher_call(tQryStr)
                tdf = utils.process_result(trq_res)

                result['rootTaxonomies'] = tdf['taxonomy'].values.tolist()

            return result


    @staticmethod
    def row_to_dict(row):
        toRet = {}
        # toRet['lineage'] = row['lineage']
        toRet['end'] = row['end']
        toRet['partition'] = None
        toRet['leafIndex'] = row['leafIndex']
        toRet['nchildren'] = row['nchildren']
        toRet['label'] = row['label']
        toRet['name'] = row['label']
        toRet['start'] = row['start']
        toRet['depth'] = row['depth']
        toRet['globalDepth'] = row['depth']
        # toRet['lineageLabel'] = row['lineageLabel']
        toRet['nleaves'] = row['nleaves']
        toRet['parentId'] = row['parentId']
        toRet['order'] = row['order']
        toRet['id'] = row['id']
        toRet['selectionType'] = 1
        toRet['taxonomy'] = 'taxonomy' + (str(int(toRet['depth']) + 1))
        toRet['size'] = 1
        toRet['children'] = []
        return toRet

    @staticmethod
    def df_to_tree(root, df):

        children = df[df['parentId'] == root['id']]

        if len(children) == 0:
            root['children'] = None
            return root

        otherChildren = df[~(df['parentId'] == root['id'])]
        # children.sort_values('order')
        # for old version of pandas
        children.sort('order')
        # root['size'] = len(children)

        for index,row in children.iterrows():
            childDict = HierarchyRequest.row_to_dict(row)
            subDict = HierarchyRequest.df_to_tree(childDict, otherChildren)
            if subDict is None:
                root['children'] = None
            else:
                root['children'].append(subDict)

        return root


class DiversityRequest(Request):

    def __init__(self, params):
        super(DiversityRequest, self).__init__(params)

    def get_data(self):
        in_params_selectedLevels = self.params.get('selected_levels')
        in_params_samples = self.params.get('samples')

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

        measurements = {}
        for s in vals:
            sample_id = s.get('sample_id')
            alpha_diversity = s.get('alphaDiversity')
            measurements[sample_id] = {'alphaDiversity': alpha_diversity}

        resRowsCols = {"data": vals,
                       "measurements": measurements}
        self.result = resRowsCols

        return resRowsCols

    def get_measurements(self):
        if self.result:
            return self.result.get('measurements')
        return self.get_data().get('measurements')
    