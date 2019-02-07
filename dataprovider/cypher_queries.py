
def create_function_hier_request(in_datasource, all_nodes):

    function_hier_request = "MATCH (ds:FunctionDatasource {label: '" + in_datasource + "'})-[:FUNCTION_DATASOURCE_OF]->" \
                "(:FunctionFeature)-[:FUNCTION_PARENT_OF*]->(f:FunctionFeature)-[:FUNCTION_PARENT_OF*0..3]->(f2:FunctionFeature) " \
                "WHERE f.id IN " + all_nodes + " " \
                "OPTIONAL MATCH (f)<-[:PARENT_OF]-(fParent:FunctionFeature) with collect(f2) + f + fParent as nodesFeat " \
                "unwind nodesFeat as ff return distinct ff.start as start, " \
                "ff.label as label, ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, " \
                "ff.partition as partition, ff.end as end, ff.id as id, " \
                "ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order " \
                "ORDER by ff.depth, ff.leafIndex, ff.order"

    return function_hier_request

def function_hier_taxonomy_request(in_datasource):
    function_hier_taxonomy_request  = "MATCH (ds:FunctionDatasource {label: '" + in_datasource + "'})-" \
                "[:FUNCTION_DATASOURCE_OF]->(f:FunctionFeature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as " \
                "depth ORDER BY f.depth" + " UNION " + "MATCH (ds:FunctionDatasource {label: '" + in_datasource + "'})-" \
                "[:FUNCTION_DATASOURCE_OF]->(:FunctionFeature)-[:FUNCTION_PARENT_OF*]->(f:FunctionFeature) RETURN " \
                "DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth"
    return function_hier_taxonomy_request


def function_hier_without_root_request(in_datasource, root_node):
    qryStr = "MATCH (ds:FunctionDatasource {label: '" + in_datasource + "'})-[:FUNCTION_DATASOURCE_OF]->" \
            "(f:FunctionFeature {id:'" + root_node + "'})-[:FUNCTION_PARENT_OF*0..3]->(f2:FunctionFeature) " \
            "with collect(f2) + f as nodesFeat unwind nodesFeat as ff " \
            "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
            "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as partition, " \
            "ff.end as end, ff.id as id, " \
            "ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order ORDER by ff.depth, ff.leafIndex, ff.order"

    return qryStr

def function_hier_taxonomy_without_root_request(in_datasource):

    tQryStr = "MATCH (ds:FunctionDatasource {label: '" + in_datasource + "'})-[:FUNCTION_DATASOURCE_OF]->" \
            "(f:FunctionFeature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth" + " UNION " + "MATCH (ds:FunctionDatasource {label: '" + in_datasource + "'})-[:FUNCTION_DATASOURCE_OF]->(:FunctionFeature)-[:FUNCTION_PARENT_OF*]->(f:FunctionFeature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth"
    return tQryStr


def function_hier_with_root_request(in_datasource, root_node):
    qryStr = "MATCH (ds:FunctionDatasource {label: '" + in_datasource + "'})-[:FUNCTION_DATASOURCE_OF]->(:FunctionFeature)-[:FUNCTION_PARENT_OF*]->(f:FunctionFeature {id:'" + root_node + "'})-[:FUNCTION_PARENT_OF*0..3]->(f2:FunctionFeature) " \
    "OPTIONAL MATCH (f)<-[:FUNCTION_PARENT_OF]-(fParent:FunctionFeature) with collect(f2) + f + fParent as nodesFeat " \
    "unwind nodesFeat as ff return distinct ff.start as start, " \
    "ff.label as label, ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, " \
    "ff.partition as partition, ff.end as end, ff.id as id, " \
    "ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order " \
    "ORDER by ff.depth, ff.leafIndex, ff.order"

    return qryStr

def function_hier_taxonomy_with_root_request(in_datasource):
    tQryStr = "MATCH (ds:FunctionDatasource {label: '" + in_datasource + "'})-[:FUNCTION_DATASOURCE_OF]->(f:FunctionFeature) " \
    "RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth" + " UNION " + "MATCH (ds:FunctionDatasource " \
    "{label: '" + in_datasource + "'})-[:FUNCTION_DATASOURCE_OF]->(:FunctionFeature)-[:FUNCTION_PARENT_OF*]->(f:FunctionFeature) " \
    "RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth"
    return tQryStr

def function_hier__otu_request(in_datasource, otu_parent_id):
    qryStr = "MATCH (ds:FunctionDatasource {label: '" + in_datasource + "'})-[:FUNCTION_DATASOURCE_OF]->(:FunctionFeature)-[:FUNCTION_PARENT_OF*]->(f:FunctionFeature {id:'" + otu_parent_id + "'})-[:FUNCTION_PARENT_OF*0..3]->(f2:FunctionFeature) " \
    "OPTIONAL MATCH (f)<-[:FUNCTION_PARENT_OF]-(fParent:FunctionFeature) with collect(f2) + f + fParent as nodesFeat " \
    "unwind nodesFeat as ff return distinct ff.start as start, " \
    "ff.label as label, ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, " \
    "ff.partition as partition, ff.end as end, ff.id as id, " \
    " ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order " \
    "ORDER by ff.depth, ff.leafIndex, ff.order"
    return qryStr

def function_hier_taxonomy_otu_request(in_datasource):
    tQryStr = "MATCH (ds:FunctionDatasource {label: '" + in_datasource + "'})-[:FUNCTION_DATASOURCE_OF]->(f:Feature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth" + " UNION " + "MATCH (ds:FunctionDatasource {label: '" + in_datasource + "'})-[:FUNCTION_DATASOURCE_OF]->(:FunctionFeature)-[:FUNCTION_PARENT_OF*]->(f:FunctionFeature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth"
    return tQryStr

def otu_expand_request_taxa_hier_with_filter(in_datasource, all_nodes, filter_id):
    qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-[:PARENT_OF*0..3]->(f2:Feature)-[:LEAF_OF]->()<-[:KO_TERM_OF]-()<-[:LEAF_OF]-(k:Feature) " \
            "WHERE f.id IN " + all_nodes + " AND k.id IN " + str(filter_id) + " " \
            "OPTIONAL MATCH (f)<-[:PARENT_OF]-(fParent:Feature) with collect(f2) + f + fParent as nodesFeat " \
            "unwind nodesFeat as ff return distinct ff.lineage as lineage, ff.start as start, " \
            "ff.label as label, ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, " \
            "ff.partition as partition, ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, " \
            "ff.nchildren as nchildren, ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order " \
            "ORDER by ff.depth, ff.leafIndex, ff.order"
    return qryStr

def otu_expand_request_taxa_hier_without_filter(in_datasource, all_nodes):
    qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature)-[:PARENT_OF*0..3]->(f2:Feature) " \
             "WHERE f.id IN " + all_nodes + " " \
             "OPTIONAL MATCH (f)<-[:PARENT_OF]-(fParent:Feature) with collect(f2) + f + fParent as nodesFeat " \
             "unwind nodesFeat as ff return distinct ff.lineage as lineage, ff.start as start, " \
             "ff.label as label, ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, " \
             "ff.partition as partition, ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, " \
             "ff.nchildren as nchildren, ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order " \
             "ORDER by ff.depth, ff.leafIndex, ff.order"
    return qryStr

def otu_expand_request_taxa_taxonomy_request(in_datasource):
    tQryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(f:Feature) RETURN DISTINCT " \
            "f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth" + " UNION " + "MATCH (ds:Datasource " \
            "{label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature) RETURN DISTINCT " \
            "f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth"
    return tQryStr

def not_otu_request_taxa_hier_with_filter_root(in_datasource, root_node, filter_id):
    qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(f:Feature {id:'" + root_node + "'})-" \
        "[:PARENT_OF*0..3]->(f2:Feature)-[:LEAF_OF]->()<-[:KO_TERM_OF]-()<-[:LEAF_OF]-(k:Feature) " \
        "WHERE k.id IN " + str(filter_id) + " with collect(f2) + f as nodesFeat unwind nodesFeat as ff " \
        "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
        "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as partition, " \
        "ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as nchildren, " \
        "ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order ORDER by ff.depth, ff.leafIndex, ff.order"
    return qryStr

def not_otu_request_taxa_hier_with_filter_not_root(in_datasource, root_node, filter_id):
    qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->()-[:PARENT_OF*]->(f:Feature {id:'" + root_node + "'})-" \
        "[:PARENT_OF*0..3]->(f2:Feature)-[:LEAF_OF]->()<-[:KO_TERM_OF]-()<-[:LEAF_OF]-(k:Feature) " \
        "WHERE k.id IN " + str(filter_id) + " with collect(f2) + f as nodesFeat unwind nodesFeat as ff " \
        "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
        "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as partition, " \
        "ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as nchildren, " \
        "ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order ORDER by ff.depth, ff.leafIndex, ff.order"
    return qryStr


def not_otu_request_taxa_hier_without_filter_root(in_datasource, root_node):
    qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(f:Feature {id:'" + root_node + "'})-[:PARENT_OF*0..3]->(f2:Feature)" \
    " with collect(f2) + f as nodesFeat unwind nodesFeat as ff " \
    "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
    "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as partition, " \
    "ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as nchildren, " \
    "ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order ORDER by ff.depth, ff.leafIndex, ff.order"
    return qryStr

def not_otu_request_taxa_hier_without_filter_not_root(in_datasource, root_node):
    qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->()-[:PARENT_OF*]->(f:Feature {id:'" + root_node + "'})-[:PARENT_OF*0..3]->(f2:Feature)" \
    " with collect(f2) + f as nodesFeat unwind nodesFeat as ff " \
    "return distinct ff.lineage as lineage, ff.start as start, ff.label as label, " \
    "ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, ff.partition as partition, " \
    "ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, ff.nchildren as nchildren, " \
    "ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order ORDER by ff.depth, ff.leafIndex, ff.order"
    return qryStr

def not_otu_request_taxa_taxonomy(in_datasource):
    tQryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(f:Feature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth" + " UNION " + "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth"
    return tQryStr


def otu_request_taxa_hier_with_filter(in_datasource, root_node, filter_id):
    qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature {id:'" + root_node + "'})-[:PARENT_OF*0..3]->(f2:Feature)-[:LEAF_OF]->()<-[:KO_TERM_OF]-()<-[:LEAF_OF]-(k:Feature) WHERE k.id IN " + str(
        filter_id) + " " \
                     "OPTIONAL MATCH (f)<-[:PARENT_OF]-(fParent:Feature) with collect(f2) + f + fParent as nodesFeat " \
                     "unwind nodesFeat as ff return distinct ff.lineage as lineage, ff.start as start, " \
                     "ff.label as label, ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, " \
                     "ff.partition as partition, ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, " \
                     "ff.nchildren as nchildren, ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order " \
                     "ORDER by ff.depth, ff.leafIndex, ff.order"
    return qryStr


def otu_request_taxa_hier_without_filter(in_datasource, root_node):

    qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature {id:'" + root_node + "'})-[:PARENT_OF*0..3]->(f2:Feature) " \
    "OPTIONAL MATCH (f)<-[:PARENT_OF]-(fParent:Feature) with collect(f2) + f + fParent as nodesFeat " \
    "unwind nodesFeat as ff return distinct ff.lineage as lineage, ff.start as start, " \
    "ff.label as label, ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, " \
    "ff.partition as partition, ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, " \
    "ff.nchildren as nchildren, ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order " \
    "ORDER by ff.depth, ff.leafIndex, ff.order"

    return qryStr

def otu_request_taxa_taxonomy(in_datasource):
    tQryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(f:Feature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth" + " UNION " + "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth"
    return tQryStr


def otu_parent_request_taxa_hier_with_filter(in_datasource, otu_parent_id, filter_id):

    qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature {id:'" + otu_parent_id + "'})-[:PARENT_OF*0..3]->(f2:Feature)-[:LEAF_OF]->()<-[:KO_TERM_OF]-()<-[:LEAF_OF]-(k:Feature) WHERE k.id IN " + str(
    filter_id) + " " \
                 "OPTIONAL MATCH (f)<-[:PARENT_OF]-(fParent:Feature) with collect(f2) + f + fParent as nodesFeat " \
                 "unwind nodesFeat as ff return distinct ff.lineage as lineage, ff.start as start, " \
                 "ff.label as label, ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, " \
                 "ff.partition as partition, ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, " \
                 "ff.nchildren as nchildren, ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order " \
                 "ORDER by ff.depth, ff.leafIndex, ff.order"
    return qryStr


def otu_parent_request_taxa_hier_without_filter(in_datasource, otu_parent_id):

    qryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature {id:'" + otu_parent_id + "'})-[:PARENT_OF*0..3]->(f2:Feature) " \
                             "OPTIONAL MATCH (f)<-[:PARENT_OF]-(fParent:Feature) with collect(f2) + f + fParent as nodesFeat " \
                             "unwind nodesFeat as ff return distinct ff.lineage as lineage, ff.start as start, " \
                             "ff.label as label, ff.leafIndex as leafIndex, ff.parentId as parentId, ff.depth as depth, " \
                             "ff.partition as partition, ff.end as end, ff.id as id, ff.lineageLabel as lineageLabel, " \
                             "ff.nchildren as nchildren, ff.taxonomy as taxonomy, ff.nleaves as nleaves, ff.order as order " \
                             "ORDER by ff.depth, ff.leafIndex, ff.order"
    return qryStr

def otu_parent_request_taxa_taxonomy(in_datasource):
    tQryStr = "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(f:Feature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth" + " UNION " + "MATCH (ds:Datasource {label: '" + in_datasource + "'})-[:DATASOURCE_OF]->(:Feature)-[:PARENT_OF*]->(f:Feature) RETURN DISTINCT f.taxonomy as taxonomy, f.depth as depth ORDER BY f.depth"
    return tQryStr
