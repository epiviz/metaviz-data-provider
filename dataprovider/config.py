class Configs(object):
    """

    """

    NEO4J_USER = "neo4j_user"
    NEO4J_PASSWORD = "neo4j_pass"
    NEO4J_HOST = "localhost"
    NEO4J_PORT = "7474"
    NEO4J_TRANSACTIONS_URL = "http://%s:%s/db/data/transaction/commit" % (NEO4J_HOST, NEO4J_PORT)
    NEO4J_DB_URL = "http://%s:%s/db/data" % (NEO4J_HOST, NEO4J_PORT)
    METAVIZ_WORKSPACE_URL = "http://metaviz.cbcb.umd.edu/data/main.php?requestId=8&version=4&action=getWorkspaces&ws="
    METAVIZ_REDIRECT_FILE_PATH = "./data/igs_workspace_lookup_table.csv"
    METAVIZ_REDIRECT_URL = "http://metaviz.cbcb.umd.edu/?ws="

preset_configs = Configs