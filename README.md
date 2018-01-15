# metaviz-data-provider

# Description

This Flask app sends queries to a Neo4j instance.

# Setting up

This app requires a running Neo4j instance with the default setting to localhost. The default port for Neo4j is 7474, if NEO4j is running on a different port, modify the url and port in the config.py file.

Neo4j requires a username and password to access the database.  To run this project, update the config.py with the values of two variables, neo4j_username and neo4j_password.

# Running

To run this project, call 

`$: python run.py`

