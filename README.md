# neo4j-data-provider

# Description

This Flask app sends queries to a Neo4j instance.

# Setting up

This app requires a running Neo4j instance with the default setting to localhost. The default port for Neo4j is 7474, if NEO4j is running on a different port, modify the url in the utils.py file.  

Neo4j requires a username and password to access the database.  To run this project, create a file named credential.py and specify two variables, neo4j_username and neo4j_password.  Alternatively, update utils.py with the username and password for the Neo4j instance.

# Running

To run this project, call 

`$: python metavizRoute.py`

