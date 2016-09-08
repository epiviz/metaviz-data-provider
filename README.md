# neo4j-data-provider

This project expects a running NEO4j instance on the machine.  Neo4j requires a username and password to be configured before it can be used.  To run this project, create a file named credential.py and specify two variables, neo4j_username and neo4j_pass.  To run this project, call 

`$: python metavizRoute.py`

# Neo4j installation - 

* if neo4j is not running on the default port, update the `url` in `utils.py`
* Update `utils.py` with the username and password for the neo4j instance
