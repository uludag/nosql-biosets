
# Configuration files

[dbservers.json](dbservers.json): Includes default values for Elasticsearch,
 MongoDB, and Neo4j server's host names and port numbers

*  `es_host`: Hostname for the Elasticsearch server
*  `es_port`: Port number for the Elasticsearch server

*  `mongodb_host`: Hostname for the MongoDB server [*]
*  `mongodb_port`: Port number for the MongoDB server

*  `neo4j_host`: Hostname for the Neo4j server
*  `neo4j_port`: Port number for the Neo4j server
*  `neo4j_user`: User name for the Neo4j server
*  `neo4j_password`: Password for the Neo4j user


[*] The `mongodb_host` setting is the `host` parameter to pymongo
[MongoClient](
http://api.mongodb.com/python/current/api/pymongo/mongo_client.html) class.
It can be a full [mongodb URI](<http://dochub.mongodb.org/core/connections>),
in addition to a simple hostname.

Example `mongodb_host` setting as mongodb-URI:

    mongodb+srv://guest:guest@biosets-m3vbc.mongodb.net
