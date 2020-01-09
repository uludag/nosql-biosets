from __future__ import print_function

import json
import logging
import os
import sys

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ElasticsearchException
from pymongo import MongoClient

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
logger.addHandler(ch)


class DBconnection(object):
    i = 0  # counter for the number of objects indexed

    def __init__(self, db, index, host=None, port=None, mdbcollection=None,
                 user=None, password=None, recreateindex=False,
                 es_indexsettings=None, es_indexmappings=None):
        assert index is not None
        self.index = index
        self.db = db
        if port is not None and not isinstance(port, int):
            port = int(port)
        try:
            # TODO: option to specify config file
            cfgfile = "./dbservers.json"
            if not os.path.exists(cfgfile):
                if os.path.exists("./conf/dbservers.json"):
                    cfgfile = "./conf/dbservers.json"
                elif os.path.exists("../conf/dbservers.json"):
                    cfgfile = "../conf/dbservers.json"
                else:
                    cfgfile = "../../conf/dbservers.json"
            logger.info("Servers configuration file: %s" % cfgfile)
            with open(cfgfile, "r") as cfgf:
                conf = json.load(cfgf)
        except IOError:
            conf = {"es_host": "localhost", "es_port": 9200,
                    "mongodb_host": "localhost", "mongodb_port": 27017}
        if db == 'Elasticsearch':
            if host is None:
                host = conf['es_host']
            if port is None:
                port = conf['es_port'] if 'es_port' in conf else 9200
            # TODO: should ES index default be * ?
            self.es = Elasticsearch(host=host, port=port, timeout=220,
                                    maxsize=80)
            if not self.es.ping():
                print('Elasticsearch server looks unreachable')
                exit()
            logger.info("New Elasticsearch connection to host '%s'" % host)
            self.check_elasticsearch_index(recreateindex, es_indexsettings,
                                           es_indexmappings)
        elif db == 'Neo4j':
            from neo4j import GraphDatabase, basic_auth
            if host is None:
                host = conf['neo4j_host']
            if port is None:
                port = conf['neo4j_port']
            if user is None:
                user = conf['neo4j_user']
            if password is None:
                password = conf['neo4j_password']
            self.driver = GraphDatabase.driver("bolt://{}:{}".
                                               format(host, port),
                                               auth=basic_auth(user, password))
            logger.info("New Neo4j connection to host '%s'" % host)
            self.neo4jc = self.driver.session()
        elif db == "MongoDB":
            if host is None:
                host = conf['mongodb_host']
            if port is None and 'mongodb_port' in conf:
                port = conf['mongodb_port']
            mc = MongoClient(host, port)
            logger.info("New MongoDB connection: '%s:%d'" % (host, port))
            self.mdbi = mc[index]
            if mdbcollection is not None:
                self.mdbcollection = mdbcollection
                if recreateindex :
                    self.mdbi.drop_collection(mdbcollection)
        else:  # Assume PostgreSQL
            from sqlalchemy import create_engine
            if port is None:
                port = 5432
            if host is None:
                host = 'localhost'
            url = 'postgresql://{}:{}@{}:{}/{}'
            url = url.format(user, password, host, port, index)
            self.sqlc = create_engine(url, client_encoding='utf8', echo=False)

    def check_elasticsearch_index(self, recreate, settings, indexmappings):
        if self.db == 'Elasticsearch':
            if len(self.index) > 0:
                e = self.es.indices.exists(index=self.index)
                if e and recreate:
                    print("Deleting existing index " + self.index)
                    r = self.es.indices.delete(index=self.index,
                                               params={"timeout": "20s"})
                    print(r)
                    # TODO: check delete-request return status
                    e = False
                if not e:
                    if settings is None:
                        settings = {"index.number_of_replicas": 0,
                                    "index.write.wait_for_active_shards": 1,
                                    "index.refresh_interval": "30s"}
                    if indexmappings is None:
                        indexmappings = {}
                    r = self.es.indices.create(index=self.index,
                                               params={"timeout": "10s"},
                                               ignore=400,
                                               body={"settings": settings,
                                                     "mappings": indexmappings})
                    if 'error' in r:
                        logger.error(r['error']['reason'])
                        raise ElasticsearchException(r['error']['reason'])

    def close(self):
        if self.db == 'Elasticsearch':
            self.es.indices.refresh(index=self.index)

    # Prints '.' to stdout as indication of progress after 'n' entries indexed
    def reportprogress(self, n=160):
        self.i += 1
        if self.i % n == 0:
            print(".", end='')
            sys.stdout.flush()
            if self.i % (n*80) == 0:
                print("{}".format(self.i))


def dbargs(argp, mdbdb='biosets', mdbcollection=None, esindex=None,
           multipleindices=False):
    """ Given ArgumentParser object, argp, add database arguments """
    argp.add_argument('--mdbdb',
                      default=mdbdb,
                      help='Name of the MongoDB database')
    if not multipleindices:
        argp.add_argument('--mdbcollection',
                          default=mdbcollection,
                          help='Collection name for MongoDB')
        argp.add_argument('--esindex',
                          default=esindex,
                          help='Index name for Elasticsearch')
    argp.add_argument('--recreateindex',
                      default=False,
                      help='Delete existing Elasticsearch index or MongoDB collection')
    argp.add_argument('--host',
                      help='Elasticsearch or MongoDB server hostname')
    argp.add_argument('--port', type=int,
                      help="Elasticsearch or MongoDB server port number")
    argp.add_argument('--dbtype', default='Elasticsearch',
                      help="Database: 'Elasticsearch' or 'MongoDB'")
    argp.add_argument('--user',
                      help="Database user name, "
                           "supported with PostgreSQL option only")
    argp.add_argument('--password',
                      help="Password for the database user, "
                           " supported with PostgreSQL option only")
