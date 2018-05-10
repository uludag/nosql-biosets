from __future__ import print_function

import json
import logging
import os
import sys

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ElasticsearchException
from pymongo import MongoClient
from neo4j.v1 import GraphDatabase, basic_auth

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


class DBconnection(object):
    i = 0  # counter for the number of objects indexed

    # TODO: rename es_indexsettings as es_settings
    def __init__(self, db, index, host=None, port=None,
                 user=None, password=None, recreateindex=False,
                 es_indexsettings=None, es_indexmappings=None):
        d = os.path.dirname(os.path.abspath(__file__))
        assert index is not None
        self.index = index
        self.db = db
        if port is not None and not isinstance(port, int):
            port = int(port)
        try:
            # TODO: option to specify config file
            confile = "./conf/dbservers.json"
            if not os.path.exists(confile) \
                    and os.path.exists("./dbservers.json"):
                confile = "./dbservers.json"
            else:
                confile = d + "/../conf/dbservers.json"
            with open(confile, "r") as conff:
                conf = json.load(conff)
        except IOError:
            conf = {"es_host": "localhost", "es_port": 9200,
                    "mongodb_host": "localhost", "mongodb_port": 27017}
        if db == 'Elasticsearch':
            if host is None:
                host = conf['es_host']
            if port is None:
                port = conf['es_port']
            # TODO: should ES index default be * ?
            self.es = Elasticsearch(host=host, port=port, timeout=120,
                                    maxsize=130)
            logger.info("New Elasticsearch connection to host '%s'" % host)
            self.check_elasticsearch_index(recreateindex, es_indexsettings,
                                           es_indexmappings)
        elif db == 'Neo4j':
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
            if port is None:
                port = conf['mongodb_port']
            mc = MongoClient(host, port)
            logger.info("New MongoDB connection: '%s:%d'" % (host, port))
            self.mdbi = mc[index]
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
                    self.es.indices.delete(index=self.index,
                                           params={"timeout": "10s"})
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
