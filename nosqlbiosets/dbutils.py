from __future__ import print_function

import json
import logging
import os
import sys

from elasticsearch import Elasticsearch
from pymongo import MongoClient

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
logger.addHandler(ch)


class DBconnection(object):
    i = 0

    def __init__(self, db, index, host=None, port=None, recreateindex=False,
                 es_indexsettings=None, es_indexmappings=None):
        d = os.path.dirname(os.path.abspath(__file__))
        self.index = index
        self.db = db
        try:
            with open(d + "/../conf/dbservers.json", "r") as conff:
                conf = json.load(conff)
        except IOError:
            conf = {"es_host": "localhost", "es_port": 9200,
                    "mongodb_host": "localhost", "mongodb_port": 27017}
        if db == 'Elasticsearch':
            if host is None:
                host = conf['es_host']
            if port is None:
                port = conf['es_port']
            self.es = Elasticsearch(host=host, port=port, timeout=120)
            logger.debug('New Elasticsearch connection to host \'%s\'' % host)
            self.recreateindex(recreateindex, es_indexsettings,
                               es_indexmappings)
        else:
            if host is None:
                host = conf['mongodb_host']
            if port is None:
                port = conf['mongodb_port']
            mc = MongoClient(host, port)
            self.mdbi = mc[index]

    def recreateindex(self, recreate, es_indexsettings, es_indexmappings):
        if self.db == 'Elasticsearch':
            if es_indexsettings is None:
                es_indexsettings = {"number_of_replicas": 0}
            if es_indexmappings is None:
                es_indexmappings = {}
            e = self.es.indices.exists(index=self.index)
            if e and recreate:
                self.es.indices.delete(index=self.index,
                                       params={"timeout": "10s"})
                e = False
            if not e:
                self.es.indices.create(index=self.index,
                                       params={"timeout": "10s"}, ignore=400,
                                       body={"settings": es_indexsettings,
                                             "mappings": es_indexmappings})

    def close(self):
        if self.db == 'Elasticsearch':
            self.es.indices.refresh(index=self.index)

    # Prints '.' to stdout as indication of progress after 'n' entries indexed
    def reportprogress(self, n=160):
        self.i += 1
        if self.i % n == 0:
            print(".", end='')
            sys.stdout.flush()
