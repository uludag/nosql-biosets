from elasticsearch import Elasticsearch
from pymongo import MongoClient
import os
import json


class DBconnection(object):

    def __init__(self, db, index, host=None, port=None, recreateindex=True,
                 es_indexsettings=None):
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
            self.recreateindex(recreateindex, es_indexsettings)
        else:
            if host is None:
                host = conf['mongodb_host']
            if port is None:
                port = conf['mongodb_port']
            mc = MongoClient(host, port)
            self.mdbi = mc[index]

    def recreateindex(self, recreate, es_indexsettings):
        if self.db == 'Elasticsearch':
            if es_indexsettings is None:
                es_indexsettings = {"number_of_replicas": 0}
            e = self.es.indices.exists(index=self.index)
            if e and recreate:
                self.es.indices.delete(index=self.index,
                                       params={"timeout": "10s"})
                e = False
            if not e:
                self.es.indices.create(index=self.index,
                                       params={"timeout": "10s"}, ignore=400,
                                       body={"settings": es_indexsettings})

    def close(self):
        if self.db == 'Elasticsearch':
            self.es.indices.refresh(index=self.index)
