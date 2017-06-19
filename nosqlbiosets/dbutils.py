from elasticsearch import Elasticsearch
from pymongo import MongoClient
import os
import json


class DBconnection(object):

    def __init__(self, db, index, host=None, port=None):
        d = os.path.dirname(os.path.abspath(__file__))
        try:
            conf = json.load(open(d + "/../conf/dbservers.json", "r"))
        except IOError:
            conf = {"es_host": "localhost", "es_port": 9200,
                    "mongodb_host": "localhost", "mongodb_port": 27017}

        if db == 'Elasticsearch':
            if host is None:
                host = conf['es_host']
            if port is None:
                port = conf['es_port']
            self.es = Elasticsearch(host=host, port=port, timeout=120)
            # if es.indices.exists(index=index):
            #    es.indices.delete(index=index, params={"timeout": "10s"})
            self.es.indices.create(index=index, params={"timeout": "10s"},
                                   ignore=400,
                                   body={"settings": {"number_of_replicas": 0}})
        else:
            if host is None:
                host = conf['mongodb_host']
            if port is None:
                port = conf['mongodb_port']
            mc = MongoClient(host, port)
            self.mdbi = mc[index]
