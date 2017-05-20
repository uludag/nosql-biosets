#!/usr/bin/env python
""" Sample suggestion/search queries """

import json
import os
import unittest

from elasticsearch import Elasticsearch
from utils import enable_logging
enable_logging.enable_elasticsearch_logging()

typeaggs = {
    "type": {
        "terms": {
            "field": "_type",
            "size": 50,
            "order": {
                "_count": "desc"
            }}}}


class QuerySuggestions(unittest.TestCase):
    conf = {"host": "localhost", "port": 9200}
    d = os.path.dirname(os.path.abspath(__file__))
    try:
        conf = json.load(open(d + "/../conf/elasticsearch.json", "r"))
    finally:
        pass
    es = Elasticsearch(host=conf['host'], port=conf['port'], timeout=600)
    index = ""

    @staticmethod
    def suggest_queryc_pathdes(qterms):
        """Sample suggest query caluse, from PathDES project"""
        qc = {
                "bool": {
                    "must": [
                        {"query_string": {"query": "_type:compound"}},
                        {"match_phrase_prefix": {"_all": ' '.join(qterms)
                                                 }}]}}
        # TODO: highlights, typed_keys
        return qc

    @staticmethod
    def suggest_queryc_farna(qterms):
        """Sample suggest query caluse, from FARNA project"""
        qc = {
            "text": ' '.join(qterms),
            "termsuggestion": {
                "term": {
                    "prefix_length": 4,
                    "field": "_all",
                    "max_inspections": 100,
                    "min_word_length": 4,
                    "size": 6,
                    "suggest_mode": "always"
                }
            },
            "complsuggestion":
                {
                    "completion":
                        {
                            "field": "suggest",
                            "size": 10,
                            "fuzzy": "false"
                        }
                }
        }
        return qc

    @staticmethod
    def search_queryc_pathdes(qterms):
        """Sample search query caluse, from PathDES project"""
        qc = {
                "bool": {
                    "must": [
                        {"query_string": {"query": "_type:compound"}},
                        {
                            "bool": {
                                "should": [
                                    {
                                        "query_string": {
                                            "default_field": "_all",
                                            "default_operator": "AND",
                                            "query": ' '.join(qterms)
                                        }
                                    },
                                    {
                                        "match_phrase_prefix": {
                                            "_all": ' '.join(qterms)
                                        }
                                    }
                                ]
                            }
                        }
                        ]}}
        return qc

    @staticmethod
    def search_queryc_farna(qterms):
        """Sample search query caluse, from FARNA project"""
        qc = {
                "bool": {
                    "must": [
                        {
                            "query_string": {
                                "default_field": "_all",
                                "default_operator": "AND",
                                "query": ' '.join(qterms)
                            }
                        }
                        ]}}
        return qc

    def search_query(self, qc, doctype=None, index=None):
        r = self.es.search(doc_type=doctype, index=index,
                           _source=["xref.id", "desc", "source", "pmid"],
                           size=10, body={"query": qc, 'aggs': typeaggs})
        return r

    def suggest_query(self, qc, index=None):
        r = self.es.suggest(index=index, body=qc)
        return r

    def test_query_pathdes(self):
        """Make suggest query with sample query term 'kinase'
        then make sure all suggestions return hits with the search query
        """
        qterms = ['kinase']
        r = self.search_query(self.suggest_queryc_pathdes(qterms))
        hits = r['hits']['hits']
        for hit in hits:
            qt = hit['_source']['desc'].replace('[', '\[').\
                replace(']', '\]').replace('/', '\/')
            print("desc: %s" % qt)
            qc = self.search_queryc_pathdes(qterms=[qt])
            r = self.search_query(qc)
            self.assertGreater(r['hits']['total'], 0)

    def query_farna(self, qterm):
        """Execute FARNA suggest/search queries for given query term"""
        r = self.suggest_query(self.suggest_queryc_farna([qterm]),
                               index="farnacell_elim5_v2")
        for suggester in ["termsuggestion", "complsuggestion"]:
            for suggestions in r[suggester]:
                opts = suggestions['options']
                for opt in opts:
                    qt = opt['text']
                    print("opt: %s" % qt)
                    qc = self.search_queryc_farna(qterms=[qt])
                    qr = self.search_query(qc)
                    self.assertGreater(qr['hits']['total'], 0)

    def test_query_farna(self):
        """Make suggest query with sample query terms
        then make sure all suggestions return hits with the search query
        """
        qterms = ['kinase', 'p53', 'mir21', 'brca']
        for qterm in qterms:
            self.query_farna(qterm)


if __name__ == '__main__':
    unittest.main()
