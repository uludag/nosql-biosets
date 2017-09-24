#!/usr/bin/env python
""" Sample suggest/search queries with Elasticsearch"""

import unittest

from nosqlbiosets.dbutils import DBconnection
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
    index = ""
    es = DBconnection("Elasticsearch", index).es

    @staticmethod
    def prefix_queryclause(qterms):
        """Sample match_phrase_prefix query caluse"""
        qc = {
                "bool": {
                    "must": [
                        {"query_string": {"query": "_type:protein"}},
                        {"match_phrase_prefix": {"_all": ' '.join(qterms)
                                                 }}]}}
        # TODO: highlights, typed_keys
        return qc

    @staticmethod
    def term_queryclause(qterms):
        """Sample suggest query caluse"""
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
            }
            # "complsuggestion":
            #     {
            #         "completion":
            #             {
            #                 "field": "suggest",
            #                 "size": 10,
            #                 "fuzzy": False
            #             }
            #     }
        }
        return qc

    @staticmethod
    def search_queryc_pathdes(qterms):
        """Sample search query caluse"""
        qc = {
                "bool": {
                    "must": [
                        {"query_string": {"query": "_type:protein"}},
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
    def search_queryclause(qterms):
        """Sample search query caluse"""
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

    def test_prefix_suggest_queries(self):
        """Make suggest query with sample query terms'
        then make sure all suggestions return hits with the search query
        """
        qterms = ['kinase']
        r = self.search_query(self.prefix_queryclause(qterms))
        hits = r['hits']['hits']
        for hit in hits:
            qt = hit['_source']['desc'].replace('[', '\[').\
                replace(']', '\]').replace('/', '\/')
            print("desc: %s" % qt)
            qc = self.search_queryc_pathdes(qterms=[qt])
            r = self.search_query(qc)
            self.assertGreater(r['hits']['total'], 0)

    def suggestquery(self, qterm):
        """Execute suggest/search queries for given query term"""
        r = self.es.suggest(body=self.term_queryclause([qterm]),
                            index="biosets")
        for suggester in ["termsuggestion"]:  # "complsuggestion"
            for suggestions in r[suggester]:
                opts = suggestions['options']
                for opt in opts:
                    qt = opt['text']
                    qc = self.search_queryclause(qterms=[qt])
                    qr = self.search_query(qc)
                    self.assertGreater(qr['hits']['total'], 0)

    def test_term_suggest_queries(self):
        """Make suggest query with sample query terms
        then make sure all suggestions return hits with the search query
        """
        qterms = ['kinase', 'p53', 'mir21', 'brca']
        for qterm in qterms:
            self.suggestquery(qterm)


if __name__ == '__main__':
    unittest.main()
