#!/usr/bin/env python3
""" Simple queries with PubChem Bioassays indices """

# TODO: use 'assert' for checking test results

import unittest

from nosqlbiosets.dbutils import DBconnection


def query(es, index, qc):
    print("querying %s" % str(qc))
    r = es.search(index=index,
                  body={"size": 0, "query": qc})
    nhits = r['hits']['total']
    return nhits


def aggquery(es, index, qc, aggqc):
    print("Querying %s with aggregations %s" % (str(qc), str(aggqc)))
    r = es.search(index=index,
                  body={"size": 0, "query": qc, "aggs": aggqc})
    return r


class Tests(unittest.TestCase):
    es, index, doc_type = None, None, None

    def init(self, db, index, doc_type):
        self.index = index
        self.doc_type = doc_type
        dbc = DBconnection(db, index)
        self.es = dbc.es

    # Check number of tested substances for sample assays
    def check_numberoftestedsubstances(self):
        tsubts = ((638250, 5), (1120060, 27), (1224859, 5683))
        for (aid, n) in tsubts:
            qc = {"match": {"assay.descr.aid.id": aid}}
            aggqc = {
                "tested substances": {"terms": {
                    "field": "data.sid",
                    "size": 1
                }}}
            r = aggquery(self.es, self.index, qc, aggqc)
            ts = r['aggregations']['tested substances']
            a = ts['sum_other_doc_count'] + len(ts['buckets'])
            if n != a:
                print("assay has %d tested substances, expected: %d" % (a, n))

    def query_sample_assayids(self):
        l = [123, 1120060, 742589]
        for aid in l:
            qc = {"match": {"assay.descr.aid.id": aid}}
            n = query(self.es, self.index, qc)
            if n != 1:
                print("assay with id %d not found" % aid)

    def sample_aggregation_queries(self):
        # distribution of outcome methods with databases
        # for assays with at least one active outcome
        qc = {"match": {"data.outcome": "active"}}
        aggqc = {
            "database": {
                "terms": {
                    "field": "assay.descr.aid_source.db.name",
                    "size": 10},
                "aggs": {
                    "outcome_method": {
                        "terms": {
                            "field":
                                "assay.descr.activity_outcome_method",
                            "size": 10
                        }}}}}
        r = aggquery(self.es, self.index, qc, aggqc)
        if r['hits']['total'] < 4000:
            print("less than expected number of assays")
        if r['aggregations']['database']['buckets'][0]['doc_count'] < 3000:
            print("less than expected number of results")
        # distribution of substances with databases
        # for assays with target molecule type protein
        qc = {"match": {"assay.descr.target.molecule_type": "protein"}}
        aggqc = {
            "database": {
                "terms": {
                    "field": "assay.descr.aid_source.db.name",
                    "size": 100},
                "aggs": {
                    "substance": {
                        "terms": {
                            "field":
                                "data.sid",
                            "size": 10
                        }}}}}
        r = aggquery(self.es, self.index, qc, aggqc)
        b = r['aggregations']['database']['buckets']
        if len(b) > 0 and b[0]['substance']['buckets'][0]['doc_count'] < 200:
            print("less than expected number of substances")
        # distribution of reference genes with substances
        # for assays with reference genes defined
        qc = {"exists": {"field": "assay.descr.xref.xref.gene"}}
        aggqc = {
            "substance": {
                "terms": {
                    "field": "data.sid",
                    "size": 100},
                "aggs": {
                    "gene": {
                        "terms": {
                            "field":
                                "assay.descr.xref.xref.gene",
                            "size": 10
                        }}}}}
        r = aggquery(self.es, self.index, qc, aggqc)
        sb = r['aggregations']['substance']['buckets']
        if len(sb) == 0 or sb[0]['doc_count'] < 20:
            print("Less than expected number of substances")

    def test_es(self):
        db, index = "Elasticsearch", "pubchem-tests"
        tests = Tests()
        tests.init(db, index, "bioassay")
        tests.query_sample_assayids()
        tests.sample_aggregation_queries()
        tests.check_numberoftestedsubstances()


if __name__ == '__main__':
    unittest.main()
