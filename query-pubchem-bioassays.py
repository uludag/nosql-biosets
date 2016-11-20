#!/usr/bin/python3

# Sample queries for PubChem Bioassays indices

import argparse

from elasticsearch import Elasticsearch


def query(es, index, qc):
    print("querying %s" % str(qc))
    r = es.search(index=index,
                  body={"size": 0, "query": qc})
    nhits = r['hits']['total']
    if debug:
        print("query returned %d entries" % nhits)
        for doc in r['hits']['hits']:
            aid = doc["_source"]['assay']['descr']['aid']['id']
            print("%s -- " % aid)
    return nhits


def aggquery(es, index, qc, aggqc):
    print("querying %s with aggregations" % str(qc))
    r = es.search(index=index,
                  body={"size": 0, "query": qc, "aggs": aggqc})
    if debug:
        print("agg query returned %d entries" % r['hits']['total'])
        for doc in r['hits']['hits']:
            aid = doc["_source"]['assay']['descr']['aid']['id']
            print("%s -- " % aid)
    return r


class Tests:
    def __init__(self, es, index, doc_type):
        self.es = es
        self.index = index
        self.doc_type = doc_type

    # Check number of tested substances for sample assays
    def check_numberoftestedsubstances(self):
        tsubts = ((638250, 5), (1120060, 27))
        for (aid, n) in tsubts:
            qc = { "match": {"assay.descr.aid.id": aid}}
            aggqc = {
                    "tested substances": { "terms": {
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
        qc = { "match": {"data.outcome": "active"}}
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
        if r['aggregations']['database']['buckets'][0]\
            ['substance']['buckets'][0]['doc_count'] < 200:
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
        if r['aggregations']['substance']['buckets'][0]['doc_count'] < 20:
            print("less than expected number of substances")


def main(es, index):
    tests = Tests(es, index, "bioassay")
    tests.query_sample_assayids()
    tests.sample_aggregation_queries()
    tests.check_numberoftestedsubstances()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Query PubChem Bioassays Elasticsearch index')
    parser.add_argument('--index',
                        default="pubchem-bioassays-test21",
                        help='name of the elasticsearch index')
    parser.add_argument('--host', default="esnode-ruqayyah",
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default="9200",
                        help="Elasticsearch server port")
    parser.add_argument('--debug', default=False,
                        help="print more information")
    args = parser.parse_args()
    host = args.host
    port = args.port
    debug = args.debug
    con = Elasticsearch(host=host, port=port, timeout=600)
    main(con, args.index)
