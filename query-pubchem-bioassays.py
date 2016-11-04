#!/usr/bin/python3

# Sample queries for PubChem Bioassays indices

# TODO: Look at MultiQC for presentation of results

import argparse

from elasticsearch import Elasticsearch

debug = False


def query(es, index, qc):
    print("querying %s" % str(qc))
    asets = es.search(index=index,
                      body={"size": 0, "query": qc})
    print("query returned %d entries" % asets['hits']['total'])
    if debug:
        for doc in asets['hits']['hits']:
            aid = doc["_source"]['PC_AssaySubmit']['assay']['descr']['aid']['id']
            print("%s -- " % aid)


def aggquery(es, index, qc, aggqc):
    print("querying %s" % str(qc))
    asets = es.search(index=index,
                      body={"size": 0, "query": qc, "aggs": aggqc})
    print("query returned %d entries" % asets['hits']['total'])
    print("dbname bucket has %d entries" %
          len(asets['aggregations']['dbname']['buckets']))
    if debug:
        for doc in asets['hits']['hits']:
            aid = doc["_source"]['PC_AssaySubmit']['assay']['descr']['aid']['id']
            print("%s -- " % aid)


def query_assayids(es, index):
    l = [123, 234, 502]
    for aid in l:
        qc = {"match": {"PC_AssaySubmit.assay.descr.aid.id": aid}}
        query(es, index, qc)


def queries(es, index):
    query_assayids(es, index)
    qc = {"match": {"PC_AssaySubmit.data.outcome": "active"}}
    query(es, index, qc)
    qc = {"query_string": {"query": "cancer"}}
    query(es, index, qc)


def aggqueries(es, index):
    qc = { "match": {"PC_AssaySubmit.data.outcome": "active"}}
    aggqc = {
        "dbname": {
            "terms": {
                "field": "PC_AssaySubmit.assay.descr.aid_source.db.name",
                "size": 100
            },
            "aggs": {
                "outcome": {
                    "terms": {
                        "field":
                            "PC_AssaySubmit.assay.descr.activity_outcome_method",
                        "size": 10
                    }
                }
            }
        }}
    aggquery(es, index, qc, aggqc)


def main(es, index):
    queries(es, index)
    aggqueries(es, index)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Query PubChem Bioassays Elasticsearch index')
    parser.add_argument('--index',
                        default="pubchem-bioassays-test14",
                        help='name of the elasticsearch index')
    parser.add_argument('--host', default="esnode-ruqayyah",
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default="9200",
                        help="Elasticsearch server port")
    args = parser.parse_args()
    host = args.host
    port = args.port
    con = Elasticsearch(host=host, port=port, timeout=600)
    main(con, args.index)
