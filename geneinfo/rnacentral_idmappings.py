#!/usr/bin/env python
"""Index RNAcentral id mappings with Elasticsearch"""
import argparse
import csv
import gzip
import json
import logging
import os
import time

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


# Reader for RNAcentral id mappings
def mappingreader(infile):
    logging.debug("Reading %s " % infile)
    i = 0
    t1 = time.time()
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'rt')
    csvfile = f
    mappings = []
    previd = None
    for row in csv.reader(csvfile, delimiter='\t'):
        rid = row[0]
        if i != 0 and previd != rid:
            r = {'_id': previd, 'mappings': mappings}
            yield r
            mappings = []
        mappings.append({'db': row[1], 'id': row[2],
                         'org': row[3], 'type': row[4]})
        i += 1
        previd = rid
    yield {'_id': previd, 'mappings': mappings}
    f.close()
    t2 = time.time()
    logging.debug("-- %d id mappings line processed, in %dms"
                  % (i, (t2 - t1) * 1000))


# Index RNAcentral id mappings csvfile with Elasticsearch
def es_index_idmappings(es, csvfile, reader):
    for ok, result in streaming_bulk(
            es, reader(csvfile),
            index=args.index, doc_type='rnacentralidmapping', chunk_size=512
    ):
        if not ok:
            action, result = result.popitem()
            doc_id = '/%s/commits/%s' % (args.index, result['_id'])
            print('Failed to %s document %s: %r' % (action, doc_id, result))
    return


def main(es, infile, index):
    if es.indices.exists(index=index):
        es.indices.delete(index=index, params={"timeout": "10s"})
    indxcfg = {"settings": {
        "index.number_of_replicas": 0, "index.refresh_interval": '360s'}}
    r = es.indices.create(index=index, params={"timeout": "10s"},
                          ignore=400, wait_for_active_shards=1)
    logging.debug(r)
    es.indices.put_settings(index=index, body=indxcfg)
    es_index_idmappings(es, infile, mappingreader)
    es.indices.refresh(index=index)


if __name__ == '__main__':
    conf = {"host": "localhost", "port": 9200}
    d = os.path.dirname(os.path.abspath(__file__))
    try:
        conf = json.load(open(d + "/../conf/elasticsearch.json", "r"))
    finally:
        pass
    parser = argparse.ArgumentParser(
        description='Index RNAcentral id mappings with Elasticsearch')
    parser.add_argument('--infile', help='input file to index')
    parser.add_argument('--index',
                        default="rnacentral-idmapping",
                        help='Name of the Elasticsearch index')
    parser.add_argument('--host', default="bio2rdf",
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default="9200",
                        help="Elasticsearch server port")
    args = parser.parse_args()
    con = Elasticsearch(host=args.host, port=args.port, timeout=3600)
    main(con, args.infile, args.index)
