#!/usr/bin/env python
"""Index RNAcentral id mappings with Elasticsearch"""
import argparse
import csv
import gzip
import logging
import os
import time

from elasticsearch.helpers import streaming_bulk

from nosqlbiosets.dbutils import DBconnection

DOCTYPE = "rnacentral"
CHUNKSIZE = 124


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
            index=args.index, doc_type=DOCTYPE, chunk_size=CHUNKSIZE
    ):
        if not ok:
            action, result = result.popitem()
            doc_id = '/%s/commits/%s' % (args.index, result['_id'])
            print('Failed to %s document %s: %r' % (action, doc_id, result))
    return


def mongodb_index_idmappings(mdbi, csvfile, reader):
    entries = list()
    for entry in reader(csvfile):
        entries.append(entry)
        if len(entries) == CHUNKSIZE:
            mdbi[DOCTYPE].insert_many(entries)
            entries.clear()
    mdbi[DOCTYPE].insert_many(entries)
    return


def main(dbc, infile, index):
    if dbc.db == "Elasticsearch":
        es_index_idmappings(dbc.es, infile, mappingreader)
        dbc.es.indices.refresh(index=index)
    else:  # "MongoDB"
        mongodb_index_idmappings(dbc.mdbi, infile, mappingreader)


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index RNAcentral id mappings with Elasticsearch')
    parser.add_argument('--infile',
                        default=d+"/../tests/data/"
                                  "rnacentral-v7-id_mapping-first100.tsv",
                        # required=True,
                        help='Input file to index')
    parser.add_argument('--index',
                        default="rnacentral",
                        help='Name of the Elasticsearch index')
    parser.add_argument('--host',
                        help='Elasticsearch server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch server port")
    parser.add_argument('--db', default='MongoDB',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()
    dbc_ = DBconnection(args.db, args.index, host=args.host,
                        port=args.port)
    main(dbc_, args.infile, args.index)
