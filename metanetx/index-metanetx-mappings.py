#!/usr/bin/env python
from __future__ import print_function

import argparse
import csv
import json
import time

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

chunksize = 2048


# Index chem_prop.tsv which has the following header
# #MNX_ID	Description	Formula	Charge	Mass	InChi	SMILES	Source
def read_metanetx_mappings(infile):
    with open(infile, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
        for row in reader:
            if row[0][0] == '#':
                continue
            sourcelib = None; sourceid = None
            j = row[7].find(':')
            if j > 0:
                sourcelib = row[7][0:j]; sourceid = row[7][j+1:]
            r = {
                '_id': row[0], 'desc': row[1],
                'formula': row[2], 'charge': row[3],
                'mass': row[4], 'inchi': row[5],
                'smiles': row[6],
                'source': {'lib': sourcelib, 'id': sourceid}
            }
            yield r


def es_index(escon, reader, doctype):
    print("Reading %s " % doctype)
    i = 0
    t1 = time.time()
    for ok, result in streaming_bulk(
            escon,
            reader,
            index=args.index,
            doc_type=doctype,
            chunk_size=chunksize
    ):
        action, result = result.popitem(); i += 1
        doc_id = '/%s/commits/%s' % (args.index, result['_id'])
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
    t2 = time.time()
    print("-- Processed %d entries, in %d sec"
          % (i, (t2 - t1)))

    return 1


if __name__ == '__main__':
    conf = {"host": "localhost", "port": 9200}
    try:
        conf = json.load(open("../conf/elasticsearch.json", "rt"))
    finally:
        pass
    parser = argparse.ArgumentParser(
        description='Index Metanetx compound/reaction files with Elasticsearch')
    parser.add_argument('--infile',
                        default="./data/chem_prop.tsv",
                        help='input file to index')
    parser.add_argument('--index',
                        default="metanet-compound",
                        help='name of the elasticsearch index')
    parser.add_argument('--host', default=conf['host'],
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default=conf['port'],
                        help="Elasticsearch server port")
    args = parser.parse_args()
    host = args.host
    port = args.port
    es = Elasticsearch(host=host, port=port, timeout=3600)
    es.indices.delete(index=args.index, params={"timeout": "10s"})
    es.indices.create(index=args.index, params={"timeout": "10s"},
                      ignore=400)
    es_index(es, read_metanetx_mappings(args.infile), "metanet-compound")
    es.indices.refresh(index=args.index)
