#!/usr/bin/env python
from __future__ import print_function

import argparse
import csv
import json
import time

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

chunksize = 2048


# Parse records in chem_prop.tsv file which has the following header
# #MNX_ID	Description	Formula	Charge	Mass	InChi	SMILES	Source
def getcompoundrecord(row, _):
    sourcelib = None
    sourceid = None
    j = row[7].find(':')
    if j > 0:
        sourcelib = row[7][0:j]
        sourceid = row[7][j + 1:]
    r = {
        '_id':     row[0], 'desc':   row[1],
        'formula': row[2], 'charge': row[3],
        'mass':    row[4], 'inchi':  row[5],
        'smiles':  row[6], '_type':  'compound',
        'source': {'lib': sourcelib, 'id': sourceid}
    }
    return r


# Parse records in chem_xref.tsv file which has the following header
# #XREF   MNX_ID  Evidence        Description
def getcompoundxrefrecord(row, i):
    sourcelib = None
    sourceid = None
    j = row[0].find(':')
    if j > 0:
        sourcelib = row[0][0:j]
        sourceid = row[0][j + 1:]
    r = {
        '_id':      i,
        'metanetxid': row[1], 'evidence': row[2],
        'desc':       row[3], '_type':    "compoundxref",
        'xref': {'lib': sourcelib, 'id': sourceid}
    }
    return r


# Parse records in reac_xref.tsv file which has the following header
# #XREF   MNX_ID
def getreactionxrefrecord(row):
    reflib = None
    refid = None
    j = row[0].find(':')
    if j > 0:
        reflib = row[0][0:j]
        refid = row[0][j + 1:]
    metanetxid = row[1]
    return metanetxid, [reflib, refid]


# Get reaction xrefs in a map
def getreactionxrefs(infile):
    map = dict()
    with open(infile) as csvfile:
        reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
        for row in reader:
            if row[0][0] == '#':
                continue
            key, val = getreactionxrefrecord(row)
            if key not in map:
                map[key] = []
            map[key].append(val)
    return map


# Parse records in react_prop.tsv file which has the following header
# #MNX_ID Equation        Description     Balance EC      Source
def getreactionrecord(row, _):
    sourcelib = None
    sourceid = None
    j = row[5].find(':')
    if j > 0:
        sourcelib = row[5][0:j]
        sourceid = row[5][j + 1:]
    r = {
        '_id':  row[0], 'equation': row[1],
        'desc': row[2], 'balance':  row[3],
        'ecno': row[4], '_type':    "reaction",
        'source': {'lib': sourcelib, 'id': sourceid},
        'xrefs': xrefsmap[row[0]]
    }
    return r


def read_metanetx_mappings(infile, metanetxparser):
    i = 0
    with open(infile) as csvfile:
        reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
        for row in reader:
            if row[0][0] == '#':
                continue
            i += 1
            r = metanetxparser(row, i)
            yield r


def es_index(escon, reader):
    # TODO: recommended/safer way to read 'infile'
    print("Reading from %s" % reader.gi_frame.f_locals['infile'])
    i = 0
    t1 = time.time()
    for ok, result in streaming_bulk(
            escon,
            reader,
            index=args.index,
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
    parser.add_argument('--compoundsfile',
                        default="./data/chem_prop.tsv",
                        help='Metanetx chem_prop.tsv file')
    parser.add_argument('--compoundsxreffile',
                        default="./data/chem_xref.tsv",
                        help='Metanetx chem_xref.tsv file')
    parser.add_argument('--reactionsfile',
                        default="./data/reac_prop.tsv",
                        help='Metanetx reac_prop.tsv file')
    parser.add_argument('--reactionsxreffile',
                        default="./data/reac_xref.tsv",
                        help='Metanetx reac_xref.tsv file')
    parser.add_argument('--index',
                        default="metanetx",
                        help='name of the Elasticsearch index')
    parser.add_argument('--host', default=conf['host'],
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default=conf['port'],
                        help="Elasticsearch server port")
    args = parser.parse_args()
    host = args.host
    port = args.port
    es = Elasticsearch(host=host, port=port, timeout=3600)
    if es.indices.exists(index=args.index):
        es.indices.delete(index=args.index, params={"timeout": "10s"})
    es.indices.create(index=args.index, params={"timeout": "10s"},
                      ignore=400)
    es_index(es, read_metanetx_mappings(args.compoundsfile, getcompoundrecord))
    es_index(es, read_metanetx_mappings(args.compoundsxreffile,
                                        getcompoundxrefrecord))
    xrefsmap = getreactionxrefs(args.reactionsxreffile)
    es_index(es, read_metanetx_mappings(args.reactionsfile, getreactionrecord))

    es.indices.refresh(index=args.index)
