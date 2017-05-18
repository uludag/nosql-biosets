#!/usr/bin/env python
""" Index MetaNetX compound/reaction files with Elasticsearch """
from __future__ import print_function

import argparse
import csv
import json
import os
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
        'source': {'lib': sourcelib, 'id': sourceid},
        'xrefs': xrefsmap[row[0]]
    }
    return r


# Parse records in chem_xref.tsv file which has the following header
# #XREF   MNX_ID  Evidence        Description
def getcompoundxrefrecord(row):
    sourcelib = None
    sourceid = None
    j = row[0].find(':')
    if j > 0:
        sourcelib = row[0][0:j]
        sourceid = row[0][j + 1:]
    metanetxid = row[1]
    return metanetxid, [sourcelib, sourceid, row[2], row[3]]


# Collect compound xrefs in a map
def getcompoundxrefs(infile):
    cxrefs = dict()
    with open(infile) as csvfile:
        reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
        for row in reader:
            if row[0][0] == '#':
                continue
            key, val = getcompoundxrefrecord(row)
            if key not in cxrefs:
                cxrefs[key] = []
            cxrefs[key].append(val)
    return cxrefs


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


# Collect reaction xrefs in a map
def getreactionxrefs(infile):
    rxrefs = dict()
    with open(infile) as csvfile:
        reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
        for row in reader:
            if row[0][0] == '#':
                continue
            key, val = getreactionxrefrecord(row)
            if key not in rxrefs:
                rxrefs[key] = []
            rxrefs[key].append(val)
    return rxrefs


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
    print("Reading from %s" % reader.gi_frame.f_locals['infile'])
    i = 0
    t1 = time.time()
    for ok, result in streaming_bulk(
            escon,
            reader,
            index=args.index,
            chunk_size=chunksize
    ):
        action, result = result.popitem()
        i += 1
        doc_id = '/%s/commits/%s' % (args.index, result['_id'])
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
    t2 = time.time()
    print("-- Processed %d entries, in %d sec"
          % (i, (t2 - t1)))

    return 1


if __name__ == '__main__':
    conf = {"host": "localhost", "port": 9200}
    d = os.path.dirname(os.path.abspath(__file__))
    try:
        conf = json.load(open(d + "/../conf/elasticsearch.json", "r"))
    finally:
        pass
    parser = argparse.ArgumentParser(
        description='Index MetaNetX compound/reaction files with Elasticsearch')
    parser.add_argument('--compoundsfile',
                        default=d + "/data/chem_prop.tsv",
                        help='Metanetx chem_prop.tsv file')
    parser.add_argument('--compoundsxreffile',
                        default=d + "/data/chem_xref.tsv",
                        help='Metanetx chem_xref.tsv file')
    parser.add_argument('--reactionsfile',
                        default=d + "/data/reac_prop.tsv",
                        help='Metanetx reac_prop.tsv file')
    parser.add_argument('--reactionsxreffile',
                        default=d + "/data/reac_xref.tsv",
                        help='Metanetx reac_xref.tsv file')
    parser.add_argument('--index',
                        default="metanetx-0.2",
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
                      body={"settings": {"number_of_replicas": 0}})

    xrefsmap = getcompoundxrefs(args.compoundsxreffile)
    es_index(es, read_metanetx_mappings(args.compoundsfile, getcompoundrecord))
    xrefsmap = getreactionxrefs(args.reactionsxreffile)
    es_index(es, read_metanetx_mappings(args.reactionsfile, getreactionrecord))

    es.indices.refresh(index=args.index)
