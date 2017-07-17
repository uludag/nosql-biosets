#!/usr/bin/env python
""" Index MetaNetX compound/reaction files (version 3.0) with Elasticsearch
 or MongoDB"""
from __future__ import print_function

import argparse
import csv
import os
import sys
import time

from elasticsearch.helpers import streaming_bulk

from nosqlbiosets.dbutils import DBconnection

chunksize = 2048  # for Elasticsearch index requests


# Parse records in MetanetX chem_prop.tsv file which has the following header
# #MNX_ID  Description  Formula  Charge  Mass  InChi  SMILES  Source  InChIKey
def getcompoundrecord(row, xrefsmap):
    sourcelib = None
    sourceid = None
    id_ = row[0]
    j = row[7].find(':')
    if j > 0:
        sourcelib = row[7][0:j]
        sourceid = row[7][j + 1:]
    r = {
        '_id':       id_,    'desc':   row[1],
        'formula': row[2], 'charge': row[3],
        'mass':    row[4], 'inchi':  row[5],
        'smiles':  row[6], '_type':  'compound',
        'source': {'lib': sourcelib, 'id': sourceid},
        'inchikey': row[8],
        'xrefs': xrefsmap[id_] if id_ in xrefsmap else None
    }
    return r


# Parse records in MetanetX chem_xref.tsv file which has the following header
# #XREF   MNX_ID  Evidence        Description
def getcompoundxrefrecord(row):
    j = row[0].find(':')
    if j > 0:
        reflib = row[0][0:j]
        refid = row[0][j + 1:]
    else:
        reflib = 'MetanetX'
        refid = row[1]
    metanetxid = row[1]
    return metanetxid, [reflib, refid, row[2], row[3]]


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
    j = row[0].find(':')
    if j > 0:
        reflib = row[0][0:j]
        refid = row[0][j + 1:]
    else:
        reflib = 'MetanetX'
        refid = row[1]
    metanetxid = row[1]
    return metanetxid, [reflib, refid]


# Collect reaction xrefs into a dictionary
def getreactionxrefs(infile):
    rxrefs = dict()
    with open(infile) as csvfile:
        reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
        for row in reader:
            if row[0][0] == '#' or row[0] == row[1]:
                continue
            # Should we skip the xrefs to the source library?
            key, val = getreactionxrefrecord(row)
            if key not in rxrefs:
                rxrefs[key] = []
            rxrefs[key].append(val)
    return rxrefs


# Parse records in react_prop.tsv file which has the following header
# #MNX_ID  Equation  Description  Balance  EC  Source
def getreactionrecord(row, xrefsmap):
    sourcelib = None
    sourceid = None
    id_ = row[0]
    j = row[5].find(':')
    if j > 0:
        sourcelib = row[5][0:j]
        sourceid = row[5][j + 1:]
    r = {
        '_id':  id_, 'equation': row[1],
        'desc': row[2], 'balance':  row[3],
        'ecno': row[4], '_type':    "reaction",
        'source': {'lib': sourcelib, 'id': sourceid},
        'xrefs': xrefsmap[id_] if id_ in xrefsmap else None
    }
    return r


def read_metanetx_mappings(infile, metanetxparser, xrefsmap):
    with open(infile) as csvfile:
        reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
        for row in reader:
            if row[0][0] == '#':
                continue
            r = metanetxparser(row, xrefsmap)
            yield r


class Indexer(DBconnection):

    def __init__(self, db, index, host, port, doctype):
        self.doctype = doctype
        super(Indexer, self).__init__(db, index, host, port,
                                      recreateindex=False)
        if db != "Elasticsearch":
            self.mcl = self.mdbi[doctype]

    def indexall(self, reader):
        print("Reading from %s" % reader.gi_frame.f_locals['infile'])
        t1 = time.time()
        if self.db == "Elasticsearch":
            i = self.es_index(reader)
        else:
            i = self.mongodb_index(reader)
        t2 = time.time()
        print("-- Processed %d entries, in %d sec"
              % (i, (t2 - t1)))

    def es_index(self, reader):
        i = 0
        for ok, result in streaming_bulk(self.es, reader, index=self.index,
                                         chunk_size=chunksize):
            action, result = result.popitem()
            i += 1
            doc_id = '/%s/commits/%s' % (self.index, result['_id'])
            if not ok:
                print('Failed to %s document %s: %r' % (action, doc_id, result))
        return i

    def mongodb_index(self, reader):
        i = 0
        for r in reader:
            print(".", end='')
            sys.stdout.flush()
            docid = r['_id']
            spec = {"_id": docid}
            try:
                self.mcl.update(spec, r, upsert=True)
                i += 1
            except Exception as e:
                print(e)
        return i


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index MetaNetX compound/reaction files with Elasticsearch')
    parser.add_argument('--compoundsfile',
                        default=d + "/data/chem_prop.tsv",
                        help='MetaNetX chem_prop.tsv file')
    parser.add_argument('--compoundsxreffile',
                        default=d + "/data/chem_xref.tsv",
                        help='MetaNetX chem_xref.tsv file')
    parser.add_argument('--reactionsfile',
                        default=d + "/data/reac_prop.tsv",
                        help='MetaNetX reac_prop.tsv file')
    parser.add_argument('--reactionsxreffile',
                        default=d + "/data/reac_xref.tsv",
                        help='MetaNetX reac_xref.tsv file')
    parser.add_argument('--index', default="nosqlbiosets",
                        help='Name of the Elasticsearch index'
                             ' or MongoDB database')
    parser.add_argument('--host',
                        help='Elasticsearch server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch server port")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: Elasticsearch or MongoDB")
    args = parser.parse_args()

    xrefsmap_ = getcompoundxrefs(args.compoundsxreffile)
    indxr = Indexer(args.db, args.index, args.host, args.port, "compound")
    indxr.indexall(read_metanetx_mappings(args.compoundsfile,
                                          getcompoundrecord, xrefsmap_))

    xrefsmap_ = getreactionxrefs(args.reactionsxreffile)
    indxr = Indexer(args.db, args.index, args.host, args.port, "reaction")
    indxr.indexall(read_metanetx_mappings(args.reactionsfile,
                                          getreactionrecord, xrefsmap_))
    indxr.close()