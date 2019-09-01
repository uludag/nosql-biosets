#!/usr/bin/env python
""" Index MetaNetX compounds, reactions, and compartment files
    with Elasticsearch or MongoDB (tested with version 3.2) """
from __future__ import print_function

import argparse
import csv
import os
import time
from pymongo import IndexModel

from elasticsearch.helpers import streaming_bulk

from nosqlbiosets.dbutils import DBconnection

ES_CHUNK_SIZE = 256  # for Elasticsearch index requests
TYPE_COMPOUND = 'metanetx_compound'
TYPE_REACTION = 'metanetx_reaction'
TYPE_COMPARTMENT = 'metanetx_compartment'


# Parse records in MetaNetX chem_prop.tsv file which has the following header
# #MNX_ID  Description  Formula  Charge  Mass  InChi  SMILES  Source  InChIKey
def getcompoundrecord(row, xrefsmap):
    sourcelib = None
    sourceid = None
    id_ = row[0]
    j = row[7].find(':')
    if j > 0:
        sourcelib = row[7][0:j]
        sourceid = row[7][j + 1:]
    charge = int(row[3]) if len(row[3]) > 0 and row[3] != "NA" else None
    mass = float(row[4]) if len(row[4]) > 0 else None
    r = {
        '_id':     id_,     'desc':   row[1],
        'formula': row[2],  'charge': charge,
        'mass':    mass,    'inchi':  row[5],
        'smiles':  row[6],
        'source': {'lib': sourcelib, 'id': sourceid},
        'inchikey': row[8],
        'xrefs': xrefsmap[id_] if id_ in xrefsmap else None
    }
    return r


# Parse records in MetaNetX chem_xref.tsv file which has the following header
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
    return metanetxid, {"lib": reflib, "id": refid,
                        "evidence": row[2], "desc": row[3]}


def _mergecompoundxrefs(xrefs):
    r = dict()
    for c in xrefs:
        key = c['lib']+c['desc']+c['evidence']
        if key not in r:
            r[key] = {"lib": c['lib'], "id": [c['id']],
                      "evidence": c['evidence'], "desc": c['desc']}
        else:
            r[key]['id'].append(c['id'])
    return list(r.values())


# Parse MetaNetX compo_prop.tsv file which has the following header
# MNX_ID, Description, Source
def getcompartmentrecord(row, xrefsmap):
    id_ = row[0]
    j = row[2].find(':')
    if j > 0:
        sourcelib = row[2][0:j]
        sourceid = row[2][j + 1:]
    else:
        sourcelib = "MetaNetX"
        sourceid = row[2]
    r = {
        '_id':     id_,     'desc':   row[1],
        'source': {'lib': sourcelib, 'id': sourceid},
        'xrefs': xrefsmap[id_] if id_ in xrefsmap else None
    }
    return r


# Parse records in MetaNetX compo_xref.tsv file which has the following header
# XREF, MNX_ID, Description
def getcompartmentxrefrecord(row):
    j = row[0].find(':')
    if j > 0:
        reflib = row[0][0:j]
        refid = row[0][j + 1:]
    else:
        reflib = 'MetanetX'
        refid = row[1]
    metanetxid = row[1]
    return metanetxid, {"lib": reflib, "id": refid, "desc": row[2]}


# Collect xrefs in a dictionary
def getxrefs(infile, xrefparser):
    print("Collecting xrefs '%s' in a dictionary" % infile)
    cxrefs = dict()
    with open(infile) as csvfile:
        reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
        for row in reader:
            if row[0][0] == '#':
                continue
            key, val = xrefparser(row)
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
    return metanetxid, {"lib": reflib, "id": refid}


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
        'ecno': row[4].split(";"),
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
        if db == "Elasticsearch":
            index = doctype
        super(Indexer, self).__init__(db, index, host, port,
                                      recreateindex=True)
        if db == "MongoDB":
            self.mdbi.drop_collection(doctype)
            self.mcl = self.mdbi[doctype]

    def indexall(self, reader):
        print("Reading/indexing %s" % reader.gi_frame.f_locals['infile'])
        t1 = time.time()
        if self.db == "Elasticsearch":
            i = self.es_index(reader)
        else:
            i = self.mongodb_index(reader)
            if reader != getcompartmentrecord:
                collection = self.doctype
                self.mongodb_indices(collection)

        t2 = time.time()
        print("-- Processed %d entries, in %d sec"
              % (i, (t2 - t1)))

    def es_index(self, reader):
        i = 0
        for ok, result in streaming_bulk(self.es, reader, index=self.index,
                                         doc_type="_doc",
                                         chunk_size=ES_CHUNK_SIZE):
            action, result = result.popitem()
            i += 1
            doc_id = '/%s/commits/%s' % (self.index, result['_id'])
            if not ok:
                print('Failed to %s document %s: %r' % (action, doc_id, result))
            self.reportprogress()
        return i

    def mongodb_index(self, reader):
        i = 0
        for r in reader:
            try:
                self.mcl.insert_one(r)
                i += 1
                self.reportprogress()
            except Exception as e:
                print(e)
        return i

    def mongodb_indices(self, collection):
        index = IndexModel([
            ("desc", "text"),
            ("xrefs.desc" if collection == TYPE_COMPOUND else "xrefs.id",
             "text")
        ])
        self.mdbi[collection].drop_indexes()
        self.mdbi[collection].create_indexes([index])
        indx_fields = ["xrefs.id"]
        if collection == TYPE_REACTION:
            indx_fields += ["ecno"]
        for field in indx_fields:
            self.mdbi[collection].create_index(field)


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index MetaNetX compounds/reactions files'
                    ' with Elasticsearch or MongoDB')
    parser.add_argument('--metanetxdatafolder',
                        default=d + "/data/",
                        help='Name of the folder where'
                             ' all MetaNetX data files were downloaded')
    parser.add_argument('--compoundsfile',
                        help='MetaNetX chem_prop.tsv file')
    parser.add_argument('--compoundsxreffile',
                        help='MetaNetX chem_xref.tsv file')
    parser.add_argument('--compartmentsfile',
                        help='MetaNetX comp_prop.tsv file')
    parser.add_argument('--compartmentsxreffile',
                        help='MetaNetX comp_xref.tsv file')
    parser.add_argument('--reactionsfile',
                        help='MetaNetX reac_prop.tsv file')
    parser.add_argument('--reactionsxreffile',
                        help='MetaNetX reac_xref.tsv file')
    parser.add_argument('--index', default="biosets",
                        help='Name of the MongoDB database'
                             'Elasticsearch index name are predefined'
                             ' type names; metanetx_compound, metanetx_reaction'
                             ' and metanetx_compartment')
    parser.add_argument('--host',
                        help='Elasticsearch/MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch/MongoDB server port number")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: Elasticsearch or MongoDB")
    args = parser.parse_args()

    files = [("compoundsfile", "chem_prop.tsv"),
             ("compoundsxreffile", "chem_xref.tsv"),
             ("compartmentsfile", "comp_prop.tsv"),
             ("compartmentsxreffile", "comp_xref.tsv"),
             ("reactionsfile", "reac_prop.tsv"),
             ("reactionsxreffile", "reac_xref.tsv")
             ]
    v = vars(args)
    for arg, filename in files:
        if v[arg] is None:
            v[arg] = os.path.join(args.metanetxdatafolder, filename)

    xrefsmap_ = getxrefs(args.compoundsxreffile, getcompoundxrefrecord)
    for refs in xrefsmap_:
        xrefsmap_[refs] = _mergecompoundxrefs(xrefsmap_[refs])
    indxr = Indexer(args.db, args.index, args.host, args.port, TYPE_COMPOUND)
    indxr.indexall(read_metanetx_mappings(args.compoundsfile,
                                          getcompoundrecord, xrefsmap_))

    xrefsmap_ = getxrefs(args.compartmentsxreffile, getcompartmentxrefrecord)
    indxr = Indexer(args.db, args.index, args.host, args.port, TYPE_COMPARTMENT)
    indxr.indexall(read_metanetx_mappings(args.compartmentsfile,
                                          getcompartmentrecord, xrefsmap_))

    xrefsmap_ = getxrefs(args.reactionsxreffile, getreactionxrefrecord)
    indxr = Indexer(args.db, args.index, args.host, args.port, TYPE_REACTION)
    indxr.indexall(read_metanetx_mappings(args.reactionsfile,
                                          getreactionrecord, xrefsmap_))
    indxr.close()
