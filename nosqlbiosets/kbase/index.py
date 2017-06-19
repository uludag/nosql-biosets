#!/usr/bin/env python
""" Index Kbase compounds/reactions data files with Elasticsearch """
# ftp://ftp.kbase.us/assets/KBase_Reference_Data/Biochemistry/
from __future__ import print_function

import argparse
import csv
import os
import time

from elasticsearch.helpers import streaming_bulk

from nosqlbiosets.dbutils import DBconnection

chunksize = 2048


def getnumericval(val):
    nval = float(val) if len(val) > 0 else None
    return nval


# Parse records in Kbase compounds csv file which has the following header
# DATABASE,PRIMARY NAME,ABBREVIATION,NAMES,KEGG ID(S),FORMULA,CHARGE,
# DELTAG (kcal/mol),DELTAG ERROR (kcal/mol),MASS
def getcompoundrecord(row, _):
    r = {
        '_id':  row[0], 'name': row[1],
        'abbrv': row[2], 'synonym':  row[3].split('|'),
        'keggid': row[4].split('|'), 'formula':    row[5],
        'charge': getnumericval(row[6]), 'deltaG': getnumericval(row[7]),
        'deltaGerr': getnumericval(row[8]), 'mass': getnumericval(row[9]),
        '_type': 'compound'
    }
    return r


# Parse records in Kbase reactions csv file which has the following header
# DATABASE,NAME,EC NUMBER(S),KEGG ID(S),DELTAG (kcal/mol),
# DELTAG ERROR (kcal/mol),EQUATION,NAME EQ,THERMODYNAMIC FEASIBILTY
def getreactionrecord(row, _):
    ec = row[2].split('|')[1:-1]
    r = {
        '_id':  row[0], 'name': row[1], 'ec': ec,
        'keggid':  row[3].split('|') if len(row[3]) > 0 else [],
        'deltaG': getnumericval(row[4]), 'deltaGerr': getnumericval(row[5]),
        'equation_id': row[6], 'equation_name':    row[7],
        'feasibility': row[8],  '_type': 'reaction'
    }
    return r


def read_kbase_data(infile, lineparser):
    i = 0
    with open(infile) as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in reader:
            i += 1
            if i == 1:
                continue
            r = lineparser(row, i)
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


def mongodb_index(mdbc, infile, reader):
    print("Reading from %s" % infile)
    i = 0
    t1 = time.time()
    for entry in read_kbase_data(infile, reader):
        del(entry['_type'])
        mdbc.insert(entry)
    t2 = time.time()
    print("-- Processed %d entries, in %d sec"
          % (i, (t2 - t1)))
    return 1


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index KBase compounds/reactions files with'
                    ' MongoDB/Elasticsearch')
    parser.add_argument('--compoundsfile',
                        default=d + "/../../data/kbase/compounds.csv",
                        help='Kbase compounds csv file')
    parser.add_argument('--reactionsfile',
                        default=d + "/../../data/kbase/reactions.csv",
                        help='Kbase reactions csv file')
    parser.add_argument('--index', default="biosets",
                        help='Name of the Elasticsearch index')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or MongoDB server port")
    parser.add_argument('--db', default='Elasticsearch_',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()

    indxr = DBconnection(args.db, args.index, args.host, args.port)

    if args.db == 'Elasticsearch':
        es = indxr.es
        if es.indices.exists(index=args.index):  # TODO: check with user
            es.indices.delete(index=args.index, params={"timeout": "10s"})
        es.indices.create(index=args.index, params={"timeout": "10s"},
                          body={"settings": {"number_of_replicas": 0}})
        es_index(es, read_kbase_data(args.compoundsfile, getcompoundrecord))
        es_index(es, read_kbase_data(args.reactionsfile, getreactionrecord))
        es.indices.refresh(index=args.index)
    else:  # assume MongoDB
        doctype = 'compound'
        indxr.mdbi.drop_collection(doctype)
        mongodb_index(indxr.mdbi[doctype],
                      args.compoundsfile, getcompoundrecord)
        doctype = 'reaction'
        indxr.mdbi.drop_collection(doctype)
        mongodb_index(indxr.mdbi[doctype],
                      args.reactionsfile, getreactionrecord)
