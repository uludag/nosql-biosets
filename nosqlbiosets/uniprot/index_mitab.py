#!/usr/bin/env python
""" Index PSI-MI TAB interaction data files with Elasticsearch or MongoDB"""
from __future__ import print_function

import argparse
import csv
import os
import time

from elasticsearch.helpers import streaming_bulk

from nosqlbiosets.dbutils import DBconnection

ES_CHUNK_SIZE = 2048  # for Elasticsearch index requests
DOCTYPE = "interaction"


def updatemitabrecord(row, i):
    for a in ["conf"]:
        if len(row[a]) > 0 and row[a] != 'null':
            row[a] = float(row[a])
        else:
            del row[a]
    del row[None]
    row['_id'] = i
    return row


def read_mitab_datafile(infile, lineparser):
    i = 0
    fieldnames = ['idA', 'idB', 'idsA', 'idsB',
                  'aliasA', 'aliasB', 'detmethod',
                  'pubauth', 'pubid', 'taxidA', 'taxidB',
                  'type', 'source', 'interaction_id', 'conf']
    with open(infile) as csvfile:
        reader = csv.DictReader(csvfile, fieldnames,
                                delimiter='\t', quotechar='"')
        for row in reader:
            i += 1
            if i == 1:
                continue
            r = lineparser(row, i-1)
            yield r


def es_index(dbc, doctype, reader):
    print("Reading from %s" % reader.gi_frame.f_locals['infile'])
    i = 0
    t1 = time.time()
    for ok, result in streaming_bulk(
            dbc.es,
            reader,
            index=dbc.index,
            doc_type=doctype,
            chunk_size=ES_CHUNK_SIZE
    ):
        action, result = result.popitem()
        i += 1
        doc_id = '/%s/commits/%s' % (dbc.index, result['_id'])
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
    for entry in read_mitab_datafile(infile, reader):
        mdbc.insert(entry)
        i += 1
    t2 = time.time()
    print("-- Processed %d entries, in %d sec"
          % (i, (t2 - t1)))
    return 1


def main(infile, index, doctype, db, host=None, port=None):
    dbc = DBconnection(db, index, host, port)
    if db == 'Elasticsearch':
        es_index(dbc, doctype, read_mitab_datafile(infile, updatemitabrecord))
        dbc.es.indices.refresh(index=index)
    else:  # assume MongoDB
        dbc.mdbi.drop_collection(doctype)
        mongodb_index(dbc.mdbi[doctype], infile, updatemitabrecord)
        indx_fields = ["idA", "idB", "idsA", "idsB"]
        for field in indx_fields:
            dbc.mdbi[doctype].create_index(field)


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index PSI-MI-TAB files with'
                    ' MongoDB/Elasticsearch')
    parser.add_argument('--infile',
                        help='   tsv file')
    parser.add_argument('--doctype', default=DOCTYPE,
                        help='Document type name for Elasticsearch, '
                             'collection name for MongoDB')
    parser.add_argument('--index', default="mitab",
                        help='Name of the Elasticsearch index or '
                             'MongoDB database')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or MongoDB server port")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()

    main(args.infile, args.index, args.doctype, args.db, args.host, args.port)
