#!/usr/bin/env python
""" Index ModelSEEDDatabase compounds/reactions with Elasticsearch or MongoDB"""
# https://github.com/ModelSEED/ModelSEEDDatabase/blob/dev/Biochemistry
from __future__ import print_function

import argparse
import csv
import time

from elasticsearch.helpers import streaming_bulk

from nosqlbiosets.dbutils import DBconnection
from pymongo import IndexModel

ES_CHUNK_SIZE = 2048  # for Elasticsearch index requests
TYPE_COMPOUND = 'modelseed_compound'
TYPE_REACTION = 'modelseed_reaction'


def delete_attrs_with_value_null(r):
    for k, v in list(r.items()):
        if v == 'null':
            del r[k]


# Parse records in ModelSEED DB compounds tsv file which has the
# following columns:
# id, abbreviation, name, formula, mass, source, inchikey, structure, charge,
# is_core, is_obsolete, linked_compound, is_cofactor, deltag, deltagerr,
# pka, pkb, abstract_compound, comprised_of	aliases
def updatecompoundrecord(row, _):
    for a in ['charge', 'deltag', 'deltagerr', 'mass']:
        if len(row[a]) > 0 and row[a] != 'null' and row[a] != 'None':
            row[a] = float(row[a])
        else:
            del row[a]
    for a in ['is_cofactor', 'is_core', 'is_obsolete']:
        row[a] = True if row[a] == '1' else False
    row['_id'] = row['id']
    del row['id'], row['source']
    row['_type'] = '_doc'
    delete_attrs_with_value_null(row)
    return row


# Parse records in ModelSEED DB reactions tsv file which has the
# following columns:
# id, abbreviation, name, code, stoichiometry, is_transport, equation,
# definition, reversibility, direction, abstract_reaction, pathways,
# aliases ec_numbers, deltag, deltagerr, compound_ids, status,
# is_obsolete, linked_reaction
def updatereactionrecord(row, _):
    for a in ['deltag', 'deltagerr']:
        if len(row[a]) > 0 and row[a] != 'null':
            row[a] = float(row[a])
        else:
            del row[a]
    for a in ['is_transport', 'is_obsolete']:
        row[a] = True if row[a] == '1' else False
    row['_id'] = row['id']
    del row['id']
    row['_type'] = '_doc'
    delete_attrs_with_value_null(row)
    return row


def read_modelseed_datafile(infile, lineparser):
    i = 0
    print("Reading from %s" % infile)
    with open(infile) as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t', quotechar='"')
        for row in reader:
            i += 1
            r = lineparser(row, i)
            yield r


def es_index(dbc, infile, typetuner):
    i = 0
    t1 = time.time()
    for ok, result in streaming_bulk(
            dbc.es,
            read_modelseed_datafile(infile, typetuner),
            index=dbc.index,
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


def mongodb_indices(mdb):
    if mdb.name == TYPE_COMPOUND:
        index = IndexModel([
            ("name", "text"),
            ("aliases", "text"),
            ("abbreviation", "text")])
        mdb.create_indexes([index])
        indx_fields = ["mass", "deltag", "deltagerr", "charge",
                       "name", 'abbreviation', "inchikey"]
        for field in indx_fields:
            mdb.create_index(field)
    else:
        index = IndexModel([
            ("name", "text"),
            ("abbreviation", "text"),
            ("definition", "text")])
        mdb.create_indexes([index])


def mongodb_index(mdbc, infile, typetuner):
    i = 0
    t1 = time.time()
    for entry in read_modelseed_datafile(infile, typetuner):
        del(entry['_type'])
        mdbc.insert_one(entry)
        i += 1
    t2 = time.time()
    print("-- Processed %d entries, in %d sec"
          % (i, (t2 - t1)))
    return 1


def main(infile, index, doctype, db, host=None, port=None):
    dbc = DBconnection(db, index, host, port, recreateindex=True)
    if doctype == TYPE_REACTION:
        typetuner = updatereactionrecord
    else:
        typetuner = updatecompoundrecord
    if db == 'Elasticsearch':
        es_index(dbc, infile, typetuner)
        dbc.es.indices.refresh(index=index)
    else:  # assume MongoDB
        dbc.mdbi.drop_collection(doctype)
        mongodb_index(dbc.mdbi[doctype], infile, typetuner)
        mongodb_indices(dbc.mdbi[doctype])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index ModelSEEDDatabase compounds/reactions files with'
                    ' MongoDB/Elasticsearch')
    parser.add_argument('--compoundsfile',
                        help='ModelSEEDDatabase compounds tsv file')
    parser.add_argument('--reactionsfile',
                        help='ModelSEEDDatabase reactions tsv file')
    parser.add_argument('--index', default="biosets",
                        help='Name of the Elasticsearch index or '
                             'MongoDB database')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or MongoDB server port")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()

    if args.compoundsfile is not None:
        main(args.compoundsfile, args.index, TYPE_COMPOUND,
             args.db, args.host, args.port)
    if args.reactionsfile is not None:
        main(args.reactionsfile, args.index, TYPE_REACTION,
             args.db, args.host, args.port)
