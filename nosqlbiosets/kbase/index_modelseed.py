#!/usr/bin/env python
""" Index ModelSEED compounds/reactions data with Elasticsearch or MongoDB"""
# https://github.com/ModelSEED/ModelSEEDDatabase/blob/master/Biochemistry
# TODO: move to .json files, we currently index the .csv files
from __future__ import print_function

import argparse
import csv
import os
import time

from elasticsearch.helpers import streaming_bulk

from nosqlbiosets.dbutils import DBconnection

ES_CHUNK_SIZE = 2048  # for Elasticsearch index requests
TYPE_COMPOUND = 'modelseed_compound'
TYPE_REACTION = 'modelseed_reaction'


# Parse records in ModelSEED DB compounds tsv file which has the
# following columns:
# id, abbreviation, name, formula, mass, source, structure, charge,
# is_core, is_obsolete, linked_compound, is_cofactor, deltag, deltagerr,
# pka, pkb, abstract_compound, comprised_of	aliases
def updatecompoundrecord(row, _):
    for a in ['charge', 'deltag', 'deltagerr', 'mass']:
        if len(row[a]) > 0 and row[a] != 'null':
            row[a] = float(row[a])
        else:
            del row[a]
    for a in ['is_cofactor', 'is_core', 'is_obsolete']:
        row[a] = True if row[a] == '1' else False
    row['_id'] = row['id']
    del row['id'], row['source']
    row['_type'] = TYPE_COMPOUND
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
    row['_type'] = TYPE_REACTION
    return row


def read_modelseed_datafile(infile, lineparser):
    i = 0
    print(infile)
    with open(infile) as csvfile:
        reader = csv.DictReader(csvfile, delimiter='\t', quotechar='"')
        for row in reader:
            i += 1
            r = lineparser(row, i)
            yield r


def es_index(escon, typetuner):
    print("Reading from %s" % typetuner.gi_frame.f_locals['infile'])
    i = 0
    t1 = time.time()
    for ok, result in streaming_bulk(
            escon,
            typetuner,
            index=escon.index,
            chunk_size=ES_CHUNK_SIZE
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


def mongodb_index(mdbc, infile, typetuner):
    print("Reading from %s" % infile)
    i = 0
    t1 = time.time()
    for entry in read_modelseed_datafile(infile, typetuner):
        del(entry['_type'])
        mdbc.insert(entry)
        i += 1
    t2 = time.time()
    print("-- Processed %d entries, in %d sec"
          % (i, (t2 - t1)))
    return 1


# TODO: this is new, not tested yet
def main(infile, index, doctype, db, host=None, port=None):
    indxr = DBconnection(db, index, host, port)
    if doctype == TYPE_REACTION:
        typetuner = updatereactionrecord
    else:
        typetuner = updatecompoundrecord
    if db == 'Elasticsearch':
        es = indxr.es
        es_index(es, read_modelseed_datafile(infile, typetuner))
        es.indices.refresh(index=index)
    else:  # assume MongoDB
        indxr.mdbi.drop_collection(doctype)
        mongodb_index(indxr.mdbi[doctype], infile, typetuner)


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    biochemfolder = d + "/../../data/modelseeddb/Biochemistry/"
    parser = argparse.ArgumentParser(
        description='Index ModelSEED compounds/reactions files with'
                    ' MongoDB/Elasticsearch')
    parser.add_argument('--compoundsfile',
                        default=biochemfolder + "compounds.tsv",
                        help='ModelSEED compounds tsv file')
    parser.add_argument('--reactionsfile',
                        default=biochemfolder + "reactions.tsv",
                        help='ModelSEED reactions csv file')
    parser.add_argument('--index', default="ModelSEED",
                        help='Name of the Elasticsearch index or '
                             'MongoDB database')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or MongoDB server port")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()

    main(args.compoundsfile, args.index, TYPE_COMPOUND,
         args.db, args.host, args.port)
    main(args.reactionsfile, args.index, TYPE_REACTION,
         args.db, args.host, args.port)
