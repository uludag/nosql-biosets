#!/usr/bin/env python
""" Index CSV files with Elasticsearch, MongoDB or PostgreSQL """
from __future__ import print_function

import argparse
import csv
from pprint import pprint

import pandas as pd
from elasticsearch.helpers import streaming_bulk
from pymongo.errors import BulkWriteError

from nosqlbiosets.dbutils import DBconnection

CHUNK_SIZE = 256


def read_csvfile(infile, delimiter='\t', collection=None, tuner=None):
    i = 0

    with open(infile) as csvfile:
        reader = csv.DictReader(csvfile, delimiter=delimiter, quotechar='"')
        for row in reader:
            i += 1
            if tuner is not None:
                row = tuner(row, i)
            if collection is not None:
                row['_collection'] = collection
                row['_id'] = "%s %d" % (collection, i)
            else:
                row['_id'] = i
            yield row


def es_index_csv(es, csvfile, index, collection, delimiter, tuner=None):
    print(delimiter)
    for ok, result in streaming_bulk(
            es, read_csvfile(csvfile, delimiter, collection, tuner),
            index=index, doc_type="biosets", chunk_size=CHUNK_SIZE
    ):
        if not ok:
            action, result = result.popitem()
            doc_id = '/%s/commits/%s' % (args.index, result['_id'])
            print('Failed to %s document %s: %r' % (action, doc_id, result))
    return


def mongodb_index_csv(mdbi, csvfile, collection, delimiter, tuner=None):
    entries = list()
    mdbi[collection].delete_many({})
    try:
        for entry in read_csvfile(csvfile, delimiter, tuner):
            entries.append(entry)
            if len(entries) == CHUNK_SIZE:
                mdbi[collection].insert_many(entries)
                entries = list()
        mdbi[collection].insert_many(entries)
    except BulkWriteError as bwe:
        pprint(bwe.details)
    return


def pgsql_index(sqlc, infile, table, delimiter):
    reader = pd.read_csv(infile, sep=delimiter, chunksize=CHUNK_SIZE)
    i = 0
    for chunk in reader:
        chunk.to_sql(name=table,
                     if_exists='replace' if i == 0 else 'append',
                     con=sqlc)
        i += 1


def main(db, infile, index, collection, delimiter=',',
         user=None, password=None, host=None, port=None):
    dbc = DBconnection(db, index, host=host, port=port, user=user,
                       password=password)
    if dbc.db == "Elasticsearch":
        dbc.es.delete_by_query(index=index, doc_type=collection,
                               body={"query": {"match": {
                                   "_collection": collection
                               }}})
        es_index_csv(dbc.es, infile, index, collection, delimiter)
        dbc.es.indices.refresh(index=index)
    elif dbc.db == "MongoDB":
        mongodb_index_csv(dbc.mdbi, infile, collection, delimiter)
    else:  # Assume PostgreSQL
        pgsql_index(dbc.sqlc, infile, collection, delimiter)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index CSV files with Elasticsearch, '
                    'MongoDB or PostgreSQL')
    parser.add_argument('--infile',
                        required=True,
                        help='Input CSV file to index, headers required')
    parser.add_argument('--index',
                        help='Index name for Elasticsearch, '
                             'database name for MongoDB and PostgreSQL')
    parser.add_argument('--collection',
                        help='Collection name for MongoDB,'
                             ' document type name with Elasticsearch,'
                             ' table name with PostgreSQL')
    parser.add_argument('--delimiter', default=',',
                        help="Columns delimiter for input CSV file, "
                             "for tsv files enter --delimiter $'\t'")
    parser.add_argument('--host',
                        help='Hostname for the database server')
    parser.add_argument('--port',
                        help="Port number of the database server")
    parser.add_argument('--db', default='MongoDB',
                        help="Database: 'Elasticsearch', 'MongoDB',"
                             " or 'PostgreSQL'")
    parser.add_argument('--user',
                        help="Database user name, "
                             "supported with PostgreSQL option only")
    parser.add_argument('--password',
                        help="Password for the database user, "
                             "supported with PostgreSQL option only")
    args = parser.parse_args()
    main(args.db, args.infile, args.index, args.collection, args.delimiter,
         args.user, args.password, args.host, args.port)
