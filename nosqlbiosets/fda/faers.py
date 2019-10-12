#!/usr/bin/env python
# Index FDA Adverse Event Reporting System records
import argparse
import gzip
import json
from pprint import pprint

from elasticsearch.helpers import streaming_bulk
from pymongo.errors import BulkWriteError

from nosqlbiosets.dbutils import DBconnection, dbargs

CHUNKSIZE = 64
SOURCEURL = "https://download.open.fda.gov/drug/event/"


# Read FAERS report files, index using the index function specified
def read_and_index_faers_records(infile, dbc, indexfunc):
    if infile.endswith(".zip"):  # TODO: support for .zip files is broken
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    reportsinfo = json.load(f)
    r = indexfunc(dbc, reportsinfo)
    return r


def read_reports(l):
    for r in l["results"]:
        r["_id"] = int(r["safetyreportid"])
        yield r


def es_index_reports(dbc, reports):
    r = 0
    for ok, result in streaming_bulk(
            dbc.es,
            read_reports(reports),
            index=dbc.index, doc_type='_doc', chunk_size=CHUNKSIZE
    ):
        action, result = result.popitem()
        doc_id = '/%s/commits/%s' % (dbc.index, result['_id'])
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        else:
            r += 1
    return r


def mongodb_index_reports(mdbc, reports):
    entries = list()
    try:
        for entry in read_reports(reports):
            entries.append(entry)
            if len(entries) == CHUNKSIZE:
                mdbc.insert_many(entries)
                entries = list()
        mdbc.insert_many(entries)
    except BulkWriteError as bwe:
        pprint(bwe.details)
    return


def main(db, infile, mdbdb, mdbcollection, esindex,
         user=None, password=None, host=None, port=None, recreateindex=True):
    if db == "Elasticsearch":
        dbc = DBconnection(db, esindex, host=host, port=port,
                           recreateindex=recreateindex)
        read_and_index_faers_records(infile, dbc, es_index_reports)
        dbc.es.indices.refresh(index=esindex)
    elif db == "MongoDB":
        dbc = DBconnection(db, mdbdb, collection=mdbcollection,
                           host=host, port=port, user=user, password=password,
                           recreateindex=recreateindex)
        read_and_index_faers_records(infile, dbc.mdbi[mdbcollection],
                                     mongodb_index_reports)


if __name__ == '__main__':
    args = argparse.ArgumentParser(
        description='Index FDA FAERS dataset json files using Elasticsearch,'
                    ' or MongoDB, downloaded from ' + SOURCEURL)
    args.add_argument('--infile',
                      required=True,
                      help='Input HGNC file to index')
    dbargs(args)
    args = args.parse_args()
    main(args.dbtype, args.infile, args.mdbdb, args.mdbcollection,
         args.esindex,
         args.user, args.password, args.host, args.port)
