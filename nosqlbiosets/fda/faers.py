#!/usr/bin/env python
# Index FDA Adverse Event Reporting System records
import argparse
import json
import os
import zipfile
from pprint import pprint

from elasticsearch.helpers import streaming_bulk
from pymongo import IndexModel
from pymongo.errors import BulkWriteError

from nosqlbiosets.dbutils import DBconnection, dbargs

CHUNKSIZE = 64
SOURCEURL = "https://download.open.fda.gov/drug/event/"


# Read FAERS report files, index using the index function specified
def read_and_index_faers_records(infolder, dbc, indexfunc):
    if os.path.isdir(infolder):
        for child in os.listdir(infolder):
            c = os.path.join(infolder, child)
            if os.path.isdir(c):
                print("Processing %s" % c)
                read_and_index_faers_records(c, dbc, indexfunc)
            else:
                if child.endswith(".json.zip") or os.path.isdir(c):
                    print("Processing %s" % c)
                    read_and_index_faers_records_(
                        os.path.basename(infolder), c, dbc, indexfunc)


def read_and_index_faers_records_(rfolder, infile, dbc, indexfunc):
    if infile.endswith(".zip"):
        zipf = zipfile.ZipFile(infile, 'r')
        rfile = zipf.namelist()[0]
        f = zipf.open(rfile)
    else:
        f = open(infile, 'r')
    rfile = os.path.basename(infile)
    print("Processing %s" % rfile)
    reportsinfo = json.load(f)
    indexfunc(dbc, reportsinfo, rfile, rfolder)


def update_date(r, date):
    import datetime
    date += "date"
    if date in r:
        d = datetime.date(int(r[date][:4]),
                          int(r[date][4:6]) if len(r[date]) > 4 else 1,
                          int(r[date][6:8]) if len(r[date]) > 6 else 1)
        d = datetime.datetime.combine(d, datetime.time.min)
        r[date] = d
        del r[date+"format"]


def read_reports(reports, rfile, rfolder):
    for i, r in enumerate(reports["results"]):
        r["_id"] = "%s-%s-%d" % (rfolder, rfile, i)
        for date in ["receive", "transmission", "receipt"]:
            update_date(r, date)
        for drug in r['patient']['drug']:
            for date in ["drugstart", "drugend"]:
                update_date(drug, date)
        yield r


def es_index_reports(dbc, reports, rfile, rfolder):
    r = 0
    for ok, result in streaming_bulk(
            dbc.es,
            read_reports(reports, rfile, rfolder),
            index=dbc.index, doc_type='_doc', chunk_size=CHUNKSIZE
    ):
        action, result = result.popitem()
        doc_id = '/%s/commits/%s' % (dbc.index, result['_id'])
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        else:
            r += 1
    return r


def mongodb_index_reports(mdbc, reports, rfile, rfolder):
    entries = list()
    try:
        for entry in read_reports(reports, rfile, rfolder):
            entries.append(entry)
            if len(entries) == CHUNKSIZE:
                mdbc.insert_many(entries)
                entries = list()
        if len(entries) > 0:
            mdbc.insert_many(entries)
    except BulkWriteError as bwe:
        pprint(bwe.details)
    return


def mongodb_indices(mdb):
    print("\nProcessing text and field indices")
    index = IndexModel([
        ("patient.reaction.reactionmeddrapt", "text"),
        ("patient.drug.drugindication", "text")
    ], name='text')
    mdb.create_indexes([index])
    indx_fields = [
        "patient.reaction.reactionmeddrapt",
        "patient.drug.medicinalproduct",
        "patient.drug.drugindication"
    ]
    for field in indx_fields:
        mdb.create_index(field)


def main(db, infile, mdbdb, mdbcollection, esindex,
         user=None, password=None, host=None, port=None, recreateindex=False):
    if db == "Elasticsearch":
        dbc = DBconnection(db, esindex, host=host, port=port,
                           recreateindex=recreateindex)
        read_and_index_faers_records(infile, dbc, es_index_reports)
        dbc.es.indices.refresh(index=esindex)
    elif db == "MongoDB":
        dbc = DBconnection(db, mdbdb, mdbcollection=mdbcollection,
                           host=host, port=port, user=user, password=password,
                           recreateindex=recreateindex)
        read_and_index_faers_records(infile, dbc.mdbi[mdbcollection],
                                     mongodb_index_reports)
        mongodb_indices(dbc.mdbi[mdbcollection])


if __name__ == '__main__':
    args = argparse.ArgumentParser(
        description='Index FDA FAERS dataset json files using Elasticsearch,'
                    ' or MongoDB, downloaded from ' + SOURCEURL)
    args.add_argument('--infile',
                      required=True,
                      help='drug-event .json or .json.zip files or'
                           'folder that includes the .json.zip files')
    dbargs(args)
    args = args.parse_args()
    main(args.dbtype, args.infile, args.mdbdb, args.mdbcollection,
         args.esindex,
         args.user, args.password, args.host, args.port, args.recreateindex)
