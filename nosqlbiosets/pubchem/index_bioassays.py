#!/usr/bin/env python
""" Index PubChem Bioassay json files with Elasticsearch or MongoDB"""
from __future__ import print_function

import argparse
import gzip
import json
import os
import struct
import sys
import time
from zipfile import ZipFile

from nosqlbiosets.dbutils import DBconnection

# Document type name for the Elascticsearch or Collection name for MongoDB
DOCTYPE = "bioassay"
INDEX = "pubchem"

# Maximum size of uncompressed files that should be indexed
MaxEntrySize = 256*1024*1024

# Maximum total size of uncompressed files indexed
# before an Elasticsearch  _refresh call (~equivalent of database commits)
MaxBulkSize = 4*MaxEntrySize


def getuncompressedsize(filename):
    with open(filename, 'rb') as f:
        return getuncompressedsize_(f)


def getuncompressedsize_(f):
    f.seek(-4, 2)
    return struct.unpack('I', f.read(4))[0]


# Read given bioassay json file, index using the index function specified
# If the input file is a folder then iterate over files in the folder
def read_and_index_pubchem_bioassays(infile, es, indexfunc):
    print("Reading %s " % infile)
    i = 0
    t1 = time.time()
    if os.path.isdir(infile):
        for child in os.listdir(infile):
            c = os.path.join(infile, child)
            if child.endswith(".zip"):
                read_and_index_pubchem_bioassays_zipfile(c, es, indexfunc)
            else:
                read_and_index_pubchem_bioassays_file(c, es, indexfunc)
            i += 1
    else:
        if infile.endswith(".zip"):
            read_and_index_pubchem_bioassays_zipfile(infile, es, indexfunc)
        else:
            read_and_index_pubchem_bioassays_file(infile, es, indexfunc)
        i = 1
    t2 = time.time()
    print("-- %d files have been processed, in %dms"
          % (i, (t2 - t1) * 1000))
    return None


# Read given bioassays zip file, index using the index function specified
def read_and_index_pubchem_bioassays_zipfile(zipfile, dbc, indexf):
    print("\nProcessing %s " % zipfile)
    i = 0
    r = 0
    with ZipFile(zipfile) as myzip:
        for fname in myzip.namelist():
            aid = fname[fname.find('/')+1:fname.find(".json")]
            with myzip.open(fname) as jfile:
                # TODO: gzip.open() doesn't work with python2
                f = gzip.open(jfile, 'rt')  # read as text, input to json.load
                r = index_bioassay(dbc, f, r, aid, indexf)
                i += 1
    return i


# Read given bioassay file, index using the index function specified
def read_and_index_pubchem_bioassays_file(infile, dbc, indexfunc):
    if infile.endswith(".json.gz"):
        print(getuncompressedsize(infile))
        f = gzip.open(infile, 'rt')
    elif infile.endswith(".json"):
        f = open(infile, 'r')
    else:
        print('Unsupported file extension; %s' % infile)
        return
    aid = infile[infile.rfind('/') + 1:infile.find(".json")]
    r = index_bioassay(dbc, f, 0, aid, indexfunc)
    return r


# Return given date in format YY-MM-DD
def update_date(date):
    d = "{}-{:02}-{:02}".format(date["std"]["year"], date["std"]["month"],
                                date["std"]["day"])
    del (date["std"])
    return d


def update_dates(doc):
    for data in doc["PC_AssaySubmit"]["data"]:
        date = data["date"]
        d = update_date(date)
        data["date"] = d
    db = doc["PC_AssaySubmit"]["assay"]["descr"]["aid_source"]["db"]
    if "date" in db:
        d = update_date(db["date"])
        doc["PC_AssaySubmit"]["assay"]["descr"]["aid_source"]["db"]["date"] = d
    return


def index_bioassay(es, f, r, aid_, indexf):
    doc = json.load(f)
    if f.tell() < MaxEntrySize:
        update_dates(doc)
        aid = doc['PC_AssaySubmit']['assay']['descr']['aid']['id']
        if str(aid) != aid_:
            print("File name and assay ids not same, please check '%s' vs '%s'"
                  % (aid, aid_))
            return r
        r = indexf(es, f, r, aid_, doc)
    else:
        print("Large entry:  aid=%s  filesize=%d  max-entry-size=%d" %
              (aid_, f.tell(), MaxEntrySize))
    return r


def es_index_bioassay(dbc, f, r, aid, doc):
    try:
        if r > 0 and (r + f.tell() > MaxBulkSize):
            print("r", end='', file=sys.stdout)
            sys.stdout.flush()
            # refresh/commit to avoid Elasticsearch out-of-memory errors
            dbc.es.indices.refresh(index=args.index)
            dbc.es.indices.clear_cache(index=args.index)
            r = 0
        print(".", end='', file=sys.stdout)
        sys.stdout.flush()
        docx = doc['PC_AssaySubmit']
        dbc.es.index(index=dbc.index, doc_type=DOCTYPE,
                     id=aid, body=docx)
        r += f.tell()
    except Exception as e:
        print(e)
    return r


def mongodb_index_bioassay(dbc, f, r, aid, doc):
    try:
        if r > 0 and (r + f.tell() > MaxBulkSize):
            print("r", end='', file=sys.stdout)
            sys.stdout.flush()
            r = 0
        print(".", end='', file=sys.stdout)
        sys.stdout.flush()
        docx = doc['PC_AssaySubmit']
        dbc.mdbi[DOCTYPE].update({"_id": aid}, docx, upsert=True)
        r += f.tell()
    except Exception as e:
        print(e)
    return r


def main(db, infile, index=INDEX, host=None, port=None):
    if db == 'Elasticsearch':
        d = os.path.dirname(os.path.abspath(__file__))
        cfg = json.load(open(d + "/../../mappings/pubchem-bioassays.json", "r"))
        dbc = DBconnection(db, index, host, port, recreateindex=True,
                           es_indexmappings=cfg["mappings"])
        read_and_index_pubchem_bioassays(infile, dbc, es_index_bioassay)
        dbc.es.indices.refresh(index=index)
    else:
        dbc = DBconnection(db, index, host, port)
        read_and_index_pubchem_bioassays(infile, dbc,
                                         mongodb_index_bioassay)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index PubChem Bioassays json files'
                    ' with Elasticsearch or MongoDB')
    parser.add_argument('--infile', '--infolder',
                        help='Input file to index, or input folder with '
                             'zipped bioassay json files')
    parser.add_argument('--index',
                        default=INDEX,
                        help='Name of Elasticsearch index or MongoDB database')
    parser.add_argument('--host',
                        help='Elasticsearch/MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch/MongoDB server port")
    parser.add_argument('-db', '--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()
    main(args.db, args.infile, args.index, args.host, args.port)
