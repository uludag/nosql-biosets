#!/usr/bin/env python
"""Index HMDB protein/metabolite records in xml,
 download page: http://www.hmdb.ca/downloads """
from __future__ import print_function

import argparse
import json
import sys
from gzip import GzipFile
from zipfile import ZipFile

import xmltodict
from pymongo import IndexModel

from nosqlbiosets.dbutils import DBconnection


# Read HMDB Metabolites/Proteins files, index using the function indexf
def parse_hmdb_xmlfile(infile, indexf):
    infile = str(infile)
    print("Reading/indexing %s " % infile)
    if infile.endswith(".gz"):
        with GzipFile(infile) as inf:
            xmltodict.parse(inf, item_depth=2, item_callback=indexf)
    elif infile.endswith(".zip"):
        with ZipFile(infile) as zipf:
            for fname in zipf.namelist():
                with zipf.open(fname) as inf:
                    xmltodict.parse(inf, item_depth=2,
                                    item_callback=indexf)
    else:
        with open(infile, 'rb', buffering=1000) as inf:
            xmltodict.parse(inf, item_depth=2, item_callback=indexf)
    print("\nCompleted")


class Indexer(DBconnection):

    def __init__(self, db, index, host, port, doctype):
        self.doctype = doctype
        super(Indexer, self).__init__(db, index, host, port)
        if db != "Elasticsearch":
            self.mcl = self.mdbi[doctype]

    # index HMDB Metabolites/Proteins entry with Elasticsearch
    def es_index_hmdb_entry(self, _, entry):
        print(".", end='')
        sys.stdout.flush()
        docid = entry['accession']
        try:
            self.es.index(index=args.index, doc_type=self.doctype,
                          id=docid, body=json.dumps(entry))
            return True
        except Exception as e:
            print(e)
        return False

    # index HMDB Metabolites/Proteins entry with MongoDB
    def mongodb_index_hmdb_entry(self, _, entry):
        print(".", end='')
        sys.stdout.flush()
        docid = entry['accession']
        spec = {"_id": docid}
        try:
            self.mcl.update(spec, entry, upsert=True)
            return True
        except Exception as e:
            print(e)
        return False


def mongodb_textindex(mdb, doctype):
    if doctype == 'metabolite':
        index = IndexModel([
            ("description", "text"), ("name", "text"),
            ("taxanomy.description", "text")])
        mdb.create_indexes([index])
    return


def main(infile, index, doctype, db, host, port):
    indxr = Indexer(db, index, host, port, doctype)
    if db == 'Elasticsearch':
        parse_hmdb_xmlfile(infile, indxr.es_index_hmdb_entry)
        indxr.es.indices.refresh(index=index)
    else:
        parse_hmdb_xmlfile(infile, indxr.mongodb_index_hmdb_entry)
        mongodb_textindex(indxr.mcl, doctype)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index HMDB protein/metabolite entries,'
                    ' with Elasticsearch or MongoDB')
    parser.add_argument('-infile', '--infile',
                        default="../data/hmdb_proteins-first10.xml.gz",
                        help='Input file name')
    parser.add_argument('--index',
                        default="biosets",
                        help='Name of the Elasticsearch index or MongoDB db')
    parser.add_argument('--doctype',
                        help='Document type (protein or metabolite)')
    parser.add_argument('--host',
                        help='Elasticsearch or MongoDB server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or MongoDB server port number")
    parser.add_argument('--db', default='Elasticsearch_',
                        help="Database: 'Elasticsearch' or 'MongoDB'")
    args = parser.parse_args()
    doctype_ = 'metabolite'
    if args.doctype is None and 'protein' in args.infile:
        doctype_ = 'protein'
    main(args.infile, args.index, doctype_, args.db, args.host, args.port)
