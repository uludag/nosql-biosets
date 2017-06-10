#!/usr/bin/env python
"""Index HMDB protein/metabolite records in xml http://www.hmdb.ca/downloads """
from __future__ import print_function

import argparse
import json
import os
import sys
from gzip import GzipFile
from zipfile import ZipFile

import xmltodict
from elasticsearch import Elasticsearch


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


class Indexer:

    def __init__(self, doctype):
        self.doctype = doctype

    # index HMDB Metabolites/Proteins entry
    def es_index_hmdb_entry(self, _, entry):
        print(".", end='')
        sys.stdout.flush()
        docid = entry['accession']
        try:
            es.index(index=args.index, doc_type=self.doctype,
                     id=docid, body=json.dumps(entry))
            return True
        except Exception as e:
            print(e)
        return False


def main(infile, index, doctype):
    # if es.indices.exists(index=index):
    #    es.indices.delete(index=index, params={"timeout": "10s"})
    es.indices.create(index=index, params={"timeout": "10s"},
                      ignore=400, body={"settings": {"number_of_replicas": 0}})
    indxr = Indexer(doctype)
    parse_hmdb_xmlfile(infile, indxr.es_index_hmdb_entry)
    es.indices.refresh(index=index)


if __name__ == '__main__':
    conf = {"host": "localhost", "port": 9200}
    d = os.path.dirname(os.path.abspath(__file__))
    try:
        conf = json.load(open(d + "/../conf/elasticsearch.json", "r"))
    finally:
        pass
    parser = argparse.ArgumentParser(
        description='Index HMDB protein/metabolite entries, with Elasticsearch')
    parser.add_argument('-infile', '--infile',
                        default="../data/hmdb_proteins-first10.xml",
                        help='Input file name')
    parser.add_argument('--index',
                        default="hmdb",
                        help='Name of the Elasticsearch index')
    parser.add_argument('--doctype',
                        help='Document type (protein or metabolite)')
    parser.add_argument('--host', default=conf['host'],
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default=conf['port'],
                        help="Elasticsearch server port number")
    args = parser.parse_args()
    doctype_ = 'metabolite'
    if args.doctype is None and 'protein' in args.infile:
        doctype_ = 'protein'
    es = Elasticsearch(host=args.host, port=args.port, timeout=120)
    main(args.infile, args.index, doctype_)
