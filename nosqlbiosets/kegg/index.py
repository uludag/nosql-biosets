#!/usr/bin/env python
"""Index KEGG xml files """
from __future__ import print_function

import argparse
import json
import sys
import tarfile

import xmltodict
from pymongo import IndexModel

from nosqlbiosets.dbutils import DBconnection


# Read given KEGG pathway xml/tar file,
# index using the index function specified
def read_and_index_kegg_xmltarfile(infile, indexf):
    print("\nProcessing tar file: %s " % infile)
    i = 0
    tar = tarfile.open(infile, 'r:gz')
    for member in tar:
        f = tar.extractfile(member)
        if f is None:
            continue  # if the tarfile entry is a folder then skip
        r = xmltodict.parse(f, attr_prefix='')
        if not indexf(1, r['pathway']):
            break
        print(".", end='', flush=True)
    return i


# Read KEGG Pathway files, index using the function indexf
def parse_kegg_xmlfile(infile, indexf):
    infile = str(infile)
    print("Reading/indexing %s " % infile)
    if infile.endswith(".tar.gz"):
        read_and_index_kegg_xmltarfile(infile, indexf)
    elif infile.endswith(".xml"):
        with open(infile, 'rb', buffering=1000) as inf:
            r = xmltodict.parse(inf, attr_prefix='')
            indexf(1, r['pathway'])
    else:
        print("only .xml and .tar.gz files are supported")
    print("\nCompleted")


class Indexer(DBconnection):

    def __init__(self, db, index, host, port, doctype):
        self.doctype = doctype
        super(Indexer, self).__init__(db, index, host, port)
        if db != "Elasticsearch":
            self.mcl = self.mdbi[doctype]

    # index KEGG Pathway entry with Elasticsearch
    def es_index_kegg_entry(self, _, entry):
        print(".", end='')
        sys.stdout.flush()
        docid = entry['name']
        for e in entry['entry']:
            del(e['graphics'])
            if 'link' in e:
                del(e['link'])
        try:
            self.es.index(index=args.index, doc_type=self.doctype,
                          id=docid, body=json.dumps(entry))
            return True
        except Exception as e:
            print(e)
        return False

    # index KEGG Pathway entry with MongoDB
    def mongodb_index_kegg_entry(self, _, entry):
        print(".", end='')
        sys.stdout.flush()
        docid = entry['name']
        spec = {"_id": docid}
        for e in entry['entry']:
            del(e['graphics'])
            if 'link' in e:
                del(e['link'])
        # TODO: better understand the data for better representation
        try:
            self.mcl.update(spec, entry, upsert=True)
            return True
        except Exception as e:
            print(e)
        return False


def mongodb_textindex(mdb):
    index = IndexModel([("title", "text")])
    mdb.create_indexes([index])
    return


def main(infile, index, doctype, db, host, port):
    indxr = Indexer(db, index, host, port, doctype)
    if db == 'Elasticsearch':
        parse_kegg_xmlfile(infile, indxr.es_index_kegg_entry)
        indxr.es.indices.refresh(index=index)
    else:
        parse_kegg_xmlfile(infile, indxr.mongodb_index_kegg_entry)
        mongodb_textindex(indxr.mcl)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index KEGG pathway xml records,'
                    ' with Elasticsearch or MongoDB')
    parser.add_argument('-infile', '--infile',
                        default="../../data/kegg/xml/kgml/metabolic/"
                                "organisms/hsa.tar.gz",
                        # default="../../data/hsa01210.xml",
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
    doctype_ = 'pathway'
    main(args.infile, args.index, doctype_, args.db, args.host, args.port)
