#!/usr/bin/env python
""" Index DrugBank xml dataset with MongoDB,
 also can save drug interactions graph as NetworkX graph file """
from __future__ import print_function

import argparse
import os
from zipfile import ZipFile

import xmltodict
import networkx as nx

from nosqlbiosets.dbutils import DBconnection

SOURCE_URL = "https://www.drugbank.ca/releases/latest"
DOCTYPE = 'drugbankdrug'  # MongoDB collection name
# List attributes, is processed by function unifylistattributes()
LIST_ATTRS = ["transporters", "drug-interactions", "food-interactions",
              "atc-codes", "affected-organisms", "targets", "enzymes",
              "carriers", "groups"]


# Make sure list attributes type are always list, TODO: inner list attributes
def unifylistattributes(e):
    for listname in LIST_ATTRS:
        objname = listname[:-1]
        if e[listname] is None:
            del e[listname]
        else:
            if isinstance(e[listname][objname], list):
                e[listname] = e[listname][objname]
            else:
                e[listname] = [e[listname][objname]]


# Read DrugBank xml files, index using the function indexf
def parse_drugbank_xmlfile(infile, indexf):
    infile = str(infile)
    print("Reading/indexing %s " % infile)
    if infile.endswith(".zip"):
        with ZipFile(infile) as zipf:
            for fname in zipf.namelist():
                with zipf.open(fname) as inf:
                    xmltodict.parse(inf, item_depth=2, attr_prefix='',
                                    item_callback=indexf)
    else:
        with open(infile, 'rb', buffering=1000) as inf:
            xmltodict.parse(inf, item_depth=2, attr_prefix='',
                            item_callback=indexf)
    print("\nCompleted")


class Indexer(DBconnection):

    def __init__(self, db, index, host, port, doctype):
        self.doctype = doctype
        self.index = index
        super(Indexer, self).__init__(db, index, host, port)
        if db != "Elasticsearch":
            self.mcl = self.mdbi[doctype]

    # Index DrugBank entry with MongoDB
    def mongodb_index_entry(self, _, entry):
        unifylistattributes(entry)
        docid = self.getdrugid(entry)
        spec = {"_id": docid}
        try:
            self.mcl.update(spec, entry, upsert=True)
            self.reportprogress()
            r = True
        except Exception as e:
            print(e)
            r = False
        return r

    interactions = set()

    def getdrugid(self, e):
        if isinstance(e['drugbank-id'], list):
            eid = e['drugbank-id'][0]['#text']
        else:
            eid = e['drugbank-id']['#text']
        return eid

    def saveinteractions(self, _, e):
        eid = self.getdrugid(e)
        print(eid)
        if e['drug-interactions'] is not None:
            if isinstance(e['drug-interactions']['drug-interaction'], list):
                for i in e['drug-interactions']['drug-interaction']:
                    did = i['drugbank-id']
                    self.interactions.add((eid, did))
            else:
                did = e['drug-interactions']['drug-interaction']['drugbank-id']
                self.interactions.add((eid, did))
        return True

    # Save drug-drug interactions as graph files in 2 different formats
    def saveasgraph(self):
        print("#edges = %d" % len(self.interactions))
        graph = nx.MultiDiGraph(list(self.interactions))
        nx.write_adjlist(graph, self.index + ".adjl")
        nx.write_gexf(graph, self.index + ".gexf")
        return graph


# Fields for text indexing  todo: improve this list
TEXT_FIELDS = ["description", "atc-codes.level.#text",
               "go-classifiers.description",
               "mechanism-of-action",
               "general-references.references.articles.article.citation",
               "targets.references.articles.article.citation",
               "targets.polypeptide.general-function",
               "targets.polypeptide.specific-function"]


def mongodb_indices(mdb):
    import pymongo
    from pymongo import IndexModel
    indx = [(field, pymongo.TEXT) for field in TEXT_FIELDS]
    mdb.create_indexes([IndexModel(indx,
                                   name="text-index-for-selected-fields")])
    mdb.create_index("drugbank-id")


def main(infile, index, doctype, db, host, port):
    indxr = Indexer(db, index, host, port, doctype)
    if db == 'MongoDB':
        parse_drugbank_xmlfile(infile, indxr.mongodb_index_entry)
        mongodb_indices(indxr.mdbi[DOCTYPE])
    else:
        parse_drugbank_xmlfile(infile, indxr.saveinteractions)
        indxr.saveasgraph()


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index DrugBank xml dataset with MongoDB, '
                    'downloaded from ' + SOURCE_URL +
                    ', can also save drug interactions as NetworkX graph file')
    parser.add_argument('-infile', '--infile',
                        required=True,
                        help='Input file name')
    parser.add_argument('--index',
                        default="drugbank",
                        help='Name of the MongoDB database,'
                             ' or filename for NetworkX graph')
    parser.add_argument('--doctype',
                        default=DOCTYPE,
                        help='MongoDB collection name')
    parser.add_argument('--host',
                        help='MongoDB server hostname')
    parser.add_argument('--port',
                        help="MongoDB server port number")
    parser.add_argument('--db', default='MongoDB',
                        help="Database: 'MongoDB' or NetworkX")
    args = parser.parse_args()
    main(args.infile, args.index, args.doctype, args.db, args.host, args.port)
