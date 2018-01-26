#!/usr/bin/env python
""" Index DrugBank xml dataset with MongoDB or Elasticsearch,
    can also save drug interactions as graph files (experimental) """

from __future__ import print_function

import argparse
import os
from zipfile import ZipFile

import networkx as nx
import xmltodict
from pprint import pprint

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.objutils import checkbooleanattributes
from nosqlbiosets.objutils import unifylistattributes

SOURCE_URL = "https://www.drugbank.ca/releases/latest"
DOCTYPE = 'drug'  # MongoDB collection name
# List attributes, processed by function unifylistattributes()
LIST_ATTRS = ["transporters", "drug-interactions", "food-interactions",
              "atc-codes", "affected-organisms", "targets", "enzymes",
              "carriers", "groups", "salts", "products", 'categories',
              'pathways', 'go-classifiers', 'external-links']


def checkattributetypes(e):
    unifylistattributes(e, LIST_ATTRS)
    if 'pathways' in e:
        unifylistattributes(e['pathways'], ['drugs'])
    if "products" in e:
        for product in e["products"]:
            checkbooleanattributes(product,
                                   ["generic", "approved", "over-the-counter"])
    # Make sure type of numeric attributes are numeric
    atts = ["carriers", "enzymes", "targets", "transporters"]
    for att in atts:
        if att in e:
            for i in e[att]:
                if 'position' in i:
                    i['position'] = int(i['position'])
    atts = ["average-mass", "monoisotopic-mass"]
    for att in atts:
        if att in e:
            e[att] = float(e[att])
        if 'salts' in e and att in e['salts']:
            e['salts'][att] = float(e['salts'][att])


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
        if db == "MongoDB":
            self.mcl = self.mdbi[doctype]

    # Index DrugBank entry with MongoDB
    def mongodb_index_entry(self, _, entry):
        try:
            checkattributetypes(entry)
            docid = self.getdrugid(entry)
            spec = {"_id": docid}
            self.mcl.update(spec, entry, upsert=True)
            self.reportprogress()
            r = True
        except Exception as e:
            pprint(e)
            r = False
        return r

    # Index DrugBank entry with Elasticsearch
    def es_index_entry(self, _, entry):
        try:
            checkattributetypes(entry)
            docid = self.getdrugid(entry)
            entry['drugbank-id'] = docid  # TODO: keep all ids
            self.es.index(index=self.index, doc_type=self.doctype,
                          id=docid, body=entry)
            self.reportprogress()
            r = True
        except Exception as e:
            print(e)
            r = False
        return r

    def getdrugid(self, e):
        if isinstance(e['drugbank-id'], list):
            eid = e['drugbank-id'][0]['#text']
        else:
            eid = e['drugbank-id']['#text']
        return eid

    interactions = set()

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

    # Save drug-drug interactions as graph files in GML format
    # Both Cytoscape and Gephi are able to read GML files
    # For saving networks other than drug-drug interactions
    # and for saving subsets of the data see queries.py in this folder
    def saveasgraph(self):
        print("#edges = %d" % len(self.interactions))
        graph = nx.MultiDiGraph(list(self.interactions))
        nx.write_gml(graph, self.index + ".gml")
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
    mdb.create_index("name")
    mdb.create_index("products.name")


def main(infile, db, index, doctype=DOCTYPE, host=None, port=None):
    indxr = Indexer(db, index, host, port, doctype)
    if db == 'MongoDB':
        parse_drugbank_xmlfile(infile, indxr.mongodb_index_entry)
        mongodb_indices(indxr.mdbi[doctype])
    elif db == 'Elasticsearch':
        parse_drugbank_xmlfile(infile, indxr.es_index_entry)
        indxr.es.indices.refresh(index=index)
    else:
        parse_drugbank_xmlfile(infile, indxr.saveinteractions)
        indxr.saveasgraph()


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index DrugBank xml dataset with MongoDB or Elasticsearch, '
                    'downloaded from ' + SOURCE_URL +
                    ', can also save drug interactions as NetworkX graph file')
    parser.add_argument('-infile', '--infile',
                        required=True,
                        help='Input file name')
    parser.add_argument('--index',
                        default="drugbank",
                        help='Name of the MongoDB database or Elasticsearch'
                             ' index, or filename for NetworkX graph')
    parser.add_argument('--doctype',
                        default=DOCTYPE,
                        help='MongoDB collection name or'
                             ' Elasticsearch document type name')
    parser.add_argument('--host',
                        help='MongoDB or Elasticsearch server hostname')
    parser.add_argument('--port',
                        help="MongoDB or Elasticsearch server port number")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'MongoDB' or 'Elasticsearch',"
                             " if not set drug-drug interaction"
                             " network is saved to a graph file specified with"
                             " the '--graphfile' option")
    parser.add_argument('--graphfile', default='Elasticsearch',
                        help="Database: 'MongoDB' or 'Elasticsearch',"
                             "or if 'graphfile' drug-drug interaction"
                             "network saved as graph file")
    args = parser.parse_args()
    main(args.infile, args.db, args.index, args.doctype, args.host, args.port)
