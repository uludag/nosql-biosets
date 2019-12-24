#!/usr/bin/env python
""" Index DrugBank xml dataset with MongoDB or Elasticsearch,
    can also save drug interactions as graph files (experimental) """

from __future__ import print_function

import argparse
import os
from pprint import pprint
from zipfile import ZipFile

import networkx as nx
import pymongo
import xmltodict
from pymongo import IndexModel

from nosqlbiosets.dbutils import DBconnection
from nosqlbiosets.objutils import *

SOURCE_URL = "https://www.drugbank.ca/releases/latest"
DOCTYPE = 'drugbank'  # MongoDB default collection name
# List attributes, processed by function unifylistattributes()
LIST_ATTRS = ["transporters", "drug-interactions", "food-interactions",
              "atc-codes", "affected-organisms", "targets", "enzymes",
              "carriers", "groups", "salts", "products",
              'pathways', 'go-classifiers', 'external-links',
              'external-identifiers']


# Update DrugBank entry for better database representation
def update_entry_forindexing(e, slim=True):
    unifylistattributes(e, LIST_ATTRS)
    unifylistattribute(e, 'categories', 'category')
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
                if 'polypeptide' in i:
                    if isinstance(i['polypeptide'], list):
                        for pp in i['polypeptide']:
                            if slim is True:
                                del pp['amino-acid-sequence']
                                del pp['gene-sequence']
                            unifylistattributes(pp, LIST_ATTRS)
                    else:
                        if slim is True:
                            del i['polypeptide']['amino-acid-sequence']
                            del i['polypeptide']['gene-sequence']
                        unifylistattributes(i['polypeptide'], LIST_ATTRS)
    if slim is True:
        if 'sequences' in e:
            del e['sequences']
        if 'patents' in e:
            del e['patents']
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

    def __init__(self, db, index, host, port, doctype, slim=True):
        self.doctype = doctype
        self.index = index
        self.slim = slim
        super(Indexer, self).__init__(db, index, host, port,
                                      mdbcollection=doctype, recreateindex=True)
        if db == "MongoDB":
            self.mcl = self.mdbi[doctype]

    # Index DrugBank entry with MongoDB
    def mongodb_index_entry(self, _, entry):
        try:
            update_entry_forindexing(entry, slim=self.slim)
            docid = self.getdrugid(entry)
            entry["_id"] = docid
            self.mcl.insert_one(entry)
            self.reportprogress()
            r = True
        except Exception as e:
            pprint(e)
            r = False
        return r

    # Index DrugBank entry with Elasticsearch
    def es_index_entry(self, _, entry):
        try:
            update_entry_forindexing(entry)
            docid = self.getdrugid(entry)
            entry['drugbank-id'] = docid
            self.es.index(index=self.index, doc_type='_doc',
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


# Fields for text indexing
TEXT_FIELDS = ["description", "atc-codes.level.#text",
               "go-classifiers.description",
               "mechanism-of-action",
               "general-references.references.articles.article.citation",
               "targets.references.articles.article.citation",
               "targets.polypeptide.gene-name",
               "drug interactions.description",
               "targets.polypeptide.general-function",
               "targets.polypeptide.specific-function"]


def mongodb_indices(mdb):
    indx = [(field, pymongo.TEXT) for field in TEXT_FIELDS]
    mdb.create_indexes([IndexModel(indx,
                                   name="text-index-for-selected-fields")])
    indx_fields = [
        "name", "products.name",
        "classification.class", "drug-interactions.name",
        "targets.polypeptide.gene-name",
        "protein_associations.protein.protein_accession",
        "metabolite_associations.metabolite.accession",
        "metabolite_associations.metabolite.name",
        [('name', pymongo.ASCENDING),
         ('drug-interactions.name', pymongo.ASCENDING)]
    ]
    for field in indx_fields:
        mdb.create_index(field)


def main(infile, db, index, doctype=DOCTYPE, host=None, port=None, slim=True):
    indxr = Indexer(db, index, host, port, doctype, slim)
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
                        default="biosets",
                        help='Name of the MongoDB database or Elasticsearch'
                             ' index, or filename for NetworkX graph')
    parser.add_argument('--mdbcollection',
                        default=DOCTYPE,
                        help='MongoDB collection name')
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
    parser.add_argument('--allfields', default=False, action='store_true',
                        help="By default sequence fields"
                             " and the patents field is not indexed."
                             " Select this option to index all fields")
    args = parser.parse_args()
    main(args.infile, args.db, args.index, args.mdbcollection,
         args.host, args.port, not args.allfields)
