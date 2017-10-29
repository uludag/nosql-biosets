#!/usr/bin/env python
""" Index DrugBank xml dataset with MongoDB or save drug interactions
 as NetworkX graph file,
 download page: https://www.drugbank.ca/releases/latest """
from __future__ import print_function

import argparse
import os
from zipfile import ZipFile

import xmltodict

from nosqlbiosets.dbutils import DBconnection

DOCTYPE = 'drugbankdrug'


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

    drugs = set()
    interactions = set()

    def getdrugid(self, e):
        if isinstance(e['drugbank-id'], list):
            eid = e['drugbank-id'][0]['#text']
        else:
            eid = e['drugbank-id']['#text']
        return eid

    def saveinteractions(self, _, e):
        eid = self.getdrugid(e)
        self.drugs.add(eid)
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

    def saveasgraph(self):
        import networkx as nx
        print("#edges = %d" % len(self.interactions))
        graph = nx.MultiDiGraph(list(self.interactions))
        nx.write_adjlist(graph, DOCTYPE + ".adjl")
        nx.write_gexf(graph, DOCTYPE + ".gexf")
        nx.write_edgelist(graph, DOCTYPE + ".el")
        return graph


def main(infile, index, doctype, db, host, port):
    indxr = Indexer(db, index, host, port, doctype)
    if db == 'MongoDB':
        parse_drugbank_xmlfile(infile, indxr.mongodb_index_entry)
    else:
        parse_drugbank_xmlfile(infile, indxr.saveinteractions)
        indxr.saveasgraph()


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index DrugBank xml dataset with MongoDB, '
                    ' or save drug interactions as NetworkX graph file')
    parser.add_argument('-infile', '--infile',
                        required=True,
                        help='Input file name')
    parser.add_argument('--index',
                        default="drugbank",
                        help='Name of the MongoDB database,'
                             ' or filename for NetworkX graphs')
    parser.add_argument('--doctype',
                        default=DOCTYPE,
                        help='MongoDB collection name')
    parser.add_argument('--host',
                        help='MongoDB server hostname')
    parser.add_argument('--port',
                        help="MongoDB server port number")
    parser.add_argument('--db', default='NetworkX',
                        help="Database: 'MongoDB' or NetworkX")
    args = parser.parse_args()
    main(args.infile, args.index, args.doctype, args.db, args.host, args.port)
