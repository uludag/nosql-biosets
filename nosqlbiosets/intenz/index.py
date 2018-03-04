#!/usr/bin/env python
"""Index IntEnz xml files, with Elasticsearch, MongoDB, and Neo4j"""
from __future__ import print_function

import argparse
import os
import traceback

import xmltodict
from six import string_types

from nosqlbiosets.dbutils import DBconnection

DOCTYPE = 'intenz'     # Default document-type or collection name


class Indexer(DBconnection):

    def __init__(self, db, index, host=None, port=None, doctype=DOCTYPE):
        self.doctype = doctype
        self.index = index
        self.db = db
        indxcfg = {
            "index.number_of_replicas": 0,
            "index.number_of_shards": 5,
            "index.refresh_interval": "30s"}
        super(Indexer, self).__init__(db, index, host, port,
                                      es_indexsettings=indxcfg,
                                      recreateindex=False)
        if db == "MongoDB":
            self.mcl = self.mdbi[doctype]

    # Parses IntEnz xml file, then calls index function
    def parse_intenz_xmlfiles(self, infile):
        infile = str(infile)
        print("Reading/indexing %s " % infile)
        if not infile.endswith(".xml"):
            print("Input file should be IntEnz xml file")
        else:
            with open(infile, 'rb', buffering=1000) as inf:
                namespaces = {
                    'http://www.xml-cml.org/schema/cml2/react': None,
                    'http://www.ebi.ac.uk/intenz': None
                }
                xmltodict.parse(inf, item_depth=5,
                                item_callback=self.index_intenz_entry,
                                process_namespaces=True,
                                namespaces=namespaces,
                                attr_prefix='')
        print("\nCompleted")
        if self.db == "Neo4j":
            self.indexwithneo4j()
        # TODO: option to save graph, using graphutils.save_graph()
        # graph = nx.MultiDiGraph(list(self.edges))

    def index_intenz_entry(self, _, entry):
        if not isinstance(entry, string_types):
            docid = entry['ec'][3:]
            # TODO: remove extra layers, e.g. reactions.reaction -> reactions
            # reactantList.reactant -> reactants
            # productList.product -> products
            try:
                if self.db == "Elasticsearch":
                    self.es.index(index=self.index, doc_type=self.doctype,
                                  op_type='create', ignore=409,
                                  filter_path=['hits.hits._id'],
                                  id=docid, body=entry)
                elif self.db == "MongoDB":
                    spec = {"_id": docid}
                    self.mcl.update(spec, entry, upsert=True)
                else:  # Neo4j
                    self.updatereactionsandelements_sets(entry)
            except Exception as e:
                print("ERROR: %s" % e)
                print(traceback.format_exc())
                exit(-1)
            self.reportprogress(40)
        return True

    reactions = dict()
    reactants = set()
    products = set()
    edges = set()

    def indexwithneo4j(self):
        print("Indexing collected data with Neo4j")
        with self.neo4jc.begin_transaction() as tx:
            tx.run("match ()-[a:Produces]-() delete a")
            tx.run("match ()-[a:Reactant_in]-() delete a")
            tx.run("match (a:Substrate) delete a")
            tx.run("match (a:Product) delete a")
            tx.run("match (a:Reaction) delete a")
            tx.sync()
            for r in self.reactants:
                c = "CREATE (a:Substrate {id:{id}})"
                tx.run(c, {"id": r})
            for r in self.products:
                c = "CREATE (a:Product {id:{id}})"
                tx.run(c, {"id": r})
        with self.neo4jc.begin_transaction() as tx:
            for r in self.reactions:
                r = self.reactions[r]
                if 'id' in r:
                    rid = r['id']
                    tx.run("CREATE (a:Reaction {id:{rid}, name:{name}})",
                           rid=rid, name=r['name'])
                    for re in r['reactantList']['reactant']:
                        if isinstance(re, dict):
                            substrate = re['title']
                        else:
                            substrate = re
                        if (substrate, rid) in self.edges:
                            c = "MATCH (r:Reaction), (s:Substrate) " \
                                " WHERE  r.id = {rid} " \
                                " AND s.id = {substrate} " \
                                "CREATE (s)-[:Reactant_in {r:{rid}}]->(r)"
                            tx.run(c, rid=rid,
                                   substrate=substrate)
                            self.edges.remove((substrate, rid))
                    for pr in r['productList']['product']:
                        if isinstance(pr, dict):
                            product = pr['title']
                        else:
                            product = pr
                        if (rid, product) in self.edges:
                            c = "MATCH (r:Reaction), (t:Product) " \
                                " WHERE  r.id = {rid} " \
                                " AND t.id = {productid} " \
                                "CREATE (r)-[:Produces {r:{rid}}]->(t)"
                            tx.run(c, rid=rid,
                                   productid=product)
                            self.edges.remove((rid, product))
                    tx.sync()

    def updatereactionsandelements_sets(self, e):
        if 'reactions' not in e:
            return
        for r in e['reactions']['reaction']:
            if 'id' in r:
                rid = r['id']
                if rid not in self.reactions:
                    self.reactions[rid] = r
                rproducts = set()
                for pr in r['productList']['product']:
                    if isinstance(pr, dict):
                        product = pr['title']
                    else:
                        product = pr
                    rproducts.add(product)
                    self.products.add(product)
                    self.edges.add((rid, product))
                for re in r['reactantList']['reactant']:
                    if isinstance(re, dict):
                        substrate = re['title']
                    else:
                        substrate = re
                    self.reactants.add(substrate)
                    self.edges.add((substrate, rid))


def mongodb_textindex(mdb):
    index = [
        ("comments.comment.#text", "text"), ("synonyms.synonym.#text", "text")]
    mdb.create_index(index, name="text")


def main(infile, index, doctype, db, host=None, port=None):
    indxr = Indexer(db, index, host, port, doctype)
    indxr.parse_intenz_xmlfiles(infile)
    if db == 'Elasticsearch':
        indxr.es.indices.refresh(index=index)
    elif db == 'MongoDB':
        mongodb_textindex(indxr.mcl)


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index IntEnz xml files,'
                    ' with Elasticsearch, MongoDB or Neo4j')
    parser.add_argument('-infile', '--infile',
                        help='Input file name (intenz/ASCII/intenz.xml)')
    parser.add_argument('--index',
                        default="biosets",
                        help='Name of the Elasticsearch index'
                             ' or MongoDB database')
    parser.add_argument('--doctype', default=DOCTYPE,
                        help='Document type name for Elasticsearch, '
                             'collection name for MongoDB')
    parser.add_argument('--host',
                        help='Elasticsearch, MongoDB or Neo4j server hostname')
    parser.add_argument('--port', type=int,
                        help="Elasticsearch, MongoDB or Neo4j server port")
    parser.add_argument('--db', default='MongoDB',
                        help="Database: 'Elasticsearch', 'MongoDB' or 'Neo4j'")
    args = parser.parse_args()
    main(args.infile, args.index, args.doctype, args.db, args.host, args.port)
