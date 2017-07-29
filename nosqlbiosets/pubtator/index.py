#!/usr/bin/env python
# Index NCBI PubTator gene2pub/disease2pub association files with Elasticsearch
# Also included initial work with Neo4j

import argparse
import gzip
import json
import os

from elasticsearch.helpers import streaming_bulk
from neo4j.v1 import GraphDatabase, basic_auth

from nosqlbiosets.dbutils import DBconnection

ChunkSize = 2*1024


# Read given PubTator file, index using the index function specified
def read_and_index_pubtator_file(infile, es, indexfunc, doctype):
    print("Reading and indexing input file: %s" % infile)
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    r = indexfunc(es, f, doctype)
    return r


def parse_pub2gene_lines(f, r, doctype):
    f.readline()  # skip header line
    for line in f:
        a = line.strip('\n').split('\t')
        if len(a) != 4:
            print("Line has more than 4 fields: %s" % line)
            exit(-1)
        refids = a[1].replace(';', ',').split(',')
        doc = {
            '_id': r,
            "pmid": a[0],
            "mentions": a[2].split('|'),
            "resource": a[3].split('|')
        }
        if doctype == 'gene2pub':
            doc["geneids"] = refids
        elif doctype == 'disease2pub':
            doc["diseaseids"] = refids
        yield doc
        r += 1


def es_index(es, f, doctype):
    r = 0
    for ok, result in streaming_bulk(
            es,
            parse_pub2gene_lines(f, r, doctype),
            index=args.index,
            doc_type=doctype,
            chunk_size=ChunkSize
    ):
        action, result = result.popitem()
        doc_id = '/%s/commits/%s' % (args.index, result['_id'])
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        else:
            r += 1
    return r


def index(es):
    if es.indices.exists(index=args.index):
        es.indices.delete(index=args.index, params={"timeout": "10s"})
    i = es.info()
    v = int(i['version']['number'][0])
    if v >= 5:
        iconfig = json.load(open(d + "/../../mappings/pubtator.json", "r"))
    else:
        iconfig = json.load(open(d + "/../../mappings/pubtator-es2.json", "r"))
    es.indices.create(index=args.index, params={"timeout": "20s"},
                      ignore=400, body=iconfig, wait_for_active_shards=1)
    read_and_index_pubtator_file(args.gene2pubfile, es, es_index,
                                 'gene2pub')
    if os.path.exists(args.disease2pubfile):
        read_and_index_pubtator_file(args.disease2pubfile, es, es_index,
                                     'disease2pub')
    es.indices.refresh(index=args.index)


class Neo4jIndexer:

    pubs = {}
    genes = {}

    def __init__(self):
        driver = GraphDatabase.driver("bolt://localhost",
                                      auth=basic_auth("neo4j", "passwd"))
        self.session = driver.session()

    def indexwithneo4j(self):
        self.delete_existing_records()
        read_and_index_pubtator_file(args.gene2pubfile, self.session,
                                     self.index, 'gene2pub')

    def delete_existing_records(self):
        q1 = "match (a:Gene) detach delete a"
        q2 = "match (a:Pub) detach delete a"
        self.session.run(q1)
        self.session.run(q2)

    def set_unique_id_constraints(self):
        cq1 = "CREATE CONSTRAINT ON(n:Gene) ASSERT n.id IS UNIQUE"
        cq2 = "CREATE CONSTRAINT ON(n:Pub) ASSERT n.id IS UNIQUE"
        self.session.run(cq1)
        self.session.run(cq2)

    def index(self, _, f, doctype):
        r = 0
        for row in parse_pub2gene_lines(f, r, doctype):
            print(row)
            self.write_nodes(row)
            r += 1
        print("%d mentions?? has been processed" % (r-1))
        print("%d publications have been found" % len(self.pubs))
        print("%d genes have been found" % len(self.genes))
        return r

    def write_nodes(self, intr):
        sid = intr['pmid']
        if sid not in self.pubs:
            self.pubs[sid] = True
            self.session.run("CREATE (a:Pub {id:'" + sid + "'})")
        for geneid in intr['geneids']:
            if geneid not in self.genes:
                self.genes[geneid] = True
                self.session.run("CREATE (a:Gene {id:'" + geneid + "'})")
            q = "match (a:Pub), (b:Gene)" \
                " where a.id='" + sid +\
                "' AND b.id='" + geneid + "'"
            # TODO: multiple mentions and resources
            lc = " create (a)-[r:mentions {mention: '%s'}]->(b)  return r"\
                 % intr['mentions'][0]
            self.session.run(q + lc)


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index NCBI PubTator files using Elasticsearch')
    parser.add_argument('--gene2pubfile',
                        default=d + "/../../data/gene2pubtator.sample",
                        help='PubTator gene2pub file')
    parser.add_argument('--disease2pubfile',
                        default=d + "/../../data/disease2pubtator.sample",
                        help='PubTator disease2pub file')
    parser.add_argument('--index',
                        default="nosqlbiosets",
                        help='name of the Elasticsearch index')
    parser.add_argument('--host',
                        help='Elasticsearch server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch server port")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'Neo4j'")
    args = parser.parse_args()
    print(args.db)
    if args.db == 'Elasticsearch':
        dbc = DBconnection(args.db, args.index, args.host, args.port)
        index(dbc.es)
    else:
        Neo4jIndexer().indexwithneo4j()

    # TODO: indexer with MongoDB
