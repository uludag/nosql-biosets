#!/usr/bin/env python
# Index NCBI PubTator gene2pub/disease2pub association files with Elasticsearch
# or gene2pub files with Neo4j

import argparse
import gzip
import json
import os

from elasticsearch.helpers import streaming_bulk

from nosqlbiosets.dbutils import DBconnection

ChunkSize = 2*1024


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


class Indexer(DBconnection):

    pubs = {}
    genes = {}

    def __init__(self, db, index, host, port):
        config = json.load(open(d + "/../../mappings/pubtator.json", "r"))
        super(Indexer, self).__init__(db, index, host, port,
                                      es_indexmappings=config['mappings'])

    # Read given PubTator file, index using the index function specified
    def read_and_index_pubtator_file(self, infile, doctype):
        print("Reading and indexing input file: %s" % infile)
        if infile.endswith(".gz"):
            f = gzip.open(infile, 'rt')
        else:
            f = open(infile, 'r')
        if self.db == "Elasticsearch":
            r = es_index(self.es, f, doctype)
            self.es.indices.refresh(index=args.index)
        else:
            self.delete_existing_records()
            self.set_unique_id_constraints()
            r = self.neo4j_index(f, doctype)
            self.neo4jc.close()
        return r

    def delete_existing_records(self):
        q1 = "match (a:Gene) detach delete a"
        q2 = "match (a:Pub) detach delete a"
        self.neo4jc.run(q1)
        self.neo4jc.run(q2)

    def set_unique_id_constraints(self):
        cq1 = "CREATE CONSTRAINT ON(n:Gene) ASSERT n.id IS UNIQUE"
        cq2 = "CREATE CONSTRAINT ON(n:Pub) ASSERT n.id IS UNIQUE"
        self.neo4jc.run(cq1)
        self.neo4jc.run(cq2)

    def neo4j_index(self, f, doctype):
        r = 0
        for row in parse_pub2gene_lines(f, r, doctype):
            self.write_nodes(row)
            r += 1
        print("%d mentions has been processed" % (r-1))
        print("%d publications have been found" % len(self.pubs))
        print("%d genes have been found" % len(self.genes))
        return r

    def write_nodes(self, intr):
        sid = intr['pmid']
        if sid not in self.pubs:
            self.pubs[sid] = True
            self.neo4jc.run("CREATE (a:Pub {id:'" + sid + "'})")
        for geneid in intr['geneids']:
            if geneid not in self.genes:
                self.genes[geneid] = True
                self.neo4jc.run("CREATE (a:Gene {id:'" + geneid + "'})")
            q = "match (a:Pub), (b:Gene)" \
                " where a.id={sid} AND b.id={geneid}"
            # TODO: multiple mentions and resources
            lc = " create (a)-[r:mentions {mention:{mention}}]->(b)"
            self.neo4jc.run(q + lc, {"sid": sid, "geneid": geneid,
                                     "mention": intr['mentions'][0]})


if __name__ == '__main__':
    d = os.path.dirname(os.path.abspath(__file__))
    parser = argparse.ArgumentParser(
        description='Index NCBI PubTator files using Elasticsearch or Neo4j')
    parser.add_argument('--gene2pubfile',
                        default=d + "/../../data/gene2pubtator.sample",
                        help='PubTator gene2pub file')
    parser.add_argument('--disease2pubfile',
                        default=d + "/../../data/disease2pubtator.sample",
                        help='PubTator disease2pub file')
    parser.add_argument('--index',
                        default="pubtator",
                        help='name of the Elasticsearch index')
    parser.add_argument('--host',
                        help='Elasticsearch or Neo4j server hostname')
    parser.add_argument('--port',
                        help="Elasticsearch or Neo4j server port")
    parser.add_argument('--db', default='Elasticsearch',
                        help="Database: 'Elasticsearch' or 'Neo4j'")
    args = parser.parse_args()
    indxr = Indexer(args.db, args.index, args.host, args.port)
    indxr.read_and_index_pubtator_file(args.gene2pubfile, 'gene2pub')
    if args.db == 'Elasticsearch':
        indxr.read_and_index_pubtator_file(args.disease2pubfile, 'disease2pub')
        # TODO: Neo4j indexer for disease2pub files
    # TODO: indexer with MongoDB
