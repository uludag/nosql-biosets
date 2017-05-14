#!/usr/bin/env python
# Index NCBI PubTator gene2pub/disease2pub association files with Elasticsearch
# Also included initial work with Neo4j

import argparse
import gzip
import json
import os

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk
from neo4j.v1 import GraphDatabase, basic_auth

ChunkSize = 8*1024


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
        iconfig = json.load(open(d + "/../mappings/pubtator.json", "rt"))
    else:
        iconfig = json.load(open(d + "/../mappings/pubtator-es2.json", "rt"))
    es.indices.create(index=args.index, params={"timeout": "20s"},
                      ignore=400, body=iconfig, wait_for_active_shards=1)
    read_and_index_pubtator_file(args.gene2pubfile, es, es_index,
                                 'gene2pub')
    read_and_index_pubtator_file(args.disease2pubfile, es, es_index,
                                 'disease2pub')
    es.indices.refresh(index=args.index)


def neo4j_delete_existing_records(session):
    q1 = "match (a:Gene) detach delete a"
    q2 = "match (a:Pub) detach delete a"
    session.run(q1)
    session.run(q2)


def neo4j_set_unique_id_constraints(session):
    cq1 = "CREATE CONSTRAINT ON(n:Gene) ASSERT n.id IS UNIQUE"
    cq2 = "CREATE CONSTRAINT ON(n:Pub) ASSERT n.id IS UNIQUE"
    session.run(cq1)
    session.run(cq2)

intrs = {}  # map to get the number of times interactions were recorded
pubs = {}
genes = {}


def neo4j_index(session, f, doctype):
    r = 0
    for row in parse_pub2gene_lines(f, r, doctype):
        print(row)
        write_nodes(session, row)
        save_intr(row)
        r += 1
    print("%d interactions has been processed" % (r-1))
    print("%d source proteins have been found" % len(pubs))
    print("%d target lncRNA genes have been found" % len(genes))
    write_saved_intrs(session)
    return r


def write_nodes(session, intr):
    sid = intr['pmid']
    geneid = intr['geneids'][0]
    if sid not in pubs:
        pubs[sid] = True
        session.run("CREATE (a:Pub {id:'" + sid + "'})")
    if geneid not in genes:
        genes[geneid] = True
        session.run("CREATE (a:Gene {id:'" + geneid + "'})")


def save_intr(intr):
    sid = intr['pmid']
    geneid = intr['geneids'][0]
    q = "match (a:Pub), (b:Gene)" \
        " where a.id='" + sid +\
        "' AND b.id='" + geneid + "'"

    if q not in intrs:
        intrs[q] = 1
    else:
        c = intrs[q]
        intrs[q] = c + 1


def write_saved_intrs(session):
    for q in intrs:
        n = intrs[q]
        qr = q + " create (a)-[r:mentions {nmentions: %d }]->(b)  return r" % n
        if n > 1:
            print(qr)
        session.run(qr)


def indexwithneo4j():
    driver = GraphDatabase.driver("bolt://localhost",
                                  auth=basic_auth("neo4j", "nur"))
    session = driver.session()
    neo4j_delete_existing_records(session)
    read_and_index_pubtator_file(args.gene2pubfile, session, neo4j_index,
                                 'gene2pub')


if __name__ == '__main__':
    conf = {"host": "localhost", "port": 9200}
    try:
        d = os.path.dirname(os.path.abspath(__file__))
        conf = json.load(open(d + "/../conf/elasticsearch.json", "r"))
    finally:
        pass
    parser = argparse.ArgumentParser(
        description='Index NCBI PubTator files using Elasticsearch')
    parser.add_argument('--gene2pubfile',
                        default=d + "/../data/gene2pubtator.sample",
                        help='PubTator gene2pub file')
    parser.add_argument('--disease2pubfile',
                        default=d + "/../data/disease2pubtator.gz",
                        help='PubTator disease2pub file')
    parser.add_argument('--index',
                        default="pubtator",
                        help='name of the Elasticsearch index')
    parser.add_argument('--host', default=conf['host'],
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default=conf['port'],
                        help="Elasticsearch server port")
    args = parser.parse_args()
    con = Elasticsearch(host=args.host, port=args.port, timeout=3600)
    index(con)
