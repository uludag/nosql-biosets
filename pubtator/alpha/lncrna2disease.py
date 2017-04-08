#!/usr/bin/env python
# Index PMC articles that have PubTator disease and gene annotations
""" workflow to get gene and disease annotated articles:

- Read NCBI gene IDs to a list (we generated this list together this afternoon, there is 2985 ENCODE lncRNAs that has NCBI gene ids)

- Read PubTator gene annotations into a map, skip annotations with genes not included in the above lncRNA list

- Read PubTator disease annotations into another map, skip annotations with publications not included in the above map

- Read list of PMC documents, index the document if it is in above maps
"""

import argparse
import gzip
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

# Document type name for the Elascticsearch index entries
doctype = 'gene2pub'
ChunkSize = 2*1024


#  Read NCBI gene IDs to a list
def read_gene_ids():
    infile = "../data/gencode.v25.lncRNAs.ens_and_ncbi_names.txt"
    f = open(infile, 'r')
    l = []
    f.readline()
    for line in f:
        a = line.strip('\n').split('\t')
        if len(a[1]) > 0:
            l.append(int(a[1]))
    return l


#  Read PubTator gene annotations into a map,
#  skip annotations with genes not included in the lncRNA list
def read_gene_annotations(lncrnas):
    infile = "./data/gene2pubtator.gz"
    f = gzip.open(infile, 'r')
    m = {}
    f.readline()
    for line in f:
        a = line.decode().strip('\n').split('\t')
        for gid in a[1].split(','):
            gid = int()
            if gid in lncrnas:
                m[a[0]] = gid
                print(a[0], gid)
    return m


# Read given PubTator file, index using the index function specified
def read_and_index_pubtator_file(infile, es, indexfunc):
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    r = indexfunc(es, f)
    return r


def parse_pub2gene_lines(f, r):
    for line in f:
        a = line.strip('\n').split('\t')
        if len(a) != 4:
            print("Line has more than 4 fields: %s" % line)
            exit(-1)
        if r > 0:  # skip header line
            geneids = a[1].replace(';', ',').split(',')
            yield {
                '_id': r,
                "pmid": a[0], "geneid": geneids,
                "mentions": a[2].split('|'),
                "resource": a[3].split('|')
            }
        r += 1


def es_index_links(es, f):
    r = 0
    for ok, result in streaming_bulk(
            es,
            parse_pub2gene_lines(f, r),
            index=args.index,
            doc_type='gene2pub',
            chunk_size=ChunkSize
    ):
        action, result = result.popitem()
        doc_id = '/%s/commits/%s' % (args.index, result['_id'])
        # process the information from ES whether the document has been
        # successfully indexed
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        # else:
        #    print(doc_id)
    return 1


def main(es, infile, index):
    # es.indices.delete(index=index, params={"timeout": "10s"})
    gids = read_gene_ids()
    print(len(gids))
    geneannots = read_gene_annotations(gids)
    print(len(geneannots))
    exit(0)
    iconfig = json.load(open("./mappings/pmc-articles.json", "rt"))
    es.indices.create(index=index, params={"timeout": "20s"},
                      ignore=400, body=iconfig, wait_for_active_shards=1)
    read_and_index_pubtator_file(infile, es, es_index_links)
    es.indices.refresh(index=index)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index NCBI PubTator files using Elasticsearch')
    parser.add_argument('--infile',
                        default="./data/gene2pubtator.sample",
                        help='input file to index')
    parser.add_argument('--index',
                        default="pubtator-lncrna2disease",
                        help='name of the Elasticsearch index')
    parser.add_argument('--host', default="localhost",
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default="9200",
                        help="Elasticsearch server port")
    args = parser.parse_args()
    host = args.host
    port = args.port
    con = Elasticsearch(host=host, port=port, timeout=3600)
    main(con, args.infile, args.index)
