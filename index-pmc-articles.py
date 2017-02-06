#!/usr/bin/env python
from __future__ import print_function

import argparse
import gzip
import json
import os
import tarfile
import time

import pubmed_parser as pp
from elasticsearch import Elasticsearch


# Read PMC article xml files, index using the function indexf
# If the input file is a folder iterate over files in the folder
def read_and_index_pmc_articles(infile, es, indexf):
    print("Reading %s " % infile)
    n = 0
    t1 = time.time()
    if os.path.isdir(infile):
        for child in os.listdir(infile):
            c = os.path.join(infile, child)
            read_and_index_pmc_articles_file(c, es, indexf)
            n += 1
    else:
        if infile.endswith(".tar"):
            n = read_and_index_pmc_articles_tarfile(infile, es, indexf)
        else:
            read_and_index_pmc_articles_file(infile, es, indexf)
            n = 1
    t2 = time.time()
    print("-- %d files have been processed, in %dms"
          % (n, (t2 - t1) * 1000))
    return None


# Read given PMC tar file, index using the index function specified
def read_and_index_pmc_articles_tarfile(infile, es, indexfunc):
    print("\nProcessing tar file: %s " % infile)
    i = 0
    tar = tarfile.open(infile, 'r:')
    for member in tar:
        # aid = 'todo'
        # check whether the entry is already indexed
        if 1 == 1:  # not es.exists(index=args.index, doc_type='PMC_article', id=aid):
            f = tar.extractfile(member)
            if f is None:
                continue  # if the entry is folder then skip

            ar = pp.parse_pubmed_xml(f)
            f = tar.extractfile(member)
            try:
                paragraph_dicts = pp.parse_pubmed_paragraph(f)
                l = []
                for p in paragraph_dicts:
                    del (p['pmc'])
                    del (p['pmid'])
                    l.append(p)

                ar['paragraphs'] = l
                indexfunc(es, ar)
                # print("?", end='', flush=True)
                tar.members = []
                i += 1
            except Exception as e:
                print('Error parsing paragraphs: %s' % e)
                # exit(-1)
        else:
            print("-", end='', flush=True)
    return i


def pubmed_parser_parse(path_xml):
    pubmed_dict = pp.parse_pubmed_xml(path_xml)
    # pubmed_dict = pp.parse_pubmed_paragraph(path_xml)
    # print(json.dumps(pubmed_dict, indent=2))
    return pubmed_dict


# Read PM article file, index using the function indexf
def read_and_index_pmc_articles_file(infile_, es, indexf):
    infile = str(infile_)
    print("Reading %s " % infile)
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    ba = pubmed_parser_parse(f)
    r = indexf(es, ba)
    # todo: new function to avoid code duplicate
    return r


def es_index_article(es, ar):
    print(".", end='', flush=True)
    pmid = ar['pmid']
    try:
        es.index(index=args.index, doc_type='PMC_article', id=pmid,
                 body=json.dumps(ar))
    except Exception as e:
        print("error: %s" % e)
    return None


def main(es, infile, index):
    # es.indices.delete(index=index, params={"timeout": "10s"})
    iconfig = json.load(open("./mappings/pmc-articles.json", "rt"))
    es.indices.create(index=index, params={"timeout": "10s"},
                      ignore=400, body=iconfig)
    read_and_index_pmc_articles(infile, es, es_index_article)
    es.indices.refresh(index=index)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index Pubmed entries, using Elasticsearch')
    parser.add_argument('-infile', '--infile',
                        default="./data/PMC0044-1000.tar",
                        help='input file name or folder with PMC files')
    parser.add_argument('--index',
                        default="pmc-pubmed_parser-test",
                        help='name of the Elasticsearch index')
    parser.add_argument('--host', default="localhost",
                        help='Elasticsearch server hostname')
    parser.add_argument('--port', default="9200",
                        help="Elasticsearch server port")
    args = parser.parse_args()
    host = args.host
    port = args.port
    con = Elasticsearch(host=host, port=port, timeout=120)
    main(con, args.infile, args.index)
