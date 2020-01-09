#!/usr/bin/env python
""" Index PMC articles with Elasticsearch or MongoDB"""
import argparse
import datetime
import gzip
import os
import tarfile
import time

import pubmed_parser as pp

from nosqlbiosets.dbutils import DBconnection, dbargs
from nosqlbiosets.objutils import num

SOURCEURL = "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/*.xml.tar.gz"
d = os.path.dirname(os.path.abspath(__file__))


# Read PMC article xml files;
# If the input file is a folder iterate over files in the folder
def read_and_index_pmc_articles(infile, dbc):
    n = 0
    t1 = time.time()
    if os.path.isdir(infile):
        for child in os.listdir(infile):
            c = os.path.join(infile, child)
            read_and_index_pmc_articles(c, dbc)
            n += 1
    else:
        if infile.endswith(".tar") or infile.endswith(".tar.gz"):
            n = read_and_index_pmc_articles_tarfile(infile, dbc)
        else:
            read_and_index_pmc_articles_file(infile, dbc)
            n = 1
    t2 = time.time()
    print("-- %d files have been processed, in %dms"
          % (n, (t2 - t1) * 1000))
    return None


def pubmed_parser(path_xml):
    ar = pp.parse_pubmed_xml(path_xml)
    path_xml.seek(0)
    paragraph_dicts = pp.parse_pubmed_paragraph(path_xml)
    paragraphs = []
    for p in paragraph_dicts:
        del (p['pmc'])
        del (p['pmid'])
        paragraphs.append(p)
    ar['paragraphs'] = paragraphs
    num(ar, 'publication_year')
    ar['publication_date'] = datetime.datetime.strptime(
        ar['publication_date'], "%d-%m-%Y")
    return ar


def xmltodict_parser_parse(path_xml):
    import xmltodict
    namespaces = {
        'http://www.w3.org/1999/xlink': None,
        'http://www.w3.org/1998/Math/MathML': None
    }
    ar = xmltodict.parse(path_xml, process_namespaces=True,
                         namespaces=namespaces, attr_prefix='')['article']
    # Delete attributes we could not handle
    if 'glossary' in ar['back']:
        del ar['back']['glossary']
    del ar['back']['ref-list']
    return ar


PARSER = pubmed_parser


# Read given PMC tar file
def read_and_index_pmc_articles_tarfile(infile, dbc):
    print("\nProcessing tar file: %s " % infile)
    i = 0
    tar = tarfile.open(infile, 'r:gz')
    for member in tar:
        f = tar.extractfile(member)
        if f is None:
            continue  # if the tar-file entry is folder then skip
        ar = PARSER(f)
        index_article(dbc, ar)
        tar.members = []
        i += 1
    return i


# Read PMC articles file, index
def read_and_index_pmc_articles_file(infile_, dbc):
    infile = str(infile_)
    print("Reading %s " % infile)
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rb')
    else:
        f = open(infile, 'rb')
    ba = PARSER(f)
    index_article(dbc, ba)


def index_article(dbc, ar):
    num(ar, 'pmid')
    if PARSER == pubmed_parser:
        pmcid = num(ar, 'pmc')
    else:
        pmcid = ar['front']['article-meta']['article-id'][0]['#text']
    try:
        if dbc.db == "Elasticsearch":
            dbc.es.index(index=dbc.index, id=pmcid, body=ar)
        else:  # MongoDB
            spec = {"_id": pmcid}
            dbc.mdbi[dbc.mdbcollection].update(spec, ar, upsert=True)
    except Exception as e:
        print("error: %s" % e)
    return None


def main(infile, db, index, **kwargs):
    dbc = DBconnection(db, index, **kwargs)
    read_and_index_pmc_articles(infile, dbc)
    dbc.close()


if __name__ == '__main__':
    args = argparse.ArgumentParser(
        description='Index PMC XML document files (articles with full-text)'
                    ' with Elasticsearch,'
                    ' or MongoDB, downloaded from ' + SOURCEURL)
    args.add_argument('-infile', '--infile',
                      help='PMC XML document file,'
                           ' such as Biotechnol_Lett/PMC6828833.nxml'
                           ' or input folder with the XML document files')
    dbargs(args)
    args = args.parse_args()
    main(args.infile, args.dbtype, args.esindex, host=args.host, port=args.port)
