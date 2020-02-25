#!/usr/bin/env python
""" Index PMC articles with Elasticsearch or MongoDB"""
import argparse
import datetime
import gzip
import os
import tarfile
import time
from multiprocessing.pool import ThreadPool

import pubmed_parser as pp

from nosqlbiosets.dbutils import DBconnection, dbargs
from nosqlbiosets.objutils import num

SOURCEURL = "ftp://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/*.xml.tar.gz"
d = os.path.dirname(os.path.abspath(__file__))

pool = ThreadPool(14)  # Threads for parser/index calls
MAX_QUEUED_JOBS = 140  # Maximum number of jobs in queue


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
    if not isinstance(path_xml, str):
        path_xml.seek(0)
    paragraph_dicts = pp.parse_pubmed_paragraph(path_xml)
    paragraphs = []
    for p in paragraph_dicts:
        del (p['pmc'])
        del (p['pmid'])
        paragraphs.append(p)
    ar['paragraphs'] = paragraphs
    num(ar, 'publication_year')
    try:
        ar['publication_date'] = datetime.datetime.strptime(
            ar['publication_date'], "%d-%m-%Y")
    except ValueError:
        try:
            print(ar['publication_date'])
            # assume error in 'day' and retry with the first day of the month
            ar['publication_date'] = datetime.datetime.strptime(
                "01"+ar['publication_date'][2:], "%d-%m-%Y")
        except ValueError:
            # a workaround, until we have a robust parser
            ar['publication_date'] = datetime.datetime(2000, 1, 1)
    return ar


def parse_index(xml, dbc):
    ar = pubmed_parser(xml.decode())
    del xml
    index_article(dbc, ar)


# Read given PMC tar file
def read_and_index_pmc_articles_tarfile(infile, dbc):
    import gc
    print("\nProcessing tar file: %s " % infile)
    i = 0
    tar = tarfile.open(infile, 'r%s' % ':gz' if infile.endswith('.gz') else ':')
    for member in tar:
        f = tar.extractfile(member)
        if f is None:
            continue  # if the tar-file entry is folder then skip
        xml = f.read()
        pool.apply_async(parse_index, (xml, dbc))
        del xml
        f.close()
        tar.members = []
        r = gc.collect()
        if r > 0:
            print(r)
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
    ba = pubmed_parser(f)
    index_article(dbc, ba)


def index_article(dbc, ar):
    num(ar, 'pmid')
    pmcid = num(ar, 'pmc')
    try:
        if dbc.db == "Elasticsearch":
            dbc.es.index(index=dbc.index, id=pmcid, body=ar)
        else:  # MongoDB
            spec = {"_id": pmcid}
            dbc.mdbi[dbc.mdbcollection].update(spec, ar, upsert=True)
    except Exception as e:
        print("error: %s" % e)
    del ar


def main(infile, db, index, **kwargs):
    esindxcfg = {  # Elasticsearch index configuration
        "index.number_of_replicas": 0,
        "index.number_of_shards": 5}
    dbc = DBconnection(db, index, es_indexsettings=esindxcfg, **kwargs)
    read_and_index_pmc_articles(infile, dbc)
    pool.close()
    pool.join()
    pool.terminate()
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
