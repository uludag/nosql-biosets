#!/usr/bin/env python
# Index HGNC gene info files with Elasticsearch, MongoDB or PostgreSQL

import argparse
import gzip
import json
from pprint import pprint

from elasticsearch.helpers import streaming_bulk
from nosqlbiosets.dbutils import DBconnection
from pymongo.errors import BulkWriteError
from sqlalchemy import (Column, Integer, Date, create_engine)
from sqlalchemy import Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import ARRAY

# Default index name with Elascticsearch,
# database name with MongoDB and PostgreSQL
INDEX = "geneinfo"

# Default document type name for Elascticsearch index entries,
# collection name with MongoDB, and table name with PostgreSQL
DOCTYPE = 'hgncgeneinfo'

CHUNKSIZE = 64
SOURCEURL = "http://ftp.ebi.ac.uk/pub/databases/genenames/" \
            "new/json/hgnc_complete_set.json"


# Read HGNC gene info file, index using the index function specified
def read_and_index_hgnc_file(infile, dbc, indexfunc):
    if infile.endswith(".gz"):
        f = gzip.open(infile, 'rt')
    else:
        f = open(infile, 'r')
    genesinfo = json.load(f)
    r = indexfunc(dbc, genesinfo["response"])
    return r


# HGNC gene attributes information:
# https://www.genenames.org/help/statistics-downloads
def read_genes(l):
    for gene in l['docs']:
        # Following attributes are ignored until we implemented support
        for attr in ['pseudogene.org', "homeodb", "kznf_gene_catalog",
                     "intermediate_filament_db", "bioparadigms_slc",
                     "mamit-trnadb", "horde_id", "snornabase"]:
            if attr in gene:
                del(gene[attr])
        gene["_id"] = int(gene["hgnc_id"][5:])  # skip prefix "HGNC:"
        if "iuphar" in gene:
            gene["iuphar"] = int(gene["iuphar"][9:])  # skip prefix "objectId:"
        del gene["uuid"], gene["_version_"]
        gene["_id"] = int(gene["hgnc_id"][5:])  # skip prefix "HGNC:"
        if "entrez_id" in gene:
            gene["entrez_id"] = int(gene["entrez_id"])
        yield gene


def es_index_genes(dbc, genes):
    r = 0
    for ok, result in streaming_bulk(
            dbc.es,
            read_genes(genes),
            index=dbc.index,
            doc_type='_doc',
            chunk_size=CHUNKSIZE
    ):
        action, result = result.popitem()
        doc_id = '/%s/commits/%s' % (dbc.index, result['_id'])
        if not ok:
            print('Failed to %s document %s: %r' % (action, doc_id, result))
        else:
            r += 1
    return r


def mongodb_index_genes(mdbc, genes):
    entries = list()
    try:
        for entry in read_genes(genes):
            entries.append(entry)
            if len(entries) == CHUNKSIZE:
                mdbc.insert_many(entries)
                entries = list()
        mdbc.insert_many(entries)
    except BulkWriteError as bwe:
        pprint(bwe.details)
    return


Base = declarative_base()


class GeneInfo(Base):
    __tablename__ = DOCTYPE
    _id = Column(Integer, primary_key=True)
    name = Column(Text)
    symbol = Column(Text)
    prev_name = Column(ARRAY(Text))
    prev_symbol = Column(ARRAY(Text))
    alias_name = Column(ARRAY(Text))
    alias_symbol = Column(ARRAY(Text))
    gene_family = Column(ARRAY(Text))
    locus_type = Column(Text)
    locus_group = Column(Text)
    location = Column(Text)
    location_sortable = Column(Text)
    status = Column(Text)
    date_modified = Column(Date)
    date_approved_reserved = Column(Date)
    date_symbol_changed = Column(Date)
    date_name_changed = Column(Date)
    ccds_id = Column(ARRAY(Text))
    cd = Column(Text)
    cosmic = Column(Text)
    ena = Column(ARRAY(Text))
    ensembl_gene_id = Column(Text)
    entrez_id = Column(Integer)
    enzyme_id = Column(ARRAY(Text))
    gene_family_id = Column(Text)
    gene_group = Column(ARRAY(Text))
    gene_group_id = Column(ARRAY(Integer))
    gtrnadb = Column(Text)
    hgnc_id = Column(Text)
    imgt = Column(Text)
    iuphar = Column(Text)
    lncipedia = Column(Text)
    lncrnadb = Column(Text)
    lsdb = Column(ARRAY(Text))
    merops = Column(Text)
    mgd_id = Column(Text)
    mirbase = Column(Text)
    omim_id = Column(Text)
    orphanet = Column(Text)
    pubmed_id = Column(Text)
    refseq_accession = Column(Text)
    rgd_id = Column(Text)
    rna_central_id = Column(ARRAY(Text))
    ucsc_id = Column(Text)
    uniprot_ids = Column(ARRAY(Text))
    vega_id = Column(Text)


def pgsql_connect(host, port, user, password, db=INDEX):
    if port is None:
        port = 5432
    if host is None:
        host = 'localhost'
    url = 'postgresql://{}:{}@{}:{}/{}'
    url = url.format(user, password, host, port, db)
    con = create_engine(url, client_encoding='utf8', echo=False)
    Base.metadata.drop_all(con)
    Base.metadata.create_all(con)
    sm = sessionmaker(con)
    session = sm()
    return session


def pgsql_index_genes(session, genes):
    entries = list()
    for entry in read_genes(genes):
        entries.append(GeneInfo(**entry))
        if len(entries) == CHUNKSIZE:
            session.bulk_save_objects(entries)
            session.commit()
            entries = list()
    session.bulk_save_objects(entries)
    session.commit()


def main(db, infile, index, doctype,
         user=None, password=None, host=None, port=None):
    if db in ["Elasticsearch",  "MongoDB"]:
        dbc = DBconnection(db, index, collection=doctype, host=host, port=port,
                           recreateindex=True)
        if dbc.db == "Elasticsearch":
            read_and_index_hgnc_file(infile, dbc, es_index_genes)
            dbc.es.indices.refresh(index=index)
        elif dbc.db == "MongoDB":
            read_and_index_hgnc_file(infile, dbc.mdbi[doctype],
                                     mongodb_index_genes)
    else:
        session = pgsql_connect(host, port, user, password, index)
        session.query(GeneInfo).delete()
        read_and_index_hgnc_file(infile, session, pgsql_index_genes)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Index HGNC gene-info json file using Elasticsearch, '
                    'MongoDB or PostgreSQL, downloaded from ' + SOURCEURL)
    parser.add_argument('--infile',
                        required=True,
                        help='Input HGNC file to index')
    parser.add_argument('--index', default=INDEX,
                        help='Index name for Elasticsearch, '
                             'database name for MongoDB and PostgreSQL')
    parser.add_argument('--doctype', default=DOCTYPE,
                        help='Collection name for MongoDB, '
                             'table name for PostgreSQL')
    parser.add_argument('--host',
                        help='Hostname for the database server')
    parser.add_argument('--port',
                        help="Port number of the database server")
    parser.add_argument('--db', default='PostgreSQL',
                        help="Database: 'Elasticsearch', 'MongoDB',"
                             " or 'PostgreSQL'")
    parser.add_argument('--user',
                        help="Database user name, "
                             "supported with PostgreSQL option only")
    parser.add_argument('--password',
                        help="Password for the database user, "
                             " supported with PostgreSQL option only")
    args = parser.parse_args()
    main(args.db, args.infile, args.index, args.doctype,
         args.user, args.password, args.host, args.port)
