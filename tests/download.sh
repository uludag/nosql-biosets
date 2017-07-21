#!/usr/bin/env bash
# Script for downloading test datasets

# MetaNetX: 6 files, 460M
#wget -r ftp://ftp.vital-it.ch/databases/metanetx/MNXref/3.0/ --no-directories

# HMDB: 2 compressed xml files, 76M (v3.6)
wget http://www.hmdb.ca/system/downloads/current/hmdb_metabolites.zip
wget http://www.hmdb.ca/system/downloads/current/hmdb_proteins.zip
