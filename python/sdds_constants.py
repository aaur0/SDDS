#! /usr/bin/exec python

import logging

LOG_FILE_NAME = 'vmdedup.log'
CHUNK_SIZE = 4 * 1024 # 4 kb size
HOST = 'localhost'
PORT = '9160'
# keyspaces
MINHASH_KEYSPACE = 'minhash'
FILES_KEYSPACE = 'files'
# column families of minhash keyspace
MINHASH_CHUNKS_CF = 'minhash_chunks'
MINHASH_FILERECIPE_CF = 'minhash_filerecipe'
MINHASH_FULLHASH_CF = 'minhash_fullhash'
# column families of files keyspace
FILES_MINHASH_CF = 'files_minhash'

logging.basicConfig(filename =LOG_FILE_NAME, format='%(asctime)s %(lineno)d %(module)s %(message)s', level =logging.DEBUG)
