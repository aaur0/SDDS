#! /usr/lib/exec python

# Pycassa library is used to communicate with cassandra. It can be installed as :
# sudo apt-get install python-pip
# sudo pip install pycassa 
# For more inforamtion visit
# http://pycassa.github.com/pycassa/installation.html

#create keyspace minhash;
#use minhash;
#create column family chunks; # list of merged chunk ids and their data
#create column family filerecipe; # list of file names and their chunk ids
#create column family fullhash; # list of full hashes

#create keyspace files; 
#use files;
#create column family minhash; # one to one mapping of files and its minhash

import logging,sys
from pycassa.system_manager import *
from pycassa.pool import ConnectionPool
from pycassa import ColumnFamily
LOGFILENAME = 'dblayer.log'
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

class dblayer:
	def __init__(self):
		''' setup a connection to cassandra '''
		logging.basicConfig(filename = LOGFILENAME, level = logging.DEBUG, format = '%(asctime)s %(lineno)d %(module)s %(message)s')
		logging.info("inside dblayer:__init__ method")
		address = "%s:%s" % (HOST,PORT)
		try:
		     self.sysmgr = SystemManager(address)
	             
	             self.minhash_pool = ConnectionPool(MINHASH_KEYSPACE, [address])
	             self.minhash_chunks_cf = ColumnFamily(self.minhash_pool, MINHASH_CHUNKS_CF)
		     self.minhash_filerecipe_cf = ColumnFamily(self.minhash_pool, MINHASH_FILERECIPE_CF)
		     self.minhash_fullhash_cf = ColumnFamily(self.minhash_pool, MINHASH_FULLHASH_CF)
		     
	             self.files_pool = ConnectionPool(FILES_KEYSPACE, [address])
		     self.files_minhash_cf = ColumnFamily(self.files_pool, FILES_MINHASH_CF)
		     
		     logging.info("Exiting dblayer:__init__ :  connection to cassandra successful")
		     
		except Exception, e:
		     logging.error("Exiting dblayer with error %s" ,str(e))
		     raise e


	def add_chunk(self, minhash, chunk_hash, chunk_data):
		''' method to add chunk to chunks columnfamily 
		    key<String> : minhash - MD5 hash of the chunk to be added
		    value       : bytes to be written as value of the chunk

		    Assumptions:
			This method assumes that the key doesn't exisits in keyspace.
		  		
                '''
		logging.info("dblayer:addchunk : enter with param key as %s", str(key))
		colfamily = self.minhash_chunk_cf
		row = minhash
		colname = chunk_hash
		colval = chunk_data
		try:
			colfamily.insert(row,{colname:colval, "ref":"0"})
			logging.info("dblayer:addchunk: Chunk successfully added " )
		except Exception, e:
			logging.error("exiting dblayer:addchunk with error %s ", str(e))
		

	def add_file_entry(self, filename, chunklist):
		''' method to add an entry to files columnfamily '''
		logging.info("dblayer:addfileentry  : entered with filename as %s" , filename)
		colfamily = self.filecolfam
		chunknumbers = range(0, len(chunklist))
		chunkstr = list(map(str,chunknumbers))
		entries =  dict(zip(chunkstr, chunklist))
		logging.info("totalchunks = %s and totalchunklist = %s and entries = %s", len(chunkstr), len(chunklist), len(entries))
	        try:
			colfamily.insert(filename, entries)
			logging.info("dblayer:addfileentry : entry for file %s created ", filename)
		except Exception,e:
			logging.error("dblayer:addfileentry has errors %s ",str(e))
			sys.exit(1)	


	def chunk_exists(self, key):
		''' chekcs if key exisits in the Chunk keyspace and returns true/false '''
		logging.info("inside dblayer:chunkexists to check chunk %s", str(key))
		try:
		    colfamily = self.chunkcolfam
		    chunk = colfamily.get(key)
		    if None != chunk:
			return True
		    else:
			return False
		except Exception,e:
		   return False
	
	def file_exists(self,key):
		''' checks if an entry for file alread exists in DB '''
		logging.info("inside dblayer:file_exists method	with filename = %s", key)	
		try:
                    colfamily = self.filecolfam
                    chunk = colfamily.get(key)
                    if None != chunk:
                        return True
                    else:
                        return False
                except Exception,e:
                   return False


	def update_chunk_ref(self, key,value=1):
		 ''' method to update the reference count of a exisitng chunk 
		   1. fetch the chunk
		   2. increment the refcount entry by 1
		   3. update the entry
		 '''
		 try:
		 	logging.info("dblayer:update_chunk_ref invoked for key : %s", key)
			colfamily = self.chunkcolfam
	                chunk = colfamily.get(key)
        	        chunk['ref'] = str(int(chunk['ref'])+value)
                	colfamily.insert(key,chunk)
			logging.info("dblayer:update_chunk_ref successful")
		 except Exception,e:
			logging.error("dblayer:update_chunk_ref failed with error : %s", str(e))
			raise e

	
