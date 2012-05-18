!# /usr/lib/exec python

# Pycassa library is used to communicate with cassandra. It can be installed as :
# sudo apt-get install python-pip
# sudo pip install pycassa 
# For more inforamtion visit
# http://pycassa.github.com/pycassa/installation.html


import logging
from pycassa.system_manager import *
from pycassa.pool import ConnectionPool
from pycassa import ColumnFamily
LOGFILENAME = 'dblayer.log'
HOST = 'locahost'
PORT = '9160'
KEYSPACE = 'dedup'
CHUNK_COL_FAMILY = 'chunk'
FILE_COL_FAMILY = 'files'

class dblayer:
	def __init__(self):
		''' setup a connection to cassandra '''
		logging.basic_config(filename = LOGFILENAME, format = '%(level)s %(asctime)s %(message)s')
		logging.info("inside dblayer:__init__ method")
		address = "%s:%s" % (HOST,PORT)
		try:
		     self.sysmgr = SystemManager(address)
	             self.pool = ConnectionPool(KEYSPACE, [address])
		     logging.info("Exiting dblayer:__init__ :  connection to cassandra successful")
		except Exception, e:
		     logging.error("Exiting dblayer with error %s" ,str(e))


	def add_chunk(self, key, value):
		''' method to add chunk to chunks columnfamily 
		    key<String> : MD5 hash of the chunk to be added
		    value       : bytes to be written as value of the chunk

		    Assumptions:
			This method assumes that the key doesn't exisits in keyspace.
		  		
                '''
		logging.info("dblayer:addchunk : enter with param key as %s", key)
		colfamily = ColumnFamily(self.pool,CHUNK_COL_FAMILY)
		row = key
		colname = "value"
		colval = value
		try:
			colfamily.insert(row,{colname:value, "recount":0})
			logging.info("dblayer:addchunk: Chunk successfully added " )
		except Exception, e:
			logging.error("exiting dblayer:addchunk with error %s ",e)
		

	def add_file_entry(self, filename, chunklist):
		''' method to add an entry to files columnfamily '''
		logging.info("dblayer:addfileentry  : entered with filename as %s" , filename)
		colfamily = ColumnFamily(self.pool, FILE_COL_FAMILY)
		chunknumbers = range(0, len(chunklist))
	        try:
			colfamily.insert(filename, {zip(chunknumbers, chunklist)}
			logging.info("dblayer:addfileentry : entry for file %s created ", filename)
		except Exception,e :
			logging.error("dblayer:addfileentry has errors %s ",str(e))
		


	def chunk_exists(self, key)
		''' chekcs if key exisits in the Chunk keyspace and returns true/false '''
		logging.info("inside dblayer:chunkexists to check chunk %", key)
		try:
		    #ToDO: add code to check if a chunk exists
		except, Exception,e:
		   logging.error("dblayer:chunkexists has failed with error %s", str(e))
		   throw(e)


	def update_chunk_ref(self, key):
		''' method to update the reference count of a exisitng chunk 
		   1. fetch the chunk
		   2. increment the refcount entry by 1
		   3. update the entry
		'''
		pass

	
