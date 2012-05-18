!# /usr/bin/exec python

import logging
import md5,sys,os
from dblayer import *

LOG_FILE_NAME = 'vmdedup.log'
class chunker:
	''' This class all the code required for the chunking service. This service will be responsible for splitting of file into chunks and store them in cassandra '''
	def __init__():
		'''  
		    1. setup connection to cassandra
		    2. intialise logger object 
		 '''
		 try:
			 global LOG_FILE_NAME
			 logging.basic_config(filename =LOG_FILE_NAME, format='%(level)s %(asctime)s %(message)s')
			 self.db = dblayer()
		 except Exception,e:
			logging.error("chunker:__init__ failed with error %s", e)
			sys.exit(1)
		

	def chunkify(self,path):
		'''
			1. split the file into chunks	
			2. calculate MD5 hash of each chunk
			3. Check if the chunk already exists in the DB
				3.1 if yes: update the reference count
				3.2 else : create a new entry in DB
					3.2.1 add chunk to Chunk table
					3.2.2 add an entry in File Table

			Assumption:
				This method assumes that the filename passed is unique and doesn't exists in the DB
			
		'''		 	
		logging.info("chunker:Chunkify: creating chunks for the file :%s", path)
		try:
			chunklist = []
			if(not os.path.exists(path)):
			  logging.error("chunkify: chunker : invalied file specified")
			  sys.exit(1)
			with open(path, 'r') as f:
				chunk = f.read(40)
				while("" != chunk):
					key = self._getmd5(chunk)
					if(None == Key):
						 logging.error('chunker:chunkify failed with error : md5  hash was returned as None')
			                         sys.exit(1)
					if(db.chunk_exists(key)):
						logging.info("chunker:chunkify : calling update chunk reference")
						db.update_chunk_ref(key)
					else:
						logging.info("chunker:chunkify : calling add chunk method")
						db.add_chunk(key,chunk)
					chunklist.append(key)
			logging.info("chunker:chunkify : calling add file entry method")
			db.add_file_entry(path, chunklist)			
			logging.info("chunker:chunkify : successfully completed")		
			        	
		except Exception,e:
			logging.error('chunker:chunkify failed with error : %s', str(e))
			sys.exit(1)

	def _getmd5(self,chunk):
		''' returns MD5 of the chunk '''
		try:
			logging.info("chunker:_getmd5 method invoked")
			hasher = md5.new()
			hasher.update(chunk)
			logging.info("chunker:_getmd5 successful")
			return hasher.digest()
		except Exception,e:
			logging.error('chunker:_getmd5 : returned error %s',e)
			return None
		

	def _getchunk(self, fhandler, offset):
		''' given a offset creats a chunk of the file and returns it back '''
		pass
