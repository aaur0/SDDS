!# /usr/bin/exec python

import logging
import md5,sys
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
			with open(path, 'r') as f:
				chunk = f.read(40)
				while("" != chunk):
					key = self._getmd5(chunk)
					if(db.chunk_exists(key)):
						db.update_chunk_ref(key)
					else:
						db.add_chunk(key,chunk)
					chunklist.append(key)
			db.add_file_entry(path, chunklist)					
			        	
		except Exception,e:
			logging.error('chunker:chunkify failed with error : %s', str(e))
			sys.exit(1)

	def _getmd5(self,chunk):
		''' returns MD5 of the chunk '''
		try:
			hasher = md5.new()
			hasher.update(chunk)
			return hasher.digest()
		except Exception,e:
			logging.error('chunker:_getmd5 : returned error %s',e)
			return None
		

	def _getchunk(self, fhandler, offset):
		''' given a offset creats a chunk of the file and returns it back '''
		pass
