#! /usr/bin/exec python

import logging
import md5,sys,os
from dblayer import *
from datetime import datetime


LOG_FILE_NAME = 'vmdedup.log'
CHUNK_SIZE = 4 * 1024 # 4 kb size
class chunker:
	''' This class all the code required for the chunking service. This service will be responsible for splitting of file into chunks and store them in cassandra '''
	def __init__(self):
		 '''     1. setup connection to cassandra   2. intialise logger object   '''
	         try:
			 global LOG_FILE_NAME
			 logging.basicConfig(filename =LOG_FILE_NAME, format='%(asctime)s %(lineno)d %(module)s %(message)s', level =logging.DEBUG)
			 self.db = dblayer()
		 except Exception,e:
			logging.error("chunker:__init__ failed with error %s", e)
			sys.exit(1)
		
	def chunkify(self,path):
		'''
			0.1: check if entry for the file exists in DB
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
		logging.info("chunker:Chunkify: creating chunks for the file :%s",path)
		try:
			
			if(not os.path.exists(path)):
			  logging.error("chunkify: chunker : invalied file specified")
			  sys.exit(1)
			if(self.db.is_file_exists(path)):
			  logging.error("chunkify: chunker :: an entry for file already exists in db")
			  sys.exit(1)
			
			chunkmap = {}	
			filerecipe = []
			minhash = None
			fullhash = md5.new()
			
			start_time = datetime.now()
			file_size = os.path.getsize(path)
			logging.info("chunker:chunkify :: file shredding initiated for filesize :: %s (bytes) at time: %s", file_size, start_time) 
		        total_data_written = 0	
			with open(path, 'rb') as f:
				blocks_already_present = 0
                                logging.info("chunker:chunkify :: a chunk of size %s  and type %s was read", str(len(chunk)), type(chunk))
				while(True):
					chunk = f.read(CHUNK_SIZE)
					if("" == chunk):
						break
					key = self._getmd5(chunk)
					
					filerecipe.append(key)
					
					if(None == key):
						 logging.error('chunker:chunkify failed with error : md5  hash was returned as None')
			                         sys.exit(1)
					
					if(chunkmap.has_key(key)):
						value = chunkmap[key]
						value.ref_count = str(int(value.ref_count) + 1)
					else:
						value.data = chunk
						value.ref_count = "1"
						chunkmap[key] = value
					if (None == minhash) || (key < minhash) :
						minhash = key
					fullhash.update(key)	
					logging.info("chunker:chunkify :: a chunk of size %s  and type %s was read", str(len(chunk)), type(chunk))
                    
			fullhash = fullhash.hexdigest()
			
			# checking whether file exists in db or not 
			# already done
			# 
			logging.info("file chunking done ")
			self.db.add_minhash(path, minhash)
			self.db.add_file_recipe(minhash, path, filerecipe)
			self.db.add_fullhash(minhash, fullhash)
			self.db.insert_chunk_list(minhash, chunkmap)
						        	
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
			return hasher.hexdigest()
		except Exception,e:
			logging.error('chunker:_getmd5 : returned error %s',e)
			return None
			

	def _getchunk(self, fhandler, offset):
		''' given a offset creats a chunk of the file and returns it back '''
		pass

if __name__ == '__main__':
	if len(sys.argv) !=2:
		logging.error("invalid argumnets specified : chunkify <pathtothefile> ")
		sys.exit(1)
	else:
		chunkerobj = chunker()
		chunkerobj.chunkify(sys.argv[1])
		
