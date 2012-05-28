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
			else:
			  logging.debug("file exists")
	
			if(self.db.is_file_exists(path)):
			  logging.error("chunkify: chunker :: an entry for file already exists in db")
			  sys.exit(1)
			else:
			  logging.debug("new file")				
			chunkmap = {}	
			filerecipe = []
			minhash = None
			fullhash = md5.new()
			key = ""
			start_time = datetime.now()
			file_size = os.path.getsize(path)
			logging.info("chunker:chunkify :: file shredding initiated for filesize :: %s (bytes) at time: %s", file_size, start_time) 
		        total_data_written = 0	
			with open(path, 'rb') as f:
				blocks_already_present = 0
				while(True):
					chunk = f.read(CHUNK_SIZE)
					#logging.info("chunker:chunkify :: a chunk of size %s  and type %s was read", str(len(chunk)), type(chunk))
					
					if("" == chunk):
						break
					key = self._getmd5(chunk)
					filerecipe.append(key)
					
					if(None == key):
						 logging.error('chunker:chunkify failed with error : md5  hash was returned as None')
			                         sys.exit(1)
					
					if(chunkmap.has_key(key)):
						value = chunkmap[key]
						value["ref_count"] = str( int(value["ref_count"]) + 1)
					else:
						value = {"data":chunk, "ref_count":"1"}
						chunkmap[key] = value
					if (None == minhash) or (key < minhash) :
						minhash = key
					fullhash.update(chunk)	
					#logging.info("chunker:chunkify :: a chunk of size %s  and type %s was read", str(len(chunk)), type(chunk))
                    
			fullhash = fullhash.hexdigest()
			
			# checking whether file exists in db or not 
			# already done
			# 
			logging.info("file chunking done ")
			logging.info("chunk list size %s", len(chunkmap))
			#logging.debug("first chunk while inserting %s", chunkmap.values()[0]["data"])
			
			if self.db.is_fullhash_exists(minhash, fullhash):
				logging.debug("there is an exact duplicate of the file")
				self.db.add_minhash(path, file_size, minhash)
				return
				
			self.db.add_minhash(path, file_size, minhash)
			self.db.add_file_recipe(minhash, path, filerecipe)
			self.db.add_fullhash(minhash, fullhash)
			self.db.insert_chunk_list(minhash, chunkmap)
						        	
		except Exception,e:
			print e
			logging.error('chunker:chunkify failed with error : %s', str(e))
			sys.exit(1)
	
	def get_file(self, file_absolute_path):
		if(self.db.is_file_exists(file_absolute_path) == False):
			logging.error("chunker : get_file : file not found in database")
			sys.exit(1)
		minhash = self.db.get_minhash(file_absolute_path)
		chunk_list = self.db.get_file_data(minhash, file_absolute_path)
		logging.debug("chunk_list size %s", len(chunk_list))
		f = open(file_absolute_path + "1", 'wb')
		new_fullhash = md5.new()
		#logging.debug("first chunk %s", chunk_list[0])
		for chunk in chunk_list:
			f.write(chunk)
			new_fullhash.update(chunk)
		new_fullhash = new_fullhash.hexdigest()
		logging.debug("fullhash after stitching the file %s", new_fullhash)
		if(self.db.is_fullhash_exists(minhash, new_fullhash) == False):
			logging.error("chunker : get_file : wrong full hash ")
			#sys.exit(1)
		logging.debug("file created successfully!")
		f.close()			
		f = open(file_absolute_path + "1", 'r')
		wholehash = self._getmd5(f.read())
		f.close()
	
	def _getmd5(self,chunk):
		''' returns MD5 of the chunk '''
		try:
			#logging.info("chunker:_getmd5 method invoked")
			hasher = md5.new()
			hasher.update(chunk)
			#logging.info("chunker:_getmd5 successful")
			return hasher.hexdigest()
		except Exception,e:
			logging.error('chunker:_getmd5 : returned error %s',e)
			return None
			
	
	def get_saved_space(self):
		''' gets saved space '''
		try:
			db_chunks_count = self.db.get_chunks_count()
			files_saved_size = db_chunks_count * CHUNK_SIZE 
			total_input_size = self.db.get_total_input_size()
			saved_space = total_input_size - files_saved_size
			logging.debug("Saved Space : %s Bytes", saved_space)
		except Exception, e:
			logging.error('error in the get_saved_space %s ', e)
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
		#chunkerobj.get_file(sys.argv[1])
		chunkerobj.get_saved_space()	