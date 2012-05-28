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
from pycassa import NotFoundException
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
		colfamily = self.minhash_chunks_cf
		row = minhash
		colname = chunk_hash
		colval = chunk_data
		try:
			colfamily.insert(row,{colname:colval, "ref":"1"})
			logging.info("dblayer:addchunk: Chunk successfully added " )
		except Exception, e:
			logging.error("exiting dblayer:addchunk with error %s ", str(e))
		
	def add_fullhash(self, minhash, fullhash):
		''' method to add full hash entry in the fullhash col fam'''
		logging.info("dblayer:add_full_hash : min_hash : %s , fullhash : %s", minhash, fullhash)
		colfamily = self.minhash_fullhash_cf
		try:
			colfamily.insert(minhash, {fullhash:"-"})
			logging.info("dblayer:add_full_hash : Full_hash %s created for Min_Hash %s ", fullhash, minhash)
		except Exception, e:
			logging.info("dblayer:add_full_hash failed with error : %s", e)
			raise e
	
	def add_file_recipe(self, minhash, file_identifier, chunk_hash_list): 
        	colfamily = self.minhash_filerecipe_cf
        	dict1 = {}
        	for number, chunk_hash in enumerate(chunk_hash_list):
        		dict1[str(number)] = chunk_hash
        	colfamily.insert(minhash, {file_identifier: dict1})
        
	
	def add_file_entry(self, file_identifier, minhash):
		''' method to add an entry to files columnfamily '''
		logging.info("dblayer:addfileentry  : entered with filename as %s" , file_identifier)
		colfamily = self.files_minhash_cf
	        try:
			colfamily.insert(file_identifier, minhash)
			logging.info("dblayer:addfileentry : entry for fileid - minhash created ")
		except Exception,e:
			logging.error("dblayer:addfileentry has errors %s ",str(e))
			sys.exit(1)	


	def chunk_exists(self, minhash, chunk_hash):
		''' chekcs if key exisits in the Chunk keyspace and returns true/false '''
		logging.info("inside dblayer:chunkexists to check chunk %s", str(key))
		try:
		    colfamily = self.minhash_chunks_cf
		    chunk_list = colfamily.get(minhash)
		    val = chunk_list.has_key(chunk_hash)
		    return (None != val)
		except Exception,e:
		   return False
	
	def add_minhash(self, filename, filesize, minhash):
		''' method to add filename and minhash in files_minhash_cf '''
		logging.info("inside dblayer::add_min_hash method with filename = %s, minhash = %s", filename, minhash)
		colfamily =  self.files_minhash_cf
		dict1 = {}
		dict1["minhash"] = minhash
		dict1["filesize"] = str(filesize) 
		try:
			colfamily.insert(filename, dict1)
		except Exception, e:
			logging.error("dblayer: add_min_hash raised an error : %s", e)
			raise e
	
	def get_minhash(self, file_id):
		colfamily = self.files_minhash_cf
		try:
			logging.debug("colfamily.get(file_id) %s", colfamily.get(file_id)) 
			return colfamily.get(file_id)["minhash"]
		except Exception, e:
			logging.error("dblayer: get_minhash raised an error : %s", e)
			raise e	
		
	
	def is_file_exists(self, file_id):
		''' checks if an entry for file alread exists in DB '''
		logging.info("inside dblayer:file_exists method	with filename = %s", file_id)	
		try:
                    colfamily = self.files_minhash_cf
                    file = colfamily.get(file_id)
                    # chunk = colfamily.get(key)
                    # If the file_id exists, return minhash; otherwise return None
                    return file.keys()[0] if None != file else None            	    
                except Exception,e:
                   return None


	def get_chunk_list(self, minhash):
		colfamily = self.minhash_chunks_cf
	        chunk_list = colfamily.get(minhash)
	        return chunk_list
	
	
	def insert_chunk_list(self, minhash, chunk_map):
		try:	
			logging.debug("dblayer: insert_chunk_list")
			colfamily = self.minhash_chunks_cf
			try:
				db_chunk_map = colfamily.get(minhash)
				for chunk_hash in chunk_map.keys():
					if db_chunk_map.has_key( chunk_hash ):
                	                	value = chunk_map.get(chunk_hash)		
						db_chunk_map[chunk_hash]['ref'] = str(int(db_chunk_map[chunk_hash]['ref']) + value["ref_count"])
					else:
						db_chunk_map[chunk_hash]['data'] = value["data"]
						db_chunk_map[chunk_hash]['ref'] = value["ref_count"] 
			except NotFoundException, e:
				db_chunk_map = chunk_map
			colfamily.insert(minhash, db_chunk_map)
			logging.debug("chunk_map successfully added")
	        #	update_chunk_ref(this, minhash, chunk_hash,db_chunk_map,1)
		except Exception, e:
			logging.error('Error in dblayer:insert_chunk_list : %s', e)
			raise e
	        
	def delete_chunk_list(self, minhash, chunk_map):
		colfamily = self.minhash_chunks_cf
		db_chunk_map = colfamily.get(minhash)
		for chunk_hash in chunk_map.keys():
			if db_chunk_map.has_key( chunk_hash ):
                                value = chunk_map.get(chunk_hash)
                                db_chunk_map[chunk_hash]['ref'] -= value.ref_count
		        if chunk_list[chunk_hash]['ref'] == "0":
        	        	del chunk_list[chunk_hash]
	
	def update_chunk_ref(self, minhash, chunk_hash, db_chunk_map, value=1):
		 ''' method to update the reference count of a exisitng chunk 
		   1. fetch the chunk
		   2. increment the refcount entry by 1
		   3. update the entry
		 '''
		 try:
		 	logging.info("dblayer:update_chunk_ref invoked for key : %s", key)
	                chunk_list[chunk_hash]['ref'] = str(int(chunk['ref']) + value)
        	        if chunk_list[chunk_hash]['ref'] == "0":
        	        	del chunk_list[chunk_hash]
			logging.info("dblayer:update_chunk_ref successful")
		 except Exception,e:
			logging.error("dblayer:update_chunk_ref failed with error : %s", str(e))
			raise e

	
	def is_fullhash_exists(self, minhash, fullhash):
		''' method to check if there's already an exact copy of the file. 	
		    It's determined by comparing the wholehash.
	    	'''
	    	logging.info("dblayer: is_fullhash_exists")
	    	colfamily = self.minhash_fullhash_cf
	
		try:
	    		return colfamily.get(minhash).has_key(fullhash)
		except NotFoundException, e:
			logging.debug("new minhash")
			return False
	
	
	def get_file_data(self, minhash, file_id):
		''' method to re-assemble the chunks from the metadata associated with the given file. '''
		# First, get the chunk ids (hashes) from the filerecipe column family.
		logging.info("dblayer: get_file_data")
		try:
			filerecipe = self.minhash_filerecipe_cf
			db_chunk_map = filerecipe.get(minhash)[file_id]
			db_chunk_id_keys = range(0, len(db_chunk_map))
			
			
			#logging.debug('chunk_id_map: %s', chunk_id_map)
			chunk_data_list = []
			# Also, get the row (that has all the chunk data) corresponding to the minhash value in the minhash column family
			chunks_cf = self.minhash_chunks_cf 
			num_cols = chunks_cf.get_count(minhash) 
			logging.debug("Number of columns in chunks_cf %s", num_cols)
			minhash_row = chunks_cf.get(minhash, column_count=num_cols)
			#logging.debug('minhash_row: %s', minhash_row)
			# Then, for each of the chunk ids, get the chunk data and append it to the chunk_data_list.
			#logging.debug("db_chunk_map.values() %s", db_chunk_map.values())
			#logging.debug("minhash_row.keys() %s", minhash_row.keys())
			  
			for key in db_chunk_id_keys:
				#logging.debug("key %s", key)
				chunk_id = db_chunk_map[str(key)]
			        #logging.debug("minhash_row[key] %s", minhash_row[key]['data'])
				chunk_data_list.append(minhash_row[chunk_id]['data'])
			logging.debug("chunk_data_list obtained")
			return chunk_data_list
		except Exception, e:
			logging.error("Exception %s", str(e))
			return None

	def get_chunks_count(self):
		''' method to measure the efficiency of the system by calculating total disk space saved'''
		logging.info("dblayer: get_chunks_count")
		try:
			colfamily = self.minhash_chunks_cf	
			#logging.debug("colfamily.get_range() %s", colfamily.get_range())
			minhash_list = tuple(colfamily.get_range())
			#dir(minhash_list)	
			#logging.debug("testing %s", minhash_list[0][0])
			total_chunks = 0
			for row in minhash_list:
				#logging.debug("item %s", item[0])
				minhash = row[0]
				total_chunks += colfamily.get_count(minhash) 	
				#logging.debug("number of columns %s", colfamily.get_count(item[0]))
			#for minhash in minhash_list:
			#	total_chunks += colfamily.get_count(minhash)
			return total_chunks
		except Exception, e:
			logging.error("Exception %s", e)
			return 0

	def get_total_input_size(self):
		''' method to measure the input size '''
		logging.info("dblayer: get_total_input_size")
		try:
			colfamily = self.files_minhash_cf
                        files_list = tuple(colfamily.get_range())
			total_input_size = 0
			for row in files_list:
				file_id = row[0]
				total_input_size += int(colfamily.get(file_id)["filesize"])
			logging.debug("total_input_size %s", total_input_size)
			return total_input_size
		except Exception, e:
			logging.debug("Exception %s", e)
			return 0