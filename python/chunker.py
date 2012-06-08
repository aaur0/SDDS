#! /usr/bin/exec python

import logging
import hashlib
import sys,os
import time
from dblayer import *
from metrics import *
from datetime import datetime
from sdds_constants import *
from optparse import OptionParser
from time import strftime

class chunker:
	''' This class contains all the code required for the chunking service. This service will be responsible for splitting of file into chunks and storing them in cassandra. '''
	def __init__(self):
		 '''     1. setup connection to cassandra   2. intialise logger object   '''
	         try:
			 self.db = dblayer()
			 self.metricsObj = metrics()			
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
				This method assumes that the filename passed is unique and doesn't exist in the DB
			
		'''		 	
		logging.info("chunker:Chunkify: creating chunks for the file :%s",path)
		start_time = time.time()
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
			fullhash = hashlib.md5()
			key = ""
			file_size = os.path.getsize(path)
			logging.info("chunker:chunkify :: file shredding initiated for filesize :: %s (bytes) at time: %s", file_size, datetime.now()) 
		        total_data_written = 0	
			with open(path, 'rb') as f:
				blocks_already_present = 0
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
						value["ref_count"] = str( int(value["ref_count"]) + 1)
						blocks_already_present = blocks_already_present + 1
					else:
						value = {"data":chunk, "ref_count":"1"}
						chunkmap[key] = value
					if (None == minhash) or (key < minhash) :
						minhash = key
					fullhash.update(chunk)	
			fullhash = fullhash.hexdigest()
			
			# checking whether file exists in db or not 
			# already done
			# 
			logging.info("%s file chunking done ", path)
			logging.info("chunk list size %s", len(chunkmap))
			
			if self.db.is_fullhash_exists(minhash, fullhash):
				logging.debug("there is an exact duplicate of the file %s", path)
				self.db.add_minhash(path, file_size, minhash)
				return
				
			self.db.add_minhash(path, file_size, minhash)
			logging.info("%s - minhash added", path)
			self.db.add_file_recipe(minhash, path, filerecipe)
			logging.info("%s - file recipe added", path)
			self.db.add_fullhash(minhash, fullhash)
			logging.info("%s - fullhash added", path)
			self.db.insert_chunk_list(minhash, chunkmap)
			logging.info("%s - chunk list added ", path)
			logging.info("time taken for chunking and indexing file %s [file size : %s bytes] - %f seconds", path, file_size, (time.time() - start_time) )		        	
			logging.info("Number of chunks already present in the system : %s",blocks_already_present)
			logging.info("Total Space saved for the file = %s Kb",  blocks_already_present * 4)
		except Exception,e:
			print e
			logging.error('chunker:chunkify failed with error : %s', str(e))
			sys.exit(1)
	
	def get_file(self, file_absolute_path, file_new_absolute_path):
		''' Gets the specified file from the storage '''
		start_time = time.time()
		try:
			if(self.db.is_file_exists(file_absolute_path) == False):
				logging.error("chunker : get_file : file not found in database")
				sys.exit(1)
			minhash = self.db.get_minhash(file_absolute_path)
			chunk_list = self.db.get_file_data(minhash, file_absolute_path)
			logging.debug("%s chunk_list size %s", file_absolute_path, len(chunk_list))
			f = open(file_new_absolute_path, 'wb')
			new_fullhash = hashlib.md5()
			for chunk in chunk_list:
				f.write(chunk)
				new_fullhash.update(chunk)
			new_fullhash = new_fullhash.hexdigest()
			logging.debug("%s fullhash after stitching the file %s", file_absolute_path, new_fullhash)
			if(self.db.is_fullhash_exists(minhash, new_fullhash) == False):
				logging.error("chunker : get_file : wrong full hash ")
			logging.debug("%s file created successfully ", file_new_absolute_path)
			f.close()
			logging.info("time taken for retrieving file %s - %f seconds", file_new_absolute_path, (time.time() - start_time))
		except Exception,e:
			print e
			logging.error('chunker:get_file failed with error : %s', str(e))
			sys.exit(1)     	

	
	def _getmd5(self,chunk):
		''' returns MD5 of the chunk '''
		try:
			hasher = hashlib.md5()
			hasher.update(chunk)
			return hasher.hexdigest()
		except Exception,e:
			logging.error('chunker:_getmd5 : returned error %s',e)
			return None
			
	def _getchunk(self, fhandler, offset):
		''' given a offset creats a chunk of the file and returns it back '''
		pass

	def getMetric(self):
		''' Gets the efficiency of the system in terms of space saved'''
		self.metricsObj.get_saved_space()

if __name__ == '__main__':

	parser = OptionParser()
	parser.add_option("-f", "--file", dest="filename")
	parser.add_option("-s", "--set", action="store_true", dest="set")
	parser.add_option("-g", "--get", action="store_true", dest="get")
	parser.add_option("-t", "--stat", action="store_true", dest="stat")

	(options, args) = parser.parse_args()
	chunkerobj = chunker()

	if options.stat:
		chunkerobj.getMetric()
	elif options.set:
		if None != options.filename:
			chunkerobj.chunkify(options.filename)
		else:
			print "enter file name to backup"
	elif options.get:
		if None != options.filename:
			fname, fext = os.path.splitext(options.filename)
			chunkerobj.get_file(options.filename, fname + "_" + strftime("%Y-%m-%d_%H-%M-%S") + fext)
		else:
			print "enter file name to backup"
	else:
		print " <<<<<<<<<<<<< Backup client >>>>>>>>>>>>>>>>>"
		print " Help " 
		print "\t -s to store a file "
		print "\t -g to retrieve a file "
		print "\t -f to specify the file name u want to store or retrieve "
		print "\t -t to get the stats "
	
		
