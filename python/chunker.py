!# /usr/bin/exec python

import logging


LOG_FILE_NAME = 'vmdedup.log'
class chunker:
	''' This class all the code required for the chunking service. This service will be responsible for splitting of file into chunks and store them in cassandra '''
	def __init__():
		'''  
		    1. setup connection to cassandra
		    2. intialise logger object 
		 '''
		 global LOG_FILE_NAME
		 logging.basic_config(filename =LOG_FILE_NAME, format='%(level)s %(asctime)s %(message)s')
		 	 
