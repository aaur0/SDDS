#! /usr/bin/exec python

import logging
import sys,os
from dblayer import *
from datetime import datetime
from sdds_constants import *

class metrics:
	''' This class contains functionalities to calculate important metrics in order to analyse the overall efficiency of the system.'''
	def __init__(self):
		try:
			self.db = dblayer()
		except Exception, e:
			logging.error("metrics:___init__ failed with error %s", e)
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

	
