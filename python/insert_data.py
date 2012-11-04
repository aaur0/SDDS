import os
from os.path import join
from chunker import *

rootdir='../../data/en'
chunkerobj = chunker()


for subdir, dirs, files in os.walk(rootdir):
    for file in files:
	path = join(subdir, file)
	print 'inserting ' +  path
	filesize = os.path.getsize(path)
	with open("size.txt", "a+") as f:
		f.write(path + "," + str(filesize) + "\n")  
	chunkerobj.chunkify(path)
	#os.system('python chunker.py -s -f ' + path)
