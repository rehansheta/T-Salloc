import glob
import os

import constants

class RequestGenerator:
	def __init__ (self, traceFileDirectory):
		self.traceFileDirectory = traceFileDirectory

	def readTraceFile(self):
		'''
		Collects all the files in the trace directory
		'''
		files = glob.glob(self.traceFileDirectory + '*')
		for file in files:
			if os.path.isdir(file): print os.path.basename(file) + '/'	# we may need to recursively print from directories
			else: print os.path.basename(file)


if __name__ == '__main__':
	traceFileDirectory = 'inputs/'
	rg = RequestGenerator(traceFileDirectory)
	rg.readTraceFile()
