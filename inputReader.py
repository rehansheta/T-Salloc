import constants
import glob
import random

class InputReader:
	def __init__ (self):
		pass

	'''
	request format (<vm1_count, vm2_count, ...>, time, bid, deadline) => 2,1,1,1
	'''
	def readRequestFile(self):
		f = open(constants.requestFilename, 'r')
		lines = f.readlines()

		comment = lines[0]

		parts = lines[1].split(',')
		bundleSize = int(parts[0])
		timeSize = int(parts[1])
		bidSize = int(parts[2])
		deadlineSize = int(parts[3])

		bundles = []	# user x n_vm_type(=4) (multiple type vm requests are possible. for now only one can be non-zero)
		times = []		# user x 1
		bids = []		# user x 1 (multiple bids are possible)
		deadlines = []	# user x 1 (multiple deadlines are possible)

		#print bundleSize, timeSize, bidSize, deadlineSize

		nLines = len(lines)

		for i in xrange(2, nLines):

			if lines[i].startswith('#'): continue

			#print lines[i]
			parts = lines[i].split(',')
			bundle = []
			time = []
			bid = []
			deadline = []

			for j in xrange(0, bundleSize):
				bundle.append(int(parts[j]))
			for j in xrange(bundleSize, bundleSize + timeSize):
				time.append(int(parts[j]))
			for j in xrange(bundleSize + timeSize, bundleSize + timeSize + bidSize):
				bid.append(float(parts[j]))
			for j in xrange(bundleSize + timeSize + bidSize, bundleSize + timeSize + bidSize + deadlineSize):
				deadline.append(float(parts[j]))

			bundles.append(bundle)
			times.append(time)
			bids.append(bid)
			deadlines.append(deadline)

		#print bundles
		#print times
		#print bids
		#print deadlines

		return bundles, times, bids, deadlines

	'''
	 title format (type count, resource count) => 2,2
	'''
	def readVMConfigFile(self):
		f = open(constants.vmConfigFilename, 'r')
		lines = f.readlines()

		comment = lines[0]

		nResource = int(lines[1])
		vmResourceRequirement = []	# nVM x nResource
		nLines = len(lines)	# = nVM

		for i in xrange(2, nLines):

			if lines[i].startswith('#'): continue

			#print lines[i]
			parts = lines[i].split(',')
			vmResourceReq = []
			for j in xrange(0, nResource):
				vmResourceReq.append(float(parts[j]))

			vmResourceRequirement.append(vmResourceReq)

		#print vmResourceRequirement

		return vmResourceRequirement


	'''
	title format (pool count, resource count) => 1,2
	'''
	def readCapacityFile(self):
		f = open(constants.capacityFilename, 'r')
		lines = f.readlines()

		comment = lines[0]

		nResource = int(lines[1])
		resourceCaps = []		# pool x resourceCount
		nLines = len(lines)		# = nPool

		for i in xrange(2, nLines):

			if lines[i].startswith('#'): continue

			#print lines[i]
			parts = lines[i].split(',')
			resourceCap = []
			for j in xrange(0, nResource):
				resourceCap.append(float(parts[j]))

			resourceCaps.append(resourceCap)

		#print resourceCaps
		return resourceCaps

	def readAppInfo(self):
		apps = []

		for benchmark in constants.benchmarks:
			inputDir = constants.inputLogFileDirectory + benchmark + '/'

			files = glob.glob(inputDir + '*.parsed')
			for file in files:

				if '.small.' in file: vmType = constants.VM_TYPES.small
				elif '.medium.' in file: vmType = constants.VM_TYPES.medium
				elif '.large.' in file: vmType = constants.VM_TYPES.large
				elif '.xlarge.' in file: vmType = constants.VM_TYPES.xlarge

				with open(file, 'r') as openfile:
					for lines in openfile:

						if lines.startswith('#'): continue

						parts = lines.split()
						appName = parts[0]
						runtime = float(parts[1])

						# special exclusion of apps with low runtime and error in execution
						if runtime < 1.0: continue
						elif (appName in ['splash2x.barnes', 'splash2x.fmm', 'splash2x.radix']) and vmType is constants.VM_TYPES.medium:	continue
						elif (appName in ['splash2x.fft', 'splash2x.ocean_ncp']) and vmType is constants.VM_TYPES.large:	continue

						vmTypes = []
						vmCounts = []
						runtimes = []
						bids = []
						deadlines = []

						'''
						if multiple bids, deadlines etc. is evident we need to use loop version to append in collection.
						make seperate requests for each bid, deadline pair
						'''
						if (vmType is constants.VM_TYPES.small):
							vmTypes.append(vmType)
							vmCounts.append(1.0)				# hard coded 1 for single type vm request
							runtimes.append(runtime)
							bids.append(float(random.randint(1, 5)))
							deadlines.append(runtime * float(random.randint(2, 4)))

							apps.append((appName, vmTypes[0], vmCounts[0], runtimes[0], bids[0], deadlines[0]))

						elif (vmType is constants.VM_TYPES.medium):
							vmTypes.append(vmType)
							vmCounts.append(1.0)
							runtimes.append(runtime)
							bids.append(float(random.randint(5, 10)))
							deadlines.append(runtime * float(random.randint(2, 4)))

							apps.append((appName, vmTypes[0], vmCounts[0], runtimes[0], bids[0], deadlines[0]))

						elif (vmType is constants.VM_TYPES.large):
							vmTypes.append(vmType)
							vmCounts.append(1.0)
							runtimes.append(runtime)
							bids.append(float(random.randint(10, 15)))
							deadlines.append(runtime * float(random.randint(2, 4)))

							apps.append((appName, vmTypes[0], vmCounts[0], runtimes[0], bids[0], deadlines[0]))

						elif (vmType is constants.VM_TYPES.xlarge):
							vmTypes.append(vmType)
							vmCounts.append(1.0)
							runtimes.append(runtime)
							bids.append(float(random.randint(15, 20)))
							deadlines.append(runtime * float(random.randint(2, 4)))

							apps.append((appName, vmTypes[0], vmCounts[0], runtimes[0], bids[0], deadlines[0]))

		return apps


if __name__ == '__main__':
	myReader = InputReader()

	#bundles, times, bids, deadlines = myReader.readRequestFile()
	resourceCaps = myReader.readCapacityFile()
	#vmResourceRequirement = myReader.readVMConfigFile()
	#print bundles, '\n', times, '\n', bids, '\n', deadlines
	#print resourceCaps, '\n', vmResourceRequirement

	myReader.readAppInfo()
