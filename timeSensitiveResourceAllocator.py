import math

from inputReader import InputReader
from resourceAllocator import ResourceAllocator

class TimeSensitiveResourceAllocator(ResourceAllocator):

	'''
	Calculates OurMetric \eta_i
	TODO: change function name
	'''
	def calculateOurMetric(self, bids, runtimes, deadlines):
		denoms = [None] * self.nUser		# user x pool (= 1)

		#print bids

		for userID in self.U:
			reqPool = self.a[userID]
			denom = []
			for relPool in self.f:
				tmp = [x * y for x,y in zip(reqPool, relPool)]
				denom.append(sum(tmp))
				#denom.append(math.sqrt(sum(tmp)))

			# TODO: cross check - average or something else of scarcities
			#denoms[userID] = [sum(denom) / len(denom)]

			# taking the max of scarcities
			denoms[userID] = [max(denom)]

		#print 'denoms: ', denoms
		#print len(bids), len(bids[0])
		#print len(denoms), len(denoms[0])

		for userID in self.U:
			'''
			hardcoded 0 for just one 'time'
			'''
			# TODO:: zip won't work with different length lists, denoms[userID] has single value for scarcity
			temp = [x / ( y * runtimes[userID][0] * (deadlines[userID][0])) for x,y in zip(bids[userID], denoms[userID])]
			self.metric[userID] = temp

		#print 'eta metric: ', self.metric
		#print 'relPool: ', relPool


	def sortUser(self, deadlines):
		# TODO: hardcoded 0 for single bid / single scarcity
		poolId = 0	# TODO: reconsider variable name
		perPoolDensity = zip(*self.metric)
		currentDensity = list(perPoolDensity[poolId])

		self.U = [x for (x, y, z) in sorted(zip(self.U, currentDensity, deadlines), key=lambda x : (-x[1], x[2][0]))]

	def simulate(self, resourceCaps, bundles, vmResourceRequirement, runtimes, bids, deadlines):
		nUser = len(bundles)
		self.init(nUser)
		self.calculateRelevanceFactor(resourceCaps)
		self.calculateRequiredResource(bundles, vmResourceRequirement, resourceCaps)
		self.calculateOurMetric(bids, runtimes, deadlines)
		self.sortUser(deadlines)
		return self.allocate(resourceCaps, bids)



'''
To test the mechanism
'''
if __name__ == '__main__':
	reader = InputReader()
	bundles, runtimes, bids, deadlines = reader.readRequestFile()
	resourceCaps = reader.readCapacityFile()
	vmResourceRequirement = reader.readVMConfigFile()

	alloactor = TimeSensitiveResourceAllocator(len(bundles))
	alloactor.calculateRelevanceFactor(resourceCaps)
	alloactor.calculateRequiredResource(bundles, vmResourceRequirement)
	alloactor.calculateOurMetric(bids, runtimes, deadlines)
	alloactor.allocate(resourceCaps, bids)
