import math

from inputReader import InputReader
from resourceAllocator import ResourceAllocator

class LenaGReasourceAllocator(ResourceAllocator):

	'''
	Calculates DensityMetric d_i
	'''
	def calculateDensityMetric(self, bids):
		denoms = [None] * self.nUser		# user x pool (= 1)

		#print 'bids: ', bids
		#print 'U:', self.U
		#print 'a:', self.a

		for userID in self.U:
			reqPool = self.a[userID]
			denom = []
			for relPool in self.f:
				tmp = [x * y for x,y in zip(reqPool, relPool)]
				denom.append(math.sqrt(sum(tmp)))

			# TODO: cross check - average or something else of scarcities
			denoms[userID] = [sum(denom) / len(denom)]

		#print 'denoms: ', denoms
		#print len(bids), len(bids[0])
		#print len(denoms), len(denoms[0])

		for userID in self.U:
			# TODO:: zip won't work with different length lists, denoms[userID] has single value for scarcity
			temp = [x / y for x,y in zip(bids[userID], denoms[userID])]
			self.metric[userID] = temp

		#print 'density metric: ', self.metric

	def sortUser(self):
		# TODO: hardcoded 0 for single bid / single scarcity
		poolId = 0	# TODO: reconsider variable name
		perPoolDensity = zip(*self.metric)
		currentDensity = list(perPoolDensity[poolId])

		self.U = [x for (x, y) in sorted(zip(self.U, currentDensity), key=lambda x : (-x[1], x[0]))]

	def simulate(self, resourceCaps, bundles, vmResourceRequirement, runtimes, bids, deadlines):
		# runtimes is None
		# deadlines is None
		nUser = len(bundles)
		self.init(nUser)
		self.calculateRelevanceFactor(resourceCaps)
		self.calculateRequiredResource(bundles, vmResourceRequirement, resourceCaps)
		self.calculateDensityMetric(bids)
		self.sortUser()
		return self.allocate(resourceCaps, bids)


'''
To test the mechanism
'''
if __name__ == '__main__':
	reader = InputReader()
	bundles, times, bids, deadlines = reader.readRequestFile()
	resourceCaps = reader.readCapacityFile()
	vmResourceRequirement = reader.readVMConfigFile()

	alloactor = LenaGReasourceAllocator(len(bundles))
	alloactor.calculateRelevanceFactor(resourceCaps)
	alloactor.calculateRequiredResource(bundles, vmResourceRequirement)
	alloactor.calculateDensityMetric(bids)
	alloactor.allocate(resourceCaps, bids)
