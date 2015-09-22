import math

from inputReader import InputReader
from resourceAllocator import ResourceAllocator

class KTimeSensitiveResourceAllocator(ResourceAllocator):

	'''
	Calculates OurMetric \sigma_i
	TODO: change function name
	'''
	def calculateSigmaMetric(self, bids):
		denoms = [None] * self.nUser		# user x pool (= 1)

		#print bids

		for userID in self.U:
			reqPool = self.a[userID]
			denom = []
			for relPool in self.f:
				tmp = [x * y for x,y in zip(reqPool, relPool)]
				denom.append(sum(tmp))

			# TODO: cross check - average or something else of scarcities
			#denoms[userID] = [sum(denom) / len(denom)]

			# taking the max of scarcities
			denoms[userID] = [max(denom)]

		#print denoms
		#print len(bids), len(bids[0])
		#print len(denoms), len(denoms[0])

		for userID in self.U:
			# TODO:: zip won't work with different length lists, denoms[userID] has single value for scarcity
			temp = [x / y for x,y in zip(bids[userID], denoms[userID])]
			self.metric[userID] = temp

		#print 'eta metric: ', self.metric
		#print 'relPool: ', relPool


	def sortUser(self, k, deadlines):
		userSortedOnDeadlines = [x for (x, y) in sorted(zip(self.U, deadlines), key=lambda x : (x[1][0], x[0]))]
		k_userSortedOnDeadlines	= userSortedOnDeadlines[:k]
		restOfTheUsers = userSortedOnDeadlines[k:]
		k_userMetric = [self.metric[x] for x in k_userSortedOnDeadlines]
		k_deadline = [deadlines[x] for x in k_userSortedOnDeadlines]
		k_userSortedOnMetricThenDeadline = [z for (x, y, z) in sorted(zip(k_userMetric, k_deadline, k_userSortedOnDeadlines), key=lambda x : (-x[0][0], x[1][0]))] # hard code 0 for single deadline of users

		self.U = k_userSortedOnMetricThenDeadline + restOfTheUsers


	def simulate(self, resourceCaps, bundles, vmResourceRequirement, bids, deadlines, k):
		nUser = len(bundles)
		self.init(nUser)
		self.calculateRelevanceFactor(resourceCaps)
		self.calculateRequiredResource(bundles, vmResourceRequirement, resourceCaps)
		self.calculateSigmaMetric(bids)
		self.sortUser(k, deadlines)

		orderedKUserIDs = self.U[:k]
		welfare, winnerLoser = self.allocate(resourceCaps, bids, k)

		return orderedKUserIDs, welfare, winnerLoser



'''
To test the mechanism
'''
if __name__ == '__main__':
	reader = InputReader()
	k = 2
	bundles, runtimes, bids, deadlines = reader.readRequestFile()
	resourceCaps = reader.readCapacityFile()
	vmResourceRequirement = reader.readVMConfigFile()

	alloactor = KTimeSensitiveResourceAllocator()
	#alloactor.calculateRelevanceFactor(resourceCaps)
	#alloactor.calculateRequiredResource(bundles, vmResourceRequirement)
	#alloactor.calculateOurAnotherMetric(bids)
	alloactor.sortUserOnDeadline(k, deadlines)
	#alloactor.allocate(resourceCaps, bids)
