import copy
import sys
import operator
import random

from inputReader import InputReader
import constants

class ResourceAllocator:

	V = None	# welfare
	f = None	# pool x resourceCount
	a = None	# user x resourceCount
	U = None	# sorted user-list
	x = None	# winning user-list
	nUser = None
	metric = None

	def init(self, nUser):
		self.nUser = nUser
		self.V = 0
		self.U = [x for x in xrange(self.nUser)] # equals [0, nUser), nothing simillar to userIDs from packedAppsForEachAvgSysLoad structure
		self.a = [None] * self.nUser
		self.x = [-1] * self.nUser
		self.metric = [None] * self.nUser

	'''
	Calculates the RelevanceFactor f as in G-VMPAC-2-ALLOC
	'''
	def calculateRelevanceFactor(self, resourceCaps):
		self.f = []

		for resourceCap in resourceCaps:
			'''
			if capacity of some resources are zero then user request will fail
			'''
			self.f.append([1 / x if x > 0 else sys.float_info.max for x in resourceCap])

		#print 'f: ', self.f


	'''
	Calculates the resource requirement a_ir
	Ex. bundles = [[1, 2], ...], vmResourceRequirement = [[2, 4], [1, 2]]
	aBundle = [1, 2], 	perVMReq = [1*2, 1*4], perUserReq = [2, 4]
						perVMReq = [2*1, 2*2], perUserReq = [4, 8]
	etc.
	a = [[4, 8], ...]
	'''
	def calculateRequiredResource(self, bundles, vmResourceRequirement, resourceCaps):


		for userID in self.U:
			aBundle = bundles[userID]
			perUserReq= [0] * len(vmResourceRequirement[0])

			for idx, count in enumerate(aBundle):
				perVMReq = [x * count for x in vmResourceRequirement[idx]]
				perUserReq = [sum(x) for x in zip(perUserReq, perVMReq)]

			self.a[userID] = perUserReq

		resourceSum = sum([x[0] for x in self.a])

		#print 'a: ', self.a, 'sum:', resourceSum, 'percentage:', (resourceSum/resourceCaps[0][0] * 100)

	'''
	Allocates resource and returns the maximized welfare and winning user list
	@param resourceCaps: scheduler.resourceCaps, changes will reflect
	@param k: is None or in [1, nUser]
	'''
	def allocate(self, resourceCaps, bids, k=None):
		#print 'currentD: ', currentDensity
		#print 'sorted user id: ', self.U

		if k is None: k = self.nUser

		poolIds = [x for x in xrange(len(resourceCaps))]

		if constants.first_fit is True: pass
		elif constants.best_fit is True: poolIds = [x for (x, y) in sorted(zip(poolIds, resourceCaps), key=lambda x : (min(x[1]), x[0]))]
		elif constants.worst_fit is True: poolIds = [x for (x, y) in sorted(zip(poolIds, resourceCaps), key=lambda x : (-min(x[1]), x[0]))]

		for userID in self.U[:k]:

			for poolId in poolIds:

				flag = True
				tmpResCap = [0] * len(resourceCaps[poolId])

				for r, capacity in enumerate(resourceCaps[poolId]):
					tmpResCap[r] = resourceCaps[poolId][r] - self.a[userID][r]

					#print 'resourceCaps[' + str(poolId) + '][' + str(r) + ']:', resourceCaps[poolId][r]
					#print 'self.a[' + str(userID) + '][' + str(r) + ']', self.a[userID][r]
					#print 'tmpResCap[' + str(r) + ']', tmpResCap[r]

					if tmpResCap[r] < 0:
						flag = False
						#print flag,
						break

				if flag:
					#print flag
					self.V = self.V + bids[userID][0]	# might need to handle this differently for multiple bids
					self.x[userID] = poolId
					resourceCaps[poolId] = copy.deepcopy(tmpResCap)
					break

		#print 'total bid: ', self.V
		#print 'winner list:', self.x

		#print 'resourceCaps:', resourceCaps
		return self.V, self.x


if __name__ == '__main__':
	reader = InputReader()
	#bundles, times, bids, deadlines = reader.readRequestFile()
	resourceCaps = reader.readCapacityFile()
	#vmResourceRequirement = reader.readVMConfigFile()

	alloactor = ResourceAllocator()
	alloactor.calculateRelevanceFactor(resourceCaps)
	#alloactor.calculateRequiredResource(bundles, vmResourceRequirement)
