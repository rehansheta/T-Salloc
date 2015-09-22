import math

from inputReader import InputReader
from resourceAllocator import ResourceAllocator


'''
This mechanism only considers deadlines.
'''
class PureTimeSensitiveResourceAllocator(ResourceAllocator):

	def sortUser(self, deadlines):
		userSortedOnDeadlines = [x for (x, y) in sorted(zip(self.U, deadlines), key=lambda x : (x[1][0], x[0]))]
		self.U = userSortedOnDeadlines


	def simulate(self, resourceCaps, bundles, vmResourceRequirement, runtimes, bids, deadlines):
		# runtimes is None
		nUser = len(bundles)
		self.init(nUser)
		self.calculateRelevanceFactor(resourceCaps)
		self.calculateRequiredResource(bundles, vmResourceRequirement, resourceCaps)
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

	alloactor = PureTimeSensitiveResourceAllocator()
	nUser = len(bundles)
	alloactor.init(nUser)
	alloactor.calculateRelevanceFactor(resourceCaps)
	alloactor.calculateRequiredResource(bundles, vmResourceRequirement)
	alloactor.sortUser(deadlines)
	alloactor.allocate(resourceCaps, bids)
