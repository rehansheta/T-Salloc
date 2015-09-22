import math
import copy
import random
import heapq
import json

import constants
from inputReader import InputReader
from resourceAllocator import ResourceAllocator
from lenaGResourceAllocator import LenaGReasourceAllocator
from timeSensitiveResourceAllocator import TimeSensitiveResourceAllocator
from scheduler import Scheduler

class TSRAScheduler(Scheduler):

	'''
	Update all the request pools
	'''
	def updateRequestPools(self, winnerLoser, timeStamp, userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes):

		for idx, poolId in enumerate(winnerLoser):
			if poolId is not -1:
				self.schedulerFileWrite('SCHD', timeStamp, userIDs[idx], appNames[idx], bundles[idx], runtimes[idx], bids[idx], deadlines[idx], arrivalTimes[idx])
				heapq.heappush(self.exitRequestPool, (timeStamp + runtimes[idx][0], arrivalTimes[idx][0], userIDs[idx], poolId))	# hard coded 0 for single bidding system
			elif (poolId is -1) and (timeStamp + runtimes[idx][0] < deadlines[idx][0] + arrivalTimes[idx][0]): # hard coded 0 for single bidding system
				self.schedulerFileWrite('WAIT', timeStamp, userIDs[idx], appNames[idx], bundles[idx], runtimes[idx], bids[idx], deadlines[idx], arrivalTimes[idx])
				self.waitingRequestPool.append((arrivalTimes[idx][0], userIDs[idx]))
			elif (poolId is -1) and (timeStamp + runtimes[idx][0] >= deadlines[idx][0] + arrivalTimes[idx][0]): # hard coded 0 for single bidding system
				self.schedulerFileWrite('MISS', timeStamp, userIDs[idx], appNames[idx], bundles[idx], runtimes[idx], bids[idx], deadlines[idx], arrivalTimes[idx])
				self.missedRequestPool.append((arrivalTimes[idx][0], userIDs[idx]))
			else: print 'ERROR: control should not come here!'


	'''
	Simulates the run of all the machanisms from 0 to MAX_TIME_INTERVAL.
	Might need to time this.
	'''
	def runScheduler(self, avgSystemLoad):

		totalWelfare = 0
		totalWinningUser = 0
		tsraAllocator = TimeSensitiveResourceAllocator()

		#print 'TSRA start'
		# remaining capacity sum for each resource type
		resourceWisePercRemainingCapSum = [0] * len(self.resourceCaps[0])

		for timeStamp in xrange(0, constants.MAX_TIME_INTERVAL + 1):

			#print
			#print 'timeStamp::', timeStamp

			if constants.POOL_DEBUG is True:
				print 'waiting pool: ', len(self.waitingRequestPool)
				print 'exit pool: ', len(self.exitRequestPool)
				print 'missed pool: ', len(self.missedRequestPool)
				print

			self.updateResourcePool(avgSystemLoad, timeStamp)
			allPoolInitialResCaps = self.getPoolResourceCapacitySum()

			userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes = copy.deepcopy(Scheduler.packedAppsForEachAvgSysLoad[timeStamp])
			self.requestValidation(userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes)
			self.packFromWaitingPool(avgSystemLoad, userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes)

			if len(bundles) > 0:
				welfare, winnerLoser = tsraAllocator.simulate(self.resourceCaps, bundles, self.vmResourceRequirement, runtimes, bids, deadlines)

				totalWelfare += welfare
				totalWinningUser += (len(winnerLoser) - winnerLoser.count(-1))

				self.updateRequestPools(winnerLoser, timeStamp, userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes)

			allPoolRemainingResCaps = self.getPoolResourceCapacitySum()

			# TODO: replace with this one after testing
			#resourceWisePercRemainingCapSum = map(add, resourceWisePercRemainingCapSum, [x/y if y != 0.0 else 0 for (x, y) in zip(allPoolRemainingResCaps, allPoolInitialResCaps)])

			# remaining resouce percentage calculation
			for idx, initialResCap in enumerate(allPoolInitialResCaps):
				if initialResCap != 0.0:
					resourceWisePercRemainingCapSum[idx] += (allPoolRemainingResCaps[idx] / initialResCap)

		if constants.CLEAR_THE_POOL is True:
			totalWelfare, totalWinningUser = self.clearWaitingPool(tsraAllocator, avgSystemLoad, totalWelfare, totalWinningUser)

		# remaining resouce average percentage calculation
		resourceWisePercAvgRemainingCap = [(x * 100 /(constants.MAX_TIME_INTERVAL + 1)) for x in resourceWisePercRemainingCapSum]

		#print 'percentage:', resourceWisePercRemainingCapSum
		#print 'avg_percentage:', (resourceWisePercRemainingCapSum/(constants.MAX_TIME_INTERVAL + 1))

		return totalWelfare, totalWinningUser, resourceWisePercAvgRemainingCap


if __name__ == '__main__':
	pass
