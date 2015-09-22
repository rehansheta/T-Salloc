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
from ktimeSensitiveResourceAllocator import KTimeSensitiveResourceAllocator
from scheduler import Scheduler

class KTSRAScheduler(Scheduler):

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
	Clear the pools
	'''
	def clearWaitingPool(self, kallocator, tsraAllocator, avgSystemLoad, totalWelfare, totalWinningUser, k):
		print 'Clearing the pool of size:', len(self.exitRequestPool)
		while len(self.exitRequestPool) > 0:
			timeStamp = self.exitRequestPool[0][0]

			#print
			#print 'timeStamp', timeStamp

			self.updateResourcePool(avgSystemLoad, timeStamp)

			if len(self.waitingRequestPool) > 0:
				userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes = [], [], [], [], [], [], []
				self.packFromWaitingPool(avgSystemLoad, userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes)

				#print 'users:', len(userIDs)

				# check value of k compared to total user ready to be scheduled
				if k > len(userIDs): k = len(userIDs)
				orderedKUserIdxs, welfare, winnerLoser = kallocator.simulate(self.resourceCaps, bundles, self.vmResourceRequirement, bids, deadlines, k)

				remainingUserIdxs = []
				if len(userIDs) - k > 0:
					tempBundles, tempRuntimes, tempBids, tempDeadlines = [], [], [], []
					for idx in xrange(len(userIDs)):
						if idx not in orderedKUserIdxs:
							remainingUserIdxs.append(idx)
							tempBundles.append(bundles[idx])
							tempRuntimes.append(runtimes[idx])
							tempBids.append(bids[idx])
							tempDeadlines.append(deadlines[idx])

					welfare_tsra, winnerLoser_tsra = tsraAllocator.simulate(self.resourceCaps, tempBundles, self.vmResourceRequirement, tempRuntimes, tempBids, tempDeadlines)
				else:
					welfare_tsra = 0
					winnerLoser_tsra = []

				assert (len(winnerLoser) - len(winnerLoser_tsra) == k)

				# arrange from ktsra and tsra
				for idx, userID in enumerate(remainingUserIdxs):
					winnerLoser[userID] = winnerLoser_tsra[idx]

				totalWelfare += (welfare + welfare_tsra)
				totalWinningUser += (len(winnerLoser) - winnerLoser.count(-1))

				#print totalWelfare, welfare, welfare_tsra
				self.updateRequestPools(winnerLoser, timeStamp, userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes)

			if constants.POOL_DEBUG is True:
				print 'after timeline waiting pool: ', len(self.waitingRequestPool)
				print 'after timeline exit pool: ', len(self.exitRequestPool)
				print 'after timeline missed pool: ', len(self.missedRequestPool)
				print

		return totalWelfare, totalWinningUser


	'''
	Simulates the run of all the machanisms from 0 to MAX_TIME_INTERVAL.
	Might need to time this.
	'''
	def runScheduler(self, avgSystemLoad, k_default):

		totalWelfare = 0
		totalWinningUser = 0
		kallocator = KTimeSensitiveResourceAllocator()
		tsraAllocator = TimeSensitiveResourceAllocator()

		# remaining capacity sum for each resource type
		resourceWisePercRemainingCapSum = [0] * len(self.resourceCaps[0])

		for timeStamp in xrange(0, constants.MAX_TIME_INTERVAL + 1):

			#print
			#print 'timeStamp', timeStamp

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
				# TODO: reconsider criteria for the selection of k
				k = math.ceil(((float(k_default)/100) * min(allPoolInitialResCaps)))
				#print 'we found K:', k

				# check value of k compared to total user ready to be scheduled
				if k > len(userIDs): k = len(userIDs)
				orderedKUserIdxs, welfare, winnerLoser = kallocator.simulate(self.resourceCaps, bundles, self.vmResourceRequirement, bids, deadlines, int(k))


				remainingUserIdxs = []
				if len(userIDs) - k > 0:
					tempBundles, tempRuntimes, tempBids, tempDeadlines = [], [], [], []
					for idx in xrange(len(userIDs)):
						if idx not in orderedKUserIdxs:
							remainingUserIdxs.append(idx)
							tempBundles.append(bundles[idx])
							tempRuntimes.append(runtimes[idx])
							tempBids.append(bids[idx])
							tempDeadlines.append(deadlines[idx])

					welfare_tsra, winnerLoser_tsra = tsraAllocator.simulate(self.resourceCaps, tempBundles, self.vmResourceRequirement, tempRuntimes, tempBids, tempDeadlines)
				else:
					welfare_tsra = 0
					winnerLoser_tsra = []


				assert (len(winnerLoser) - len(winnerLoser_tsra) == k)

				# arrange from ktsra and tsra
				for idx, userID in enumerate(remainingUserIdxs):
					winnerLoser[userID] = winnerLoser_tsra[idx]

				totalWelfare += (welfare + welfare_tsra)
				totalWinningUser += (len(winnerLoser) - winnerLoser.count(-1))
				
				#print totalWelfare, welfare, welfare_tsra
				self.updateRequestPools(winnerLoser, timeStamp, userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes)

			allPoolRemainingResCaps = self.getPoolResourceCapacitySum()

			# TODO: replace with this one after testing
			#resourceWisePercRemainingCapSum = map(add, resourceWisePercRemainingCapSum, [x/y if y != 0.0 else 0 for (x, y) in zip(allPoolRemainingResCaps, allPoolInitialResCaps)])

			# remaining resouce percentage calculation
			for idx, initialResCap in enumerate(allPoolInitialResCaps):
				if initialResCap != 0.0:
					resourceWisePercRemainingCapSum[idx] += (allPoolRemainingResCaps[idx] / initialResCap)

		if constants.CLEAR_THE_POOL is True:
			totalWelfare, totalWinningUser = self.clearWaitingPool(kallocator, tsraAllocator, avgSystemLoad, totalWelfare, totalWinningUser, int(k))

		# remaining resouce average percentage calculation
		resourceWisePercAvgRemainingCap = [(x * 100 /(constants.MAX_TIME_INTERVAL + 1)) for x in resourceWisePercRemainingCapSum]

		#print 'percentage:', resourceWisePercRemainingCapSum
		#print 'avg_percentage:', (resourceWisePercRemainingCapSum/(constants.MAX_TIME_INTERVAL + 1))

		return totalWelfare, totalWinningUser, resourceWisePercAvgRemainingCap

if __name__ == '__main__':
	pass
