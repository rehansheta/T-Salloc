import math
import copy
import random
import heapq
import json
import glob
from operator import add

import constants
from inputReader import InputReader
from resourceAllocator import ResourceAllocator
from lenaGResourceAllocator import LenaGReasourceAllocator
from timeSensitiveResourceAllocator import TimeSensitiveResourceAllocator

#@static_vars(packedAppsForEachAvgSysLoad = None)
class Scheduler:
	vmResourceRequirement = None		# nVM x nResource
	resourceCaps = None					# pool x resourceCount
	exitRequestPool = None				# priority queue of (exitTime, arrivalTime, userID)'s -> key = exitTime
	waitingRequestPool = None			# list of (arrivalTime, userID)'s
	missedRequestPool = None			# list of (arrivalTime, userID)'s
	appsList = None						# format: tuple as (appName, vmType, vmCount, runtime, bid, deadline)
	nVM = None
	packedAppsForEachAvgSysLoad = None	# format: {avgLoad:{timeStamp:[(userIDs, bundles, runtimes, bids, deadlines, arrivalTimes), ...]}}
	scheduleFileForEachSysLoad = None


	'''
	Generates system load for different 'average system load'
	'''
	#@staticmethod
	def generateLoad(self, avgSystemLoad):
		Scheduler.packedAppsForEachAvgSysLoad = self.packAppsToSysLoad(avgSystemLoad)

	'''
	Write separate load files for each 'average system load'
	'''
	#@staticmethod
	def writePackedAppsForEachAvgSysLoadInFile(self, avgSystemLoad):
		fileForEachSysLoad = open('inputs/packedApp_sysLoad_' + str(avgSystemLoad), 'w')
		json.dump(Scheduler.packedAppsForEachAvgSysLoad.items(), fileForEachSysLoad, ensure_ascii=False)

	'''
	Read separate load files of each 'average system load' and keep them in single data structure
	'''
	#@staticmethod
	def readPackedAppsForEachAvgSysLoadInFile(self, avgSystemLoad):
			fileForEachSysLoad = open('inputs/packedApp_sysLoad_' + str(avgSystemLoad), 'r')
			Scheduler.packedAppsForEachAvgSysLoad = dict(json.load(fileForEachSysLoad))

	'''
	Constructor, initialize some private members
	'''
	def __init__ (self):
		reader = InputReader()
		self.resourceCaps = reader.readCapacityFile()
		self.vmResourceRequirement = reader.readVMConfigFile()
		self.appsList = reader.readAppInfo()
		self.nVM = len(self.vmResourceRequirement)
		Scheduler.packedAppsForEachAvgSysLoad = {}


	'''
	initialize pools and resource capacity
	'''
	def reInit(self, avgSystemLoad):
		self.currentRequestPool = []
		self.waitingRequestPool = []
		self.exitRequestPool = []
		self.missedRequestPool = []
		reader = InputReader()
		self.resourceCaps = reader.readCapacityFile()


	'''
	Get application pack for an 'average system load'
	'''
	def packAppsToSysLoad(self, avgSystemLoad):
		'''
		Helper funciton for packAppsToSysLoad(self, avgSystemLoad)
		Get application pack for a 'system load'
		'''
		def packApps(systemLoad, idCounter, arrivalTime):
			userIDs = []		# nUser
			appNames = []		# application names
			bundles = []		# nUser x n_vm_type(=4) (multiple type vm requests are possible. for now only one can be non-zero)
			runtimes = []		# nUser x 1
			bids = []			# nUser x 1 (multiple bids are possible)
			deadlines = []		# nUser x 1 (multiple deadlines are possible)
			arrivalTimes = []	# nUser x 1 (multiple arrivalTimes are possible)

			#sumOfMaxAppLoad = 0

			#print 'init:', 'arrivalTime:', arrivalTime, 'avgSystemLoad', avgSystemLoad, 'systemLoad:', systemLoad, 'sumOfMaxAppLoad:', sumOfMaxAppLoad

			allPoolResourceCaps = [0] * len(self.resourceCaps[0])
			for singlePoolResourceCaps in self.resourceCaps:
				allPoolResourceCaps = map(add, allPoolResourceCaps, singlePoolResourceCaps)

			#debugPrint = ''
			while systemLoad > 0:
				appInfo = random.choice(self.appsList)
				appName, vmType, vmCount, runtime, bid, deadline = appInfo

				appLoadForEachResource = [vmCount * resourceRequirement for resourceRequirement in self.vmResourceRequirement[vmType-1]]
				appLoadPercentageForEachResource = [item/allPoolResourceCaps[idx] for idx,item in enumerate(appLoadForEachResource)]	

				#debugPrint += 'appInfo:' + str(appInfo) + '\n'
				#debugPrint += 'allPoolResourceCaps:' + str(allPoolResourceCaps) + '\n'
				#debugPrint += 'appLoadForEachResource:' + str(appLoadForEachResource) + '\n'
				#debugPrint += 'appLoadPercentageForEachResource:' + str(appLoadPercentageForEachResource) + '\n'
				#debugPrint += 'systemLoad:' + str(systemLoad) + '\n'
				#debugPrint += str([x*100 for x in appLoadPercentageForEachResource])

				maxAppLoad = max(appLoadPercentageForEachResource) * 100
				#debugPrint += 'maxAppLoad:' + str(maxAppLoad) + '\n'

				systemLoad -= maxAppLoad
				#debugPrint += 'mod systemLoad:' + str(systemLoad) + '\n'
				if (systemLoad >= 0):
					#sumOfMaxAppLoad += maxAppLoad
					bundle = [0] * self.nVM
					bundle[vmType - 1] = vmCount
					bundles.append(bundle)

					userIDs.append(idCounter)
					appNames.append(appName)
					arrivalTimes.append([arrivalTime])
					bids.append([bid])

					if constants.USE_LENA is True:						
						runtimes.append([1])										# only when we compare with lena
						deadlines.append([1 * float(random.randint(2, 4))])			# only when we compare with lena
					else:
						runtimes.append([runtime/constants.TIME_UNIT])				# scaled it down to the time unit. Only when we want to have t_i > 1
						deadlines.append([deadline/constants.TIME_UNIT])			# scaled it down to the time unit. Only when we want to have t_i > 1

					idCounter += 1

				#print appLoadForEachResource
				#print appLoadPercentageForEachResource
				#print maxAppLoad

			#if len(bundles) == 0:
				#print 'bundle 0:', debugPrint
			#print 'end:', 'arrivalTime:', arrivalTime, 'avgSystemLoad', avgSystemLoad, 'systemLoad:', systemLoad, 'sumOfMaxAppLoad:', sumOfMaxAppLoad

			#print userIDs
			return (userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes), idCounter

		'''
		packAppsToSysLoad(self, avgSystemLoad) instructions start here
		'''
		idCounter = 0
		maxLoad = avgSystemLoad + constants.AVG_LOAD_RANGE
		minLoad = 0 if (avgSystemLoad - constants.AVG_LOAD_RANGE) < 0 else avgSystemLoad - constants.AVG_LOAD_RANGE
		#print 'max: ', maxLoad, ' min: ', minLoad, ' avg: ', avgSystemLoad

		loadSeries = [item for item in xrange(minLoad, maxLoad+1)]

		#random.shuffle(loadSeries)
		packedAppsForSysLoad = {}
		#shuffledLoadSeries = []

		totalLoads = 0
		for timeStamp in xrange(0, constants.MAX_TIME_INTERVAL + 1):
			#systemLoad = loadSeries[timeStamp]
			systemLoad = random.choice(loadSeries)
			
			#shuffledLoadSeries.append(systemLoad)

			#totalLoads += systemLoad
			packedApps, idCounter = packApps(systemLoad, idCounter, timeStamp)
			packedAppsForSysLoad[timeStamp] = packedApps

		#self.myTest(avgSystemLoad, shuffledLoadSeries)

		#print 'avgSystemLoad:', avgSystemLoad, 'calcLoads:', (totalLoads/(constants.MAX_TIME_INTERVAL+1))
		return packedAppsForSysLoad

	def myTest(self, avgSystemLoad, loadSeries):
		loss = 0
		carry = 0

		for load in loadSeries:
			print load, carry
			load += carry
			carry = max(0, load - 100)

		print 'avgSystemLoad:', avgSystemLoad, 'carry:', carry

	'''
	Invalid requests should go to separate log
	'''
	def requestValidation(self, userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes):
		size = len(userIDs)
		for idx in xrange(size):
			if deadlines[idx][0] < runtimes[idx][0]:	# hard coded 0 for single bidding system
				userIDs.pop(idx)
				appNames.pop(idx)
				bundles.pop(idx)
				runtimes.pop(idx)
				bids.pop(idx)
				deadlines.pop(idx)
				arrivalTimes.pop(idx)

	def schedulerFileWrite(self, eventName, timeStamp, userID, appName, bundle, runtime, bid, deadline, arrivalTime):
		self.scheduleFileForEachSysLoad.write(str(timeStamp) + '\t' + eventName + '\t' + str(userID) + '\t' + appName + '\t' + str(bundle) + '\t' + str(runtime) + '\t' + str(bid) + '\t' + str(deadline) + '\t' + str(arrivalTime) + '\n')


	'''
	Clear the pools
	'''
	def clearWaitingPool(self, allocator, avgSystemLoad, totalWelfare, totalWinningUser):
		print 'Clearing the pool of size:', len(self.exitRequestPool)
		while len(self.exitRequestPool) > 0:
			timeStamp = self.exitRequestPool[0][0]

			#print
			#print 'timeStamp', timeStamp

			self.updateResourcePool(avgSystemLoad, timeStamp)

			if len(self.waitingRequestPool) > 0:
				userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes = [], [], [], [], [], [], []
				self.packFromWaitingPool(avgSystemLoad, userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes)

				welfare, winnerLoser = allocator.simulate(self.resourceCaps, bundles, self.vmResourceRequirement, runtimes, bids, deadlines)

				totalWelfare += welfare
				totalWinningUser += winnerLoser.count(True)

				#print totalWelfare, welfare, welfare_tsra
				self.updateRequestPools(winnerLoser, timeStamp, userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes)

			if constants.POOL_DEBUG is True:
				print 'after timeline waiting pool: ', len(self.waitingRequestPool)
				print 'after timeline exit pool: ', len(self.exitRequestPool)
				print 'after timeline missed pool: ', len(self.missedRequestPool)
				print

		return totalWelfare, totalWinningUser

	'''
	Update all the resource pools
	'''
	def updateResourcePool(self, avgSystemLoad, timeStamp):
		while len(self.exitRequestPool) > 0 and self.exitRequestPool[0][0] <= timeStamp:
			exitTime, arrivalTime, userID, poolId = heapq.heappop(self.exitRequestPool)

			tempUserIDs, tempAppNames, tempBundles, tempRuntimes, tempBids, tempDeadlines, tempArrivalTimes = Scheduler.packedAppsForEachAvgSysLoad[arrivalTime]

			#userIndex = tempUserIDs.index(userID)
			userIndex = userID - tempUserIDs[0]
			aBundle = tempBundles[userIndex]

			self.schedulerFileWrite('EXIT', timeStamp, tempUserIDs[userIndex], tempAppNames[userIndex], tempBundles[userIndex], tempRuntimes[userIndex], tempBids[userIndex], tempDeadlines[userIndex], tempArrivalTimes[userIndex])

			#print 'resource pool: ', self.resourceCaps[poolId]
			for idx, count in enumerate(aBundle):
				perVMReq = [x * count for x in self.vmResourceRequirement[idx]]
				self.resourceCaps[poolId] = [c + r for c, r in zip(self.resourceCaps[poolId], perVMReq)]

			#print 'modified resource pool: ', self.resourceCaps[poolId]

	
	def getExecutionTimes(self, schedName, size):
		execTimes = [0] * size
		files = glob.glob(constants.traceFileDirectory + schedName + '*.trace')
		for file in files:
			systemLoad = int(file.split('_')[4].split('.')[0])
			with open(file, 'r') as openfile:
				lines = openfile.readlines()
				time = lines[len(lines) - 1].split('\t')[0]
				execTimes[(systemLoad - constants.MIN_AVG_LOAD)/constants.AVG_LOAD_STEPS] = float(time)
		return execTimes

	'''
	return sum of resouce (type wise) capacities
	'''
	def getPoolResourceCapacitySum(self):
		allPoolResCaps = [0] * len(self.resourceCaps[0])
		for singlePoolResourceCaps in self.resourceCaps:
			allPoolResCaps = map(add, allPoolResCaps, singlePoolResourceCaps)

		return allPoolResCaps

	'''
	Returns the total missed welfare for different Schedulers
	'''
	def getPoolWelfare(self, pool):
		totalBid = 0.0
		for arrivalTime, userID in pool:
			tempUserIDs, tempAppNames, tempBundles, tempRuntimes, tempBids, tempDeadlines, tempArrivalTimes = Scheduler.packedAppsForEachAvgSysLoad[arrivalTime]
			userIndex = userID - tempUserIDs[0]
			totalBid += tempBids[userIndex][0]

		return totalBid

	'''
	Get application pack from waiting pool
	'''
	def packFromWaitingPool(self, avgSystemLoad, userIDs, appNames, bundles, runtimes, bids, deadlines, arrivalTimes):
		for arrivalTime, userID in self.waitingRequestPool:
			#print 'packing From WaitingPool'
			tempUserIDs, tempAppNames, tempBundles, tempRuntimes, tempBids, tempDeadlines, tempArrivalTimes = Scheduler.packedAppsForEachAvgSysLoad[arrivalTime]

			#userIndex = tempUserIDs.index(userID)
			userIndex = userID - tempUserIDs[0]

			userIDs.append(userID)
			appNames.append(tempAppNames[userIndex])
			bundles.append(tempBundles[userIndex])
			runtimes.append(tempRuntimes[userIndex])
			bids.append(tempBids[userIndex])
			deadlines.append(tempDeadlines[userIndex])
			arrivalTimes.append(tempArrivalTimes[userIndex])

			#print userID, tempAppNames[userIndex], tempBundles[userIndex], tempRuntimes[userIndex], tempDeadlines[userIndex], tempArrivalTimes[userIndex]

		self.waitingRequestPool = []

