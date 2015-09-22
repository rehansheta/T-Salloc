from lenaScheduler import LenaScheduler
from tsraScheduler import TSRAScheduler
from ktsraScheduler import KTSRAScheduler
from puretsraScheduler import PureTSRAScheduler
from scheduler import Scheduler
import constants
import time
import json
import numpy
import subprocess

'''
gnu data plot for three curves
'''
def writeGNUPlotData(x, y1, y2, y3, imageName):
	with open(imageName+'.dat', 'w') as datafile:
		datafile.write('xaxis\tEDF\tTSRA')

		if constants.USE_LENA is True:
			i_length = str(4)
			datafile.write('\t' + 'G-VMPAC' + '\n')
			for idx, item in enumerate(x):
				datafile.write(str(x[idx]) + '\t' + str(y1[idx]) + '\t' + str(y2[idx]) + '\t' + str(y3[idx]) + '\n')
		else:
			for k in xrange(constants.K_MIN, constants.K_MAX + constants.K_STEP_SIZE, constants.K_STEP_SIZE):
				datafile.write('\t' + 'k-EDF=' + str(k) + '%')
			datafile.write('\n')

			tempLen = ((constants.K_MAX - constants.K_MIN)/constants.K_STEP_SIZE) + 1
			i_length = str(3 + tempLen)

			for idx, item in enumerate(x):
				datafile.write(str(x[idx]) + '\t' + str(y1[idx]) + '\t' + str(y2[idx]))
				for k in xrange(0, tempLen, 1):
					datafile.write('\t' + str(y3[idx][k]))
				datafile.write('\n')

		datafile.close()

	# histogram dem
	if constants.DEM_REWRITE is True:
		with open(imageName+'.dem', 'w') as demFile:
			baseName = imageName.split(constants.gnuplotDirectory)[1]
			demFile.write('set terminal postscript eps size 3.5,2.5 color font \'arial,10\'\n')
			demFile.write('set output \'' + baseName + '.eps\'\n')
			demFile.write('set style fill solid 1.00 border lt -1\n')
			demFile.write('set key inside left top vertical Right noreverse noenhanced autotitle box\n')
			demFile.write('set style histogram clustered gap 1 title textcolor lt -1\n')
			demFile.write('set datafile missing \'-\'\n')
			demFile.write('set style data histograms\n')
			demFile.write('set xtics border in scale 0,0 nomirror rotate by 0 offset 0,0\n')
			demFile.write('set xtics norangelimit\n')
			demFile.write('set xtics()\n')
			#demFile.write('#set title \"' + serverName + '\" \n')
			demFile.write('set xlabel "average system load (%)" \n')
			demFile.write('set ylabel \"' + baseName + ' (%)\" \n')
			demFile.write('set yrange [0 : ] noreverse nowriteback\n')
			demFile.write('x = 0.0\n')
			demFile.write('i = ' + i_length + '\n')
			demFile.write('plot \'' + baseName + '.dat\' using 2:xtic(1) title columnheader(2), for [i=3:' + i_length + '] \'\' using i title columnheader(i)\n')
			demFile.close()


def callScheduler(schedulerName, scheduler, avgSystemLoad, missWelfareCount, welfare, winnerCount, missCount, remainingCap, k):

	scheduler.readPackedAppsForEachAvgSysLoadInFile(avgSystemLoad)

	userCount = 0.0
	totalWelfare = 0.0
	for key in Scheduler.packedAppsForEachAvgSysLoad.keys():
		userCount += len(Scheduler.packedAppsForEachAvgSysLoad[key][0])
		totalWelfare += sum(sum(x) for x in Scheduler.packedAppsForEachAvgSysLoad[key][4])	# hard coded 0 for single bid system

	scheduler.reInit(avgSystemLoad)
	scheduler.scheduleFileForEachSysLoad = open(constants.traceFileDirectory + schedulerName + '_D40_sysLoad_' + str(avgSystemLoad) + '.trace', 'w')
	scheduler.scheduleFileForEachSysLoad.write('Total user requests: ' + str(userCount) + '\n\n')

	if isinstance(scheduler, KTSRAScheduler):	totalGainedWelfare, totalWinningUser, percAvgRemainingCap = scheduler.runScheduler(avgSystemLoad, k)
	else:										totalGainedWelfare, totalWinningUser, percAvgRemainingCap = scheduler.runScheduler(avgSystemLoad)

	totalMissedBid = scheduler.getPoolWelfare(scheduler.missedRequestPool)
	totalWaitingBid = scheduler.getPoolWelfare(scheduler.waitingRequestPool)
	missWelfareCount.append((float(totalMissedBid)/(totalWelfare - totalWaitingBid)) * 100)
	welfare.append((float(totalGainedWelfare)/(totalWelfare - totalWaitingBid)) * 100)
	winnerCount.append(float(totalWinningUser)/(userCount - len(scheduler.waitingRequestPool)) * 100)
	missCount.append(float(len(scheduler.missedRequestPool))/(userCount - len(scheduler.waitingRequestPool)) * 100)
	remainingCap.append(percAvgRemainingCap)

	#print 'totalWelfare:', totalWelfare, '=', totalGainedWelfare + totalMissedBid + totalWaitingBid
	assert (totalWelfare == (totalGainedWelfare + totalMissedBid + totalWaitingBid))

	print schedulerName, ':', 'userCount:', userCount, 'winning%:', (float(totalWinningUser)/(userCount - len(scheduler.waitingRequestPool))*100), 'waiting%:', (float(len(scheduler.waitingRequestPool))/userCount*100), 'miss%:', (float(len(scheduler.missedRequestPool))/(userCount - len(scheduler.waitingRequestPool))*100), 'total:', (totalWinningUser + len(scheduler.missedRequestPool) + len(scheduler.waitingRequestPool))
	print 'welfare%: ', (float(totalGainedWelfare)/(totalWelfare - totalWaitingBid) * 100), 'missed welfare%: ', (float(totalMissedBid)/(totalWelfare - totalWaitingBid) * 100)
	#print 'remainingCap:', percAvgRemainingCap


def experimentForComparison():

	tsraScheduler = TSRAScheduler()
	puretsraScheduler = PureTSRAScheduler()

	if constants.USE_LENA is True: lenaScheduler = LenaScheduler()
	else: ktsraScheduler = KTSRAScheduler()

	'''
	Generates the intermidiate json input files for different system_loads. This loop should be executed just once
	so that the inputs remains same over different runs. helpful for debugging and deterministic output.
	'''
	loads = [x for x in xrange(constants.MIN_AVG_LOAD, constants.MAX_AVG_LOAD + constants.AVG_LOAD_STEPS, constants.AVG_LOAD_STEPS)]

	if constants.GENERATE_LOAD is True:
		for avgSystemLoad in loads:
			tsraScheduler.generateLoad(avgSystemLoad)
			tsraScheduler.writePackedAppsForEachAvgSysLoadInFile(avgSystemLoad)

	tsraWelfare = []
	tsraWinnerCount = []
	tsraMissCount = []
	tsraMissWelfareCount = []
	tsraRemainingCap = []

	puretsraWelfare = []
	puretsraWinnerCount = []
	puretsraMissCount = []
	puretsraMissWelfareCount = []
	puretsraRemainingCap = []

	if constants.USE_LENA is True:
		lenaWelfare = []
		lenaWinnerCount = []
		lenaMissCount = []
		lenaMissWelfareCount = []
		lenaRemainingCap = []
	else:
		ktsraWelfareCollection = []
		ktsraWinnerCountCollection = []
		ktsraMissCountCollection = []
		ktsraMissWelfareCountCollection = []
		ktsraRemainingCapCollection = []

	startTime = time.time()

	for avgSystemLoad in loads:
		print 'avgSystemLoad::', avgSystemLoad

		callScheduler('tsra_scheduler', tsraScheduler, avgSystemLoad, tsraMissWelfareCount, tsraWelfare, tsraWinnerCount, tsraMissCount, tsraRemainingCap, k=None)
		callScheduler('puretsra_scheduler', puretsraScheduler, avgSystemLoad, puretsraMissWelfareCount, puretsraWelfare, puretsraWinnerCount, puretsraMissCount, puretsraRemainingCap, k=None)

		if constants.USE_LENA is True:
			callScheduler('lena_scheduler', lenaScheduler, avgSystemLoad, lenaMissWelfareCount, lenaWelfare, lenaWinnerCount, lenaMissCount, lenaRemainingCap, k=None)
		else:
			Ks = [x for x in range(constants.K_MIN, constants.K_MAX + constants.K_STEP_SIZE, constants.K_STEP_SIZE)]
			ktsraWelfare = []
			ktsraWinnerCount = []
			ktsraMissCount = []
			ktsraMissWelfareCount = []
			ktsraRemainingCap = []

			for k in Ks:
				print 'K:', k
				callScheduler('ktsra_scheduler', ktsraScheduler, avgSystemLoad, ktsraMissWelfareCount, ktsraWelfare, ktsraWinnerCount, ktsraMissCount, ktsraRemainingCap, k)

			ktsraMissWelfareCountCollection.append(ktsraMissWelfareCount)
			ktsraWelfareCollection.append(ktsraWelfare)
			ktsraWinnerCountCollection.append(ktsraWinnerCount)
			ktsraMissCountCollection.append(ktsraMissCount)
			ktsraRemainingCapCollection.append(ktsraRemainingCap)

		print

	endTime = time.time()

	'''
	GNUplot input files (.dat and .dem)
	'''

	if constants.USE_LENA is True:
		writeGNUPlotData(loads, puretsraWelfare, tsraWelfare, lenaWelfare, constants.gnuplotDirectory + 'welfare.' + constants.clusterConf)
		writeGNUPlotData(loads, puretsraWinnerCount, tsraWinnerCount, lenaWinnerCount, constants.gnuplotDirectory + 'winningCount.' + constants.clusterConf)
		writeGNUPlotData(loads, puretsraMissCount, tsraMissCount, lenaMissCount, constants.gnuplotDirectory + 'missCount.' + constants.clusterConf)
		writeGNUPlotData(loads, puretsraMissWelfareCount, tsraMissWelfareCount, lenaMissWelfareCount, constants.gnuplotDirectory + 'missedWelfareCount.' + constants.clusterConf)

		for resourceType in xrange(len(tsraScheduler.resourceCaps[0])):
			writeGNUPlotData(loads, list(numpy.array(puretsraRemainingCap)[:,resourceType]), list(numpy.array(tsraRemainingCap)[:,resourceType]), list(numpy.array(lenaRemainingCap)[:,resourceType]), constants.gnuplotDirectory + 'remainingCapCount.' + constants.clusterConf + '.resourceType_' + str(resourceType))
	else:
		writeGNUPlotData(loads, puretsraWelfare, tsraWelfare, ktsraWelfareCollection, constants.gnuplotDirectory + 'welfare.' + constants.clusterConf)
		writeGNUPlotData(loads, puretsraWinnerCount, tsraWinnerCount, ktsraWinnerCountCollection, constants.gnuplotDirectory + 'winningCount.' + constants.clusterConf)
		writeGNUPlotData(loads, puretsraMissCount, tsraMissCount, ktsraMissCountCollection, constants.gnuplotDirectory + 'missCount.' + constants.clusterConf)
		writeGNUPlotData(loads, puretsraMissWelfareCount, tsraMissWelfareCount, ktsraMissWelfareCountCollection, constants.gnuplotDirectory + 'missedWelfareCount.' + constants.clusterConf)

		for resourceType in xrange(len(tsraScheduler.resourceCaps[0])):
			tmpKtsraRemainingCapCollection = []
			for x in ktsraRemainingCapCollection:
				tmpKtsraRemainingCapCollection.append([y[resourceType] for y in x])

			writeGNUPlotData(loads, list(numpy.array(puretsraRemainingCap)[:,resourceType]), list(numpy.array(tsraRemainingCap)[:,resourceType]), tmpKtsraRemainingCapCollection, constants.gnuplotDirectory + 'remainingCapCount.' + constants.clusterConf + '.resourceType_' + str(resourceType))

# changing constants and experimenting for different configurations
def experimentForAllConfig():
	for constants.USE_LENA in [True, False]:
		if constants.USE_LENA is True: technique = 'with-short-t'
		else: technique = 'with-long-t'

		for clusterConfig in ['homo', 'hetero']:
			for clusterSize in ['S','M', 'L']:
				constants.GENERATE_LOAD = True
				constants.clusterConf = clusterConfig + '_' + clusterSize
				constants.capacityFilename = 'inputs/capacity/capacity_' + constants.clusterConf + '.in'

				for mappingAlgo in ['first_fit', 'best_fit', 'worst_fit']:
					if mappingAlgo is 'first_fit': constants.first_fit = True
					elif mappingAlgo is 'best_fit': constants.best_fit = True
					elif mappingAlgo is 'worst_fit': constants.worst_fit = True

					constants.traceFileDirectory = 'outputs/experimentforMultiplePool/' + technique + '/final/' + mappingAlgo + '/traces/'
					constants.gnuplotDirectory = 'outputs/experimentforMultiplePool/' + technique + '/final/' + mappingAlgo + '/'

					experimentForComparison()
					constants.GENERATE_LOAD = False

def runEpsGenerator():
	appOut = open('/dev/null')
	p = subprocess.Popen(['./epsGenerator.sh'], shell=True, stdout=appOut, stderr=appOut)
	return p.wait()

if __name__ == '__main__':
	experimentForAllConfig()
	runEpsGenerator()

