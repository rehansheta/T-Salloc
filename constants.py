benchmarks = ['parsec','spec2006']	# 'parsec', 
inputLogFileDirectory = 'inputs/'

# TODO: conditions for different VM configuration
vmConfigFilename = 'inputs/vmConfig/vmConfig.in'

capacityFilename = None
clusterConf = None
first_fit = None
best_fit = None
worst_fit = None
traceFileDirectory = None
gnuplotDirectory = None

# condition for using lena (with-short-t) or not (with-long-t)
USE_LENA = None
GENERATE_LOAD = None

# best out with lena: 1000, 90, 150, 5, 40, 150, 300
MAX_TIME_INTERVAL = 80
TIME_UNIT = 300

MIN_AVG_LOAD = 80
MAX_AVG_LOAD = 140
AVG_LOAD_STEPS = 10		# step size for average system load. simulate with average_load(s) by step AVG_LOAD_STEPS
AVG_LOAD_RANGE = 10


POOL_DEBUG = False
CLEAR_THE_POOL = False
DEM_REWRITE = True

K_MIN = 20
K_MAX = 80
K_STEP_SIZE = 20

class VM_TYPES:
	small = 1
	medium = 2
	large = 3
	xlarge = 4
