# TSRA

This project is a part of my PhD research. Three Time-Sensitive VM provisioning and resource allocation techniques for clouds are implemented along with an existing one (G-VMPAC) to compare their performances. They are greedy algorithms and best suited for heavily loaded cloud with heterogeneous resources. Based on the runtime information, collected by running spec2006, parsec-3 and splash-2 benchmarks on varius flavors of openstack virtual machines, this code runs a simulation, collects performance data and plots them using gnuplot.

The allocation mechanisms are:

1. Earlieast Deadline First (EDF) Allocation
2. Time-Sensitive Resource (TSR) Allocation
3. K-Earlieast Deadline First (K-EDF) Allocation
4. G-VMPAC Allocation

# TSRA/inputs

1. capacity/ -- contains different cluster configurations represented as resource capacities
2. vmConfig/ -- contains different virtual mechine configurations represented as resource requirements
3. spec2006/ -- contains runtime information of spec cpu2006 benchmark on different virtual machines running under openstack
4. parsec/ -- contains runtime information of parsec-3 and splash-2 benchmarks on different virtual machines running under openstack

# TSRA/

1. constants.py -- contains all the constant variables
2. inputReader.py -- reads the input files and generates request files required to run the simulation
3. experiment.py -- main file that runs the experiments
4. resourceAllocator.py -- resource allocator class
5. scheduler.py -- scheduler class
6. epsGenerator.sh -- generates graphs in eps format using gnuplot and puts them inside output folder
7. outputRemover.sh -- removes all outputs if needed

# Run

1. install gnuplot
2. change the capacity and vm configuration files
3. change the values in constants.py
4. python experiment.py

# Publication

<https://www.academia.edu/15177691/Time-Sensitive_Virtual_Machines_Provisioning_and_Resource_Allocation_in_Clouds>
