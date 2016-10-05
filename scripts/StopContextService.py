import os, sys, time
cmd = 'pkill -9 -f contextnetJar.jar'
os.system(cmd)
print "All context service instances stopped"
