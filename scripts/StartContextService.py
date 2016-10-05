import os, sys, time


configName = ''
mysqlUser  = ''
mysqlPassword = ''
# right now scripts can only work from cns top level dir
#scriptDir = 'scripts'
confDir = 'conf'

def writeDBFile():
    nodeConfigFilePath = confDir+'/'+configName+'/contextServiceConf/contextServiceNodeSetup.txt'
    lines = []
    with open(nodeConfigFilePath) as f:
        lines = f.readlines()
    f.close()
    
    dbFilePath = confDir+'/'+configName+'/contextServiceConf/dbNodeSetup.txt'
    writef = open(dbFilePath, "w")
    curr = 0
    while(curr < len(lines)):
        writeStr = str(curr)+' 3306 contextDB'+str(curr)+' '+mysqlUser+' '+mysqlPassword+"\n"
        writef.write(writeStr)
        curr = curr +1
    
    writef.close()
    print "db file write with user given username and password complete\n"
        
        
def startCSNodes():
    cmd = 'pkill -9 -f contextnetJar.jar'
    os.system(cmd)
    time.sleep(2)
    print "Killed old context service"
    
    nodeConfigFilePath = confDir+'/'+configName+'/contextServiceConf/contextServiceNodeSetup.txt'
    cmdPrefix = 'nohup java -ea -cp release/contextnetJar.jar edu.umass.cs.contextservice.nodeApp.StartContextServiceNode '+\
                    '-id'
    lines = []
    with open(nodeConfigFilePath) as f:
        lines = f.readlines()
    f.close()
    
    configDirPath = confDir+'/'+configName+'/contextServiceConf'
    curr = 0
    while(curr < len(lines)):
        cmd = cmdPrefix+' '+str(curr)+' -csConfDir '+configDirPath +' &> csLog.log & '
        print "starting context service "+cmd
        os.system(cmd)
        curr = curr + 1
        
#print "sys.argv[1] "+sys.argv[0]+" "+str(len(sys.argv))
if(len(sys.argv) == 1):
    configName = 'locationSingleNodeConf'
    print "using locationSingleNodeConf configuration and mysql username as password specified in conf/locationSingleNodeConf/contextServiceConf/dbNodeSetup.txt"
elif(len(sys.argv) == 2):
    configName = sys.argv[1]
    print "using default mysql username as password specified in conf/"+configName+"/contextServiceConf/dbNodeSetup.txt"
elif(len(sys.argv) == 3):
    print "Mysql username and password both are needed\n"

elif(len(sys.argv) == 4):
    configName = sys.argv[1]
    mysqlUser  = sys.argv[2]
    mysqlPassword = sys.argv[3]
    writeDBFile()
        
startCSNodes()
print "\n#############Context service started##############\n"
#time.sleep(5)
#print "context service started"
