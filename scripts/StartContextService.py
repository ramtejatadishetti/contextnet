import os, sys, time, getpass

CONF_DIR = 'conf/'
CONTEXT_SERVICE_CONF_DIR = '/contextServiceConf/'

NODE_SETUP_FILE =  'contextServiceNodeSetup.txt'
DB_NODE_SETUP_FILE = 'dbNodeSetup.txt'
MYSQL_PORT = 3306
MYSQL_DB_PREFIX = "contextDB"

START_CNS_CMD_PREFIX = 'nohup java -ea -cp release/contextnetJar.jar edu.umass.cs.contextservice.nodeApp.StartContextServiceNode '+\
                    '-id'

def writeDBFile(confName, userName, password):
    nodeConfigFilePath = CONF_DIR + confName + CONTEXT_SERVICE_CONF_DIR + NODE_SETUP_FILE
    lines = []
    with open(nodeConfigFilePath) as f:
        lines = f.readlines()
    f.close()

    dbFilePath =  CONF_DIR + confName + CONTEXT_SERVICE_CONF_DIR + DB_NODE_SETUP_FILE
    writef = open( dbFilePath, "w")
    curr = 0
    while(curr < len(lines)):
        writeStr = str(curr)+ ' ' + str(MYSQL_PORT) + ' ' + MYSQL_DB_PREFIX + str(curr) + ' ' + userName + ' ' + password + "\n"
        writef.write(writeStr)
        curr = curr +1
    
    writef.close()
    print "DB file write with user given username and password complete\n"
        
        
def startCSNodes(confName):
    cmd = 'pkill -9 -f contextnetJar.jar'
    os.system(cmd)
    time.sleep(2)
    print "Killed old context service"
    
    nodeConfigFilePath = CONF_DIR + confName + CONTEXT_SERVICE_CONF_DIR + NODE_SETUP_FILE

    lines = []
    with open(nodeConfigFilePath) as f:
        lines = f.readlines()
    f.close()
    
    curr = 0
    configDirPath = CONF_DIR + confName + CONTEXT_SERVICE_CONF_DIR 
    while(curr < len(lines)):
        cmd = START_CNS_CMD_PREFIX + '  ' + str(curr) + ' -csConfDir ' + configDirPath + ' &> csLog.log & '
        print "Starting context service "+cmd
        os.system(cmd)
        curr = curr + 1
        

if(len(sys.argv) == 1):
    print "Usage :"
    print "$ python scripts/StartContextService.py <configuration> \n"
    print "List of configurations available: "
    conflist = os.listdir('conf')
    for i in range(0, len(conflist)):
        print conflist[i]
    exit()

elif(len(sys.argv) == 2):
    configName = sys.argv[1]
    mysqlUser = raw_input("Enter mysql username: ")
    mysqlPassword = getpass.getpass("Enter password for '" + mysqlUser + "' user :")
    writeDBFile(configName, mysqlUser, mysqlPassword)
    startCSNodes(configName)
    print "\n#############Context service started##############\n"


else:
    print "Invalid arguments given"
    exit()

