import os, sys

configName = sys.argv[1]
mysqlUser  = sys.argv[2]
mysqlPassword = sys.argv[3]

# run base config
if(sconfig == 1):
        print "starting base config\n"
        cmd = mainDir+'/contextServiceScripts/startCSInstallerBaseConfig.sh'
        os.system(cmd)
elif(sconfig == 2):
        print "starting replicated config"
        cmd = mainDir+'/contextServiceScripts/startCSInstallerReplicatedConfig.sh'
        os.system(cmd)
