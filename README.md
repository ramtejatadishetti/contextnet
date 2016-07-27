# Getting started
Prerequisites: JRE1.8+, bash, mongodb, python, mysql, git
# Running context service and GNS together
Currently only running through source code is supported.
## Obtaining source code
```bash
  git clone https://github.com/ayadavumass/GNS.git
```
## Compiling GNS code
Go to the top level GNS directory, denoted by \<TopLevelGNSDir\>, where there is a build.xml file and issue ant
```bash
  cd <TopLevelGNSDir>
  ant
```
Build should successfully complete creating binaries in jars folder.

## Running context service locally
Prerequisites: MySQL and python should be installed before running context service. MySQL username and password is also needed by context service. 

Context service jar is included with the source code of GNS and doesn't require any compiling. The context service jar is in
```bash
  <TopLevelGNSDir>/scripts/contextServiceScripts/
```
MySQL should be running before starting context service. Start context service as follows.
```bash
  cd <TopLevelGNSDir>
  python ./scripts/contextServiceScripts/StartContextService.py locationSingleNodeConf <mysqlusername> <mysqlpassword>
```
locationSingleNodeConf is the configuration name, currently only this configuration is supported. \<mysqlusername\> and \<mysqlpassword\> is the mysql username and password that the context service needs to connect to mysql database.

The above python script starts 4 instances of context services on the local node. The context service start is complete when you see 
```bash
############# StartContextServiceNode ID 2 completed. 4total. ############
############# StartContextServiceNode ID 1 completed. 4total. ############
############# StartContextServiceNode ID 0 completed. 4total. ############
############# StartContextServiceNode ID 3 completed. 4total. ############
```
The above messages can be in any order.

After starting context service start the GNS. 

## Running single node GNS
Prerequisites: MongoDB should be installed and running on default port.
Start GNS with the follwing command.
```bash
  cd <TopLevelGNSDir>
  bin/gpServer.sh -DgigapaxosConfig=conf/gnsserver.1localCS.properties start all
```
The above command will start a single node GNS that forwards updates to context service.

The detailed instructions to run GNS are on [GNS wiki](https://github.com/MobilityFirst/GNS/wiki/Getting-Started)


## Running an example
A simple code to demonstrate the integration of GNS and CS is [here](https://github.com/ayadavumass/GNS/blob/master/src/edu/umass/cs/gnsclient/benchmarking/ContextServiceTriggerExample.java)

The above code does the follwing steps
* Creates an account GUID.
* Issues a search query to context service, which specifies a geofence.
* Updates the account GUID's location to fall within the geofence.
* Receives a trigger from context service indicating that the GUID has moved into the geofence.
* Updates the account GUID's location to fall outside the geofence.
* Receives a trigger from context service indicating that the GUID has moved out of the geofence.

Run the example code as follows.

```bash
  cd <TopLevelGNSDir>
  bin/gpClient.sh -DgigapaxosConfig=conf/gnsclient.1localCS.properties edu.umass.cs.gnsclient.benchmarking.ContextServiceTriggerExample
```
The output of the code shows the steps described above.

## Stopping GNS and context service
Stop GNS and context service as follows.
```bash
  cd <TopLevelGNSDir>
  bin/gpServer.sh -DgigapaxosConfig=conf/gnsserver.1localCS.properties stop all
  python ./scripts/contextServiceScripts/StopContextService.py
```

