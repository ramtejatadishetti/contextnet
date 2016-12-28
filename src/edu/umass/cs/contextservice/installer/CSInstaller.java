/*
 *
 *  Copyright (c) 2015 University of Massachusetts
 *
 *  Licensed under the Apache License, Version 2.0 (the "License"); you
 *  may not use this file except in compliance with the License. You
 *  may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 *
 *  Initial developer(s): aditya
 *
 */
package edu.umass.cs.contextservice.installer;


import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.networktools.ExecuteBash;
import edu.umass.cs.contextservice.networktools.RSync;
import edu.umass.cs.contextservice.networktools.SSHClient;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Installs n instances of the GNS Jars on remote hosts and executes them.
 * More specifically this copies the GNS JAR and all the required config files
 * to the remote host then starts a Name Server and a Local Name server
 * on each host.
 *
 * Typical uses:
 *
 * First time install:
 * -cp jars/GNS.jar edu.umass.cs.gnsserver.installer.GNSInstaller -scriptFile conf/ec2_mongo_java_install.bash -update kittens.name
 *
 * Later updates:
 * java -cp jars/GNS.jar edu.umass.cs.gnsserver.installer.GNSInstaller -update kittens.name
 *
 *
 * @author ayadav
 */
public class CSInstaller 
{

  private static final String FILESEPARATOR = System.getProperty("file.separator");
  private static final String CONF_FOLDER_NAME = "conf";
  private static final String KEYHOME = System.getProperty("user.home") + FILESEPARATOR + ".ssh";

  /**
   * The default datastore type.
   */
  //public static final DataStoreType DEFAULT_DATA_STORE_TYPE = DataStoreType.MONGO;
  private static final String DEFAULT_USERNAME 				= "ec2-user";
  private static final String DEFAULT_KEYNAME 				= "id_rsa";
  private static final String DEFAULT_INSTALL_PATH 			= "gns";
  private static final String INSTALLER_CONFIG_FILENAME 	= "installer_config";
  
  
  //private static final String SUBSPACE_INFO_FILENAME   		= "subspaceInfo.txt";
  
  //private static final String NS_PROPERTIES_FILENAME = "ns.properties";
  //private static final String PAXOS_PROPERTIES_FILENAME = "gigapaxos.gnsApp.properties";
  //private static final String LNS_HOSTS_FILENAME = "lns_hosts.txt";
  //private static final String NS_HOSTS_FILENAME = "ns_hosts.txt";
  private static final String DEFAULT_JAVA_COMMAND = "java -ea -Xmx4048M";
  //private static final String DEFAULT_JAVA_COMMAND_FOR_LNS = "java -ea -Xms512M";
  //private static final String KEYSTORE_FOLDER_NAME = "keyStore";
  //private static final String TRUSTSTORE_FOLDER_NAME = "trustStore";
  //private static final String TRUST_STORE_OPTION = "-Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.trustStore=conf/trustStore/node100.jks";
  //private static final String KEY_STORE_OPTION = "-Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks";
  //private static final String SSL_DEBUG = "-Djavax.net.debug=ssl";
  // should make this a config parameter
  //private static final String JAVA_COMMAND = "java -ea";

  /**
   * Hostname map
   * <nodeId, hostname> is the tuple stored
   */
  private static final ConcurrentHashMap<String, String> hostTable = new ConcurrentHashMap<String, String>();
  //
  //private static DataStoreType dataStoreType = DEFAULT_DATA_STORE_TYPE;
  private static String hostType = "linux";
  private static String userName = DEFAULT_USERNAME;
  private static String keyFile = DEFAULT_KEYNAME;
  private static String installPath = DEFAULT_INSTALL_PATH;
  private static String javaCommand = DEFAULT_JAVA_COMMAND;
  //private static String javaCommandForLNS = DEFAULT_JAVA_COMMAND_FOR_LNS; // this one isn't changed by config
  // calculated from the Jar location
  //private static String distFolderPath;
  private static String csJarFileLocation;
  private static String confFolderPath;
  //private static String csConfFolderPath;
  // these are mostly for convienence; could compute them when needed
  private static String csJarFileName;
  
  //ATTR_INFO_FILENAME  			= "attributeInfo.txt";
  //NODE_SETUP_FILENAME 			= "contextServiceNodeSetup.txt";
  //DB_SETUP_FILENAME   			= "dbNodeSetup.txt";
  //SUBSPACE_INFO_FILENAME
  
  private static String localAttrInfoFileLocation;
  private static String localNodeSetupFileLocation;
  private static String localDBSetupFileLocation;
  private static String localCSConfigFileLocation;
  private static String localRegionInfoFileLocation;
  
  private static String localAttrInfoFileName;
  private static String localNodeSetupFileName;
  private static String localDBSetupFileName;
  private static String localCSConfigFileName;
  private static String localRegionInfoFileName;

  private static final String StartCSClass 
  				 = "edu.umass.cs.contextservice.nodeApp.StartContextServiceNode";
  //private static final String StartLNSClass = "edu.umass.cs.gnsserver.localnameserver.LocalNameServer";
  //private static final String StartNSClass = "edu.umass.cs.gnsserver.gnsApp.AppReconfigurableNode";
  //private static final String StartNoopClass = "edu.umass.cs.gnsserver.gnsApp.noopTest.DistributedNoopReconfigurableNode";

  private static final String CHANGETOINSTALLDIR
          = "# make current directory the directory this script is in\n"
          + "DIR=\"$( cd \"$( dirname \"${BASH_SOURCE[0]}\" )\" && pwd )\"\n"
          + "cd $DIR\n";

  private static final String MongoRecordsClass = "edu.umass.cs.gnsserver.database.MongoRecords";
  //private static final String CassandraRecordsClass = "edu.umass.cs.gnsserver.database.CassandraRecords";

  private static void loadConfig(String configName) {

    File configFile = fileSomewhere(configName + FILESEPARATOR + INSTALLER_CONFIG_FILENAME, confFolderPath);
    InstallConfig installConfig = new InstallConfig(configFile.toString());

    keyFile = installConfig.getKeyFile();
    System.out.println("Key File: " + keyFile);
    userName = installConfig.getUsername();
    System.out.println("User Name: " + userName);
    
    hostType = installConfig.getHostType();
    System.out.println("Host Type: " + hostType);
    installPath = installConfig.getInstallPath();
    if (installPath == null) {
      installPath = DEFAULT_INSTALL_PATH;
    }
    System.out.println("Install Path: " + installPath);
    //
    javaCommand = installConfig.getJavaCommand();
    if (javaCommand == null) {
      javaCommand = DEFAULT_JAVA_COMMAND;
    }
    System.out.println("Java Command: " + javaCommand);
  }

  private static void loadCSConfFiles(String configName) throws NumberFormatException, UnknownHostException, IOException 
  {
	  String csConfFolderName = configName + FILESEPARATOR+ContextServiceConfig.CS_CONF_FOLDERNAME;
	  
	  File nodeSetupFile = fileSomewhere(csConfFolderName + FILESEPARATOR 
			  				+ ContextServiceConfig.NODE_SETUP_FILENAME, confFolderPath);
	  BufferedReader reader = new BufferedReader(new FileReader(nodeSetupFile));
	  String line = null;
	  while ( (line = reader.readLine()) != null )
	  {
		  String [] parsed = line.split(" ");
		  String readNodeId = parsed[0];
		  //InetAddress readIPAddress = InetAddress.getByName(parsed[1]);
		  //int readPort = Integer.parseInt(parsed[2]);
		  hostTable.put(readNodeId, parsed[1]);
	  }
	  reader.close();
  }

  /**
   * Copies the latest version of the JAR files to the all the hosts in the installation given by name and restarts all the servers.
   * Does this using a separate Thread for each host.
   *
   * @param name
   * @param action
   * @param removeLogs
   * @param deleteDatabase
   * @param lnsHostsFile
   * @param nsHostsFile
   * @param scriptFile
   * @param runAsRoot
   * @param noopTest
   */
  public static void updateRunSet(String name, InstallerAction action, boolean deleteDatabase,
           String scriptFile, boolean withGNS) {
    ArrayList<Thread> threads = new ArrayList<>();
    
    Enumeration<String> keyIter = hostTable.keys();
    
    while( keyIter.hasMoreElements() )
    {
    	
    	String nodeId = keyIter.nextElement();
    	String hostname = hostTable.get(nodeId);
    	threads.add(new UpdateThread(hostname, nodeId,
                action, deleteDatabase, 
                scriptFile, withGNS));
    }
    
    for (Thread thread : threads) {
      thread.start();
    }
    // and wait for them to complete
    try {
      for (Thread thread : threads) {
        thread.join();
      }
    } catch (InterruptedException e) {
    	System.out.println("Problem joining threads: " + e);
    }
//    if (action != InstallerAction.STOP) {
//      updateNodeConfigAndSendOutServerInit();
//    }
    
    System.out.println("Finished " + name + " " + action.name() 
    					+ " at " + new Date().toString());
  }

  /**
   * What action to perform on the servers.
   */
  public enum InstallerAction {
    /**
     * Makes the installer kill the servers, update all the relevant files on the remote hosts and restart.
     */
    UPDATE,
    /**
     * Makes the installer just kill and restart all the servers.
     */
    RESTART,
    /**
     * Makes the installer kill the servers.
     */
    STOP,
  };

  /**
   * This is called to install and run the GNS on a single host. This is called concurrently in
   * one thread per each host. LNSs will be run on hosts according to the contents of the lns hosts
   * file.
   * Copies the JAR and conf files and optionally resets some other stuff depending on the
   * update action given.
   * Then the various servers are started on the host.
   * @param nsId
   * @param createLNS
   * @param hostname
   * @param action
   * @param removeLogs
   * @param deleteDatabase
   * @param scriptFile
   * @param lnsHostsFile
   * @param nsHostsFile
   * @param runAsRoot
   * @param noopTest
   * @throws java.net.UnknownHostException
   */
  public static void updateAndRunGNS(String nodeId, String hostname, InstallerAction action,
          boolean deleteDatabase, String scriptFile, boolean withGNS) throws UnknownHostException 
  {
    if (!action.equals(InstallerAction.STOP)) {
    	System.out.println("**** CSNode nodeId " + nodeId + " on host " + hostname + " starting update ****");
      
      if (action == InstallerAction.UPDATE) {
        makeInstallDir(hostname);
      }
      System.out.println("Kill servers start ");
      killAllServers(hostname);
      System.out.println("Kill servers complete");
      if (scriptFile != null) {
    	  System.out.println("Executing script file");
    	  executeScriptFile(hostname, scriptFile);
      }
      
      if (deleteDatabase) {
    	System.out.println("Delete db");
        deleteDatabase(hostname);
      }
      switch (action) {
        case UPDATE:
        	System.out.println("Executing update case");
          makeConfAndcopyJarAndConfFiles(hostname);
          //copyHostsFiles(hostname, createLNS ? lnsHostsFile : null, nsHostsFile);
          //copySSLFiles(hostname);
          break;
        case RESTART:
          break;
	default:
		assert(false);
		break;
      }
      startServers(nodeId, hostname, withGNS);
      System.out.println("#### CS " + nodeId +" on host " + hostname + " finished update ####"); 
    } else {
      killAllServers(hostname);
//      if (removeLogs) {
//        removeLogFiles(hostname, runAsRoot);
//      }
      if (deleteDatabase) {
        deleteDatabase(hostname);
      }
      System.out.println("#### CS " + nodeId + " on host " + hostname + " has been stopped ####");
    }
  }
  
  /**
   * Starts a pair of active replica / reconfigurator on each host in the ns hosts file
   * plus lns servers on each host in the lns hosts file.
   * @param id
   * @param hostname
   */
  private static void startServers(String nodeId, String hostname, boolean runningWithGNS) 
  {
    File keyFileName = getKeyFile();
    System.out.println("Starting context service on "+hostname+" with nodeId "+ nodeId);
    ExecuteBash.executeBashScriptNoSudo(userName, hostname, keyFileName, 
    		buildInstallFilePath("runContextServiceId"+nodeId+"OnNode.sh"),
            "#!/bin/bash\n"
            + CHANGETOINSTALLDIR
            + "nohup " + javaCommand
            + " -cp " + csJarFileName
            + (runningWithGNS?":GNS.jar":"")
            + " " + StartCSClass + " "
            + "-id "
            + nodeId + " "
            + "-csConfDir "
            + CONF_FOLDER_NAME + FILESEPARATOR + ContextServiceConfig.CS_CONF_FOLDERNAME + " "
            + " > CSlogfileId"+nodeId+" 2>&1 &");
    System.out.println("Starting context service on "+hostname+" with nodeId "+ nodeId);
  }
  
  /**
   * Runs the script file on the remote host.
   * @param id
   * @param hostname
   */
  private static void executeScriptFile(String hostname, String scriptFileLocation) {
    File keyFileName = getKeyFile();
    System.out.println("Copying script file");
    // copy the file to remote host
    String remoteFile = Paths.get(scriptFileLocation).getFileName().toString();
    RSync.upload(userName, hostname, keyFileName, scriptFileLocation, buildInstallFilePath(remoteFile));
    // make it executable
    SSHClient.exec(userName, hostname, keyFileName, "chmod ugo+x" + " " + buildInstallFilePath(remoteFile));
    //execute it
    SSHClient.exec(userName, hostname, keyFileName, "bash " + FILESEPARATOR + buildInstallFilePath(remoteFile));
  }

  private static void makeInstallDir(String hostname) {
    System.out.println("Creating install directory");
    if (installPath != null) {
      SSHClient.exec(userName, hostname, getKeyFile(), "mkdir -p " + installPath);
    }
  }
  
  /**
   * Deletes the database on the remote host.
   *
   * @param id
   * @param hostname
   */
  private static void deleteDatabase(String hostname) {
    ExecuteBash.executeBashScriptNoSudo(userName, hostname, getKeyFile(), buildInstallFilePath("deleteDatabase.sh"),
            "#!/bin/bash\n"
            + CHANGETOINSTALLDIR
            + "java -cp " + csJarFileName + " " + MongoRecordsClass + " -clear");
  }

  /**
   * Kills all servers on the remote host.
   * @param id
   * @param hostname
   */
  private static void killAllServers(String hostname) {
	  // kill not working on emulab
	  /*try
	  {
		  System.out.println("Killing GNS servers");
		  ExecuteBash.executeBashScriptNoSudo(userName, hostname, getKeyFile(),
            //runAsRoot,
            buildInstallFilePath("killAllServers.sh"),
            ((runAsRoot) ? "sudo " : "")
            + "pkill -f \"" + gnsJarFileName + "\""
            //+ "kill -s TERM `ps -ef | grep GNS.jar | grep -v grep | awk \"{print \\$2}\"`"
           // + "kill -s TERM `ps -ef | grep GNS.jar | grep -v grep | cut -d \" \" -f2`"
            );
		  //"#!/bin/bash\nkillall java");
	  } catch(Exception | Error ex)
	  {
		  ex.printStackTrace();
	  }*/
  }

  /**
   * Copies the JAR and configuration files to the remote host.
   * @param id
   * @param hostname
   */
  private static void makeConfAndcopyJarAndConfFiles(String hostname) {
    if (installPath != null) {
      System.out.println("Creating conf, keystore and truststore directories");
      SSHClient.exec(userName, hostname, getKeyFile(), "mkdir -p " + installPath + FILESEPARATOR+CONF_FOLDER_NAME);

      SSHClient.exec(userName, hostname, getKeyFile(), "mkdir -p " + installPath 
    		  + FILESEPARATOR +CONF_FOLDER_NAME + FILESEPARATOR + ContextServiceConfig.CS_CONF_FOLDERNAME);
      //SSHClient.exec(userName, hostname, getKeyFile(), "mkdir -p " + installPath + CONF_FOLDER + FILESEPARATOR + TRUSTSTORE_FOLDER_NAME);

//      SSHClient.exec(userName, hostname, getKeyFile(), "rm " + installPath + FILESEPARATOR + "*.txt");
//      SSHClient.exec(userName, hostname, getKeyFile(), "rm " + installPath + FILESEPARATOR + "*.properties");
      File keyFileName = getKeyFile();
      System.out.println("Copying jar and conf files");
      RSync.upload(userName, hostname, keyFileName, csJarFileLocation, buildInstallFilePath(csJarFileName));
      
      // localAttrInfoFileLocation;
      // localNodeSetupFileLocation;
      // localDBSetupFileLocation;
      // localSubspaceInfoFileLocation;
      String relativeCsConfFolder = CONF_FOLDER_NAME + FILESEPARATOR +ContextServiceConfig.CS_CONF_FOLDERNAME;
      
      RSync.upload(userName, hostname, keyFileName, localAttrInfoFileLocation,
              buildInstallFilePath(relativeCsConfFolder + FILESEPARATOR + localAttrInfoFileName));
      RSync.upload(userName, hostname, keyFileName, localNodeSetupFileLocation,
              buildInstallFilePath(relativeCsConfFolder + FILESEPARATOR + localNodeSetupFileName));
      RSync.upload(userName, hostname, keyFileName, localDBSetupFileLocation,
              buildInstallFilePath(relativeCsConfFolder + FILESEPARATOR + localDBSetupFileName));
      RSync.upload(userName, hostname, keyFileName, localCSConfigFileLocation,
              buildInstallFilePath(relativeCsConfFolder + FILESEPARATOR + localCSConfigFileName));
      RSync.upload(userName, hostname, keyFileName, localRegionInfoFileLocation,
              buildInstallFilePath(relativeCsConfFolder + FILESEPARATOR + localRegionInfoFileName));
    }
  }

  /**
   * Figures out the locations of the JAR and conf files.
   * @return true if it found them
   */
  private static void determineJarAndMasterPaths() {
    File jarPath = getLocalJarPath();
    System.out.println("Jar path: " + jarPath);
    csJarFileLocation = jarPath.getPath();
    File mainPath = jarPath.getParentFile().getParentFile();
    System.out.println("Main path: " + mainPath);
    confFolderPath = mainPath + FILESEPARATOR + CONF_FOLDER_NAME;
    String csConfFolderPath = confFolderPath+FILESEPARATOR + ContextServiceConfig.CS_CONF_FOLDERNAME;
    System.out.println("Conf folder path: " + confFolderPath+" csConfPath "+csConfFolderPath);
    csJarFileName = new File(csJarFileLocation).getName();
  }

  // checks for an absolute or relative path, then checks for a path in "blessed" location.
  private static boolean fileExistsSomewhere(String filename, String fileInConfigFolder) {
    return fileSomewhere(filename, fileInConfigFolder) != null;
  }

  private static File fileSomewhere(String filename, String blessedPath) {

    File file = new File(filename);
    if (file.exists()) {
      return file;
    }
    file = new File(blessedPath + FILESEPARATOR + filename);
    if (file.exists()) {
      return file;
    }
    System.out.println("Failed to find: " + filename);
    System.out.println("Also failed to find: " + blessedPath + FILESEPARATOR + filename);
    return null;
  }

  private static boolean checkAndSetConfFilePaths(String configNameOrFolder) {
    // first check for a least a config folder
    if (!fileExistsSomewhere(configNameOrFolder, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " not found... exiting. ");
      System.exit(1);
    }

    if (!fileExistsSomewhere(configNameOrFolder + FILESEPARATOR + INSTALLER_CONFIG_FILENAME, confFolderPath)) {
      System.out.println("Config folder " + configNameOrFolder + " missing file " + INSTALLER_CONFIG_FILENAME);
    }
    
    //ATTR_INFO_FILENAME
    //NODE_SETUP_FILENAME
    //DB_SETUP_FILENAME
    //SUBSPACE_INFO_FILENAME
    
    String relativeCsConfFolder = configNameOrFolder + FILESEPARATOR +ContextServiceConfig.CS_CONF_FOLDERNAME;
    if (!fileExistsSomewhere(
    		relativeCsConfFolder + FILESEPARATOR + ContextServiceConfig.ATTR_INFO_FILENAME, confFolderPath)) {
      System.out.println("csConfig folder " + relativeCsConfFolder + " missing file " 
    		  	+ ContextServiceConfig.ATTR_INFO_FILENAME);
    }
    if (!fileExistsSomewhere(
    		relativeCsConfFolder + FILESEPARATOR + ContextServiceConfig.NODE_SETUP_FILENAME, confFolderPath)) 
    {
      System.out.println("csConfig folder " + relativeCsConfFolder 
    		  	+ " missing file " + ContextServiceConfig.NODE_SETUP_FILENAME);
    }
    if (!fileExistsSomewhere(
    		relativeCsConfFolder + FILESEPARATOR + ContextServiceConfig.DB_SETUP_FILENAME, confFolderPath)) 
    {
      System.out.println("csConfig folder " + relativeCsConfFolder 
    		  	+ " missing file " + ContextServiceConfig.DB_SETUP_FILENAME);
    }
//    if (!fileExistsSomewhere(
//    		relativeCsConfFolder + FILESEPARATOR + SUBSPACE_INFO_FILENAME, confFolderPath)) {
//      System.out.println("csConfig folder " + relativeCsConfFolder + " missing file " + SUBSPACE_INFO_FILENAME);
//    }
    
    //localAttrInfoFileLocation;
    //localNodeSetupFileLocation;
    //localDBSetupFileLocation;
    //localSubspaceInfoFileLocation;
    
    localAttrInfoFileLocation = fileSomewhere(relativeCsConfFolder + FILESEPARATOR + 
    		ContextServiceConfig.ATTR_INFO_FILENAME, confFolderPath).toString();
    localNodeSetupFileLocation = fileSomewhere(relativeCsConfFolder + FILESEPARATOR + 
    		ContextServiceConfig.NODE_SETUP_FILENAME, confFolderPath).toString();
    localDBSetupFileLocation = fileSomewhere(relativeCsConfFolder + FILESEPARATOR + 
    		ContextServiceConfig.DB_SETUP_FILENAME, confFolderPath).toString();
    localCSConfigFileLocation = fileSomewhere(relativeCsConfFolder + FILESEPARATOR + 
    		ContextServiceConfig.CS_CONFIG_FILENAME, confFolderPath).toString();
    localRegionInfoFileLocation = fileSomewhere(relativeCsConfFolder + FILESEPARATOR + 
    		ContextServiceConfig.REGION_INFO_FILENAME, confFolderPath).toString();
    
    
    localAttrInfoFileName		= new File(localAttrInfoFileLocation).getName();
    localNodeSetupFileName    	= new File(localNodeSetupFileLocation).getName();
    localDBSetupFileName 		= new File(localDBSetupFileLocation).getName();
    localCSConfigFileName		= new File(localCSConfigFileLocation).getName();
    localRegionInfoFileName 	= new File(localRegionInfoFileLocation).getName();

    assert(localAttrInfoFileName.equals(ContextServiceConfig.ATTR_INFO_FILENAME));
    assert(localNodeSetupFileName.equals(ContextServiceConfig.NODE_SETUP_FILENAME));
    assert(localDBSetupFileName.equals(ContextServiceConfig.DB_SETUP_FILENAME));
    assert(localCSConfigFileName.equals(ContextServiceConfig.CS_CONFIG_FILENAME));
    assert(localRegionInfoFileName.equals(ContextServiceConfig.REGION_INFO_FILENAME));
    
    return true;
  }

  /**
   * Returns the location of the JAR that is running.
   *
   * @return the path
   */
  private static File getLocalJarPath() {
    try {
      return new File(ContextServiceLogger.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    } catch (URISyntaxException e) {
      ContextServiceLogger.getLogger().info("Unable to get jar location: " + e);
      return null;
    }
  }

  /**
   * Returns the location of the key file (probably in the users .ssh home).
   * @return a File
   */
  private static File getKeyFile() {
    // check using full path
    return new File(keyFile).exists() ? new File(keyFile)
            : // also check in blessed location
            new File(KEYHOME + FILESEPARATOR + keyFile).exists() ? new File(KEYHOME + FILESEPARATOR + keyFile) : null;
  }

  private static String buildInstallFilePath(String filename) {
    if (installPath == null) {
      return filename;
    } else {
      return installPath + FILESEPARATOR + filename;
    }
  }
  
  // COMMAND LINE STUFF
  private static HelpFormatter formatter = new HelpFormatter();
  private static Options commandLineOptions;
  
  private static CommandLine initializeOptions(String[] args) throws ParseException {
    Option help = new Option("help", "Prints Usage");
    Option update = OptionBuilder.withArgName("installation name").hasArg()
            .withDescription("updates CS files and restarts servers in a installation")
            .create("update");
    Option restart = OptionBuilder.withArgName("installation name").hasArg()
            .withDescription("restarts CS servers in a installation")
            .create("restart");
    //Option removeLogs = new Option("removeLogs", "remove paxos and Logger log files (use with -restart or -update)");
    Option deleteDatabase = new Option("deleteDatabase", "delete the databases in a installation (use with -restart or -update)");
    //Option dataStore = OptionBuilder.withArgName("data store type").hasArg()
    //       .withDescription("data store type")
    //        .create("datastore");
    Option scriptFile = OptionBuilder.withArgName("install script file").hasArg()
            .withDescription("specifies the location of a bash script file that will install MongoDB and Java 1.7")
            .create("scriptFile");
    Option stop = OptionBuilder.withArgName("installation name").hasArg()
            .withDescription("stops CS servers in a installation")
            .create("stop");
    Option withGNS = new Option("withGNS", "run CS along with GNS.jar, GNS.jar and contextService.jar should be in the same folder");
    //Option noopTest = new Option("noopTest", "starts noop test servers instead of GNS APP servers");
    
    commandLineOptions = new Options();
    commandLineOptions.addOption(update);
    commandLineOptions.addOption(restart);
    commandLineOptions.addOption(stop);
    //commandLineOptions.addOption(removeLogs);
    commandLineOptions.addOption(deleteDatabase);
    //commandLineOptions.addOption(dataStore);
    commandLineOptions.addOption(scriptFile);
    commandLineOptions.addOption(withGNS);
    //commandLineOptions.addOption(noopTest);
    commandLineOptions.addOption(help);
    
    CommandLineParser parser = new GnuParser();
    return parser.parse(commandLineOptions, args);
  }

  private static void printUsage() {
    formatter.printHelp("java -cp GNS.jar edu.umass.cs.gnsserver.installer.GNSInstaller <options>", commandLineOptions);
  }

  /**
   * The main routine.
   * sample usage 
   * @param args
   * @throws IOException 
   * @throws UnknownHostException 
   * @throws NumberFormatException
   * java -cp ./release/context-nodoc-GNS.jar edu.umass.cs.contextservice.installer.CSInstaller -update singleNodeConf -withGNS
   */
  public static void main(String[] args) throws NumberFormatException, UnknownHostException, IOException {
    try {
    	
      CommandLine parser = initializeOptions(args);
      if (parser.hasOption("help") || args.length == 0) {
        printUsage();
        System.exit(1);
      }
      
      String runsetUpdate = parser.getOptionValue("update");
      String runsetRestart = parser.getOptionValue("restart");
      String runsetStop = parser.getOptionValue("stop");
      //boolean removeLogs = parser.hasOption("removeLogs");
      boolean deleteDatabase = parser.hasOption("deleteDatabase");
      String scriptFile = parser.getOptionValue("scriptFile");
      boolean withGNS = parser.hasOption("withGNS");
      //boolean noopTest = parser.hasOption("noopTest");
      
      /*if (dataStoreName != null) {
        try {
          dataStoreType = DataStoreType.valueOf(dataStoreName);
        } catch (IllegalArgumentException e) {
          System.out.println("Unknown data store type " + dataStoreName + "; exiting.");
          System.exit(1);
        }
      }*/

      String configName = runsetUpdate != null ? runsetUpdate
              : runsetRestart != null ? runsetRestart
                      : runsetStop != null ? runsetStop
                              : null;
      
      System.out.println("Config name: " + configName);
      System.out.println("Current directory: " + System.getProperty("user.dir"));
      
      determineJarAndMasterPaths();
      if (!checkAndSetConfFilePaths(configName)) {
        System.exit(1);
      }
      
      loadConfig(configName);
      
      if (getKeyFile() == null) {
        System.out.println("Can't find keyfile: " + keyFile + "; exiting.");
        System.exit(1);
      }
      
      loadCSConfFiles(configName);
      
      SSHClient.setVerbose(true);
      RSync.setVerbose(true);
      
      if (runsetUpdate != null) {
        updateRunSet(runsetUpdate, InstallerAction.UPDATE, deleteDatabase
        		, scriptFile, withGNS);
      } else if (runsetRestart != null) {
        updateRunSet(runsetRestart, InstallerAction.RESTART, deleteDatabase
        		, scriptFile, withGNS);
      } else if (runsetStop != null) {
        updateRunSet(runsetStop, InstallerAction.STOP, deleteDatabase,
                null, withGNS);
      } else {
        printUsage();
        System.exit(1);
      }
    } catch (ParseException e1) 
    {
      e1.printStackTrace();
      printUsage();
      System.exit(1);
    }
    System.exit(0);
  }
  
  /**
   * The thread we use to run a copy of the updater for each host we're updating.
   */
  private static class UpdateThread extends Thread {
    private final String hostname;
    private final String nodeId;
    private final InstallerAction action;
    private final boolean deleteDatabase;
    private final String scriptFile;
    private final boolean withGNS;
    
    public UpdateThread(String hostname, String nodeId, InstallerAction action
    		, boolean deleteDatabase, String scriptFile, boolean withGNS)
    {
      this.hostname = hostname;
      this.nodeId = nodeId;
      this.action = action;
      this.deleteDatabase = deleteDatabase;
      this.scriptFile = scriptFile;
      this.withGNS = withGNS;
    }
    
    @Override
    public void run() 
    {
      try {
        CSInstaller.updateAndRunGNS(nodeId, hostname, action, deleteDatabase,
                scriptFile, withGNS);
      } catch (UnknownHostException e) {
        ContextServiceLogger.getLogger().info("Unknown hostname while updating " + hostname + ": " + e);
      }
      catch(Exception | Error ex)
      {
    	  ex.printStackTrace();
      }
    }
  }
  
  /*private static void copySSLFiles(String hostname) {
  File keyFileName = getKeyFile();
  System.out.println("Copying SSL files");
  RSync.upload(userName, hostname, keyFileName,
          confFolderPath + FILESEPARATOR + KEYSTORE_FOLDER_NAME + FILESEPARATOR + "node100.jks",
          buildInstallFilePath("conf" + FILESEPARATOR + KEYSTORE_FOLDER_NAME + FILESEPARATOR + "node100.jks"));
  RSync.upload(userName, hostname, keyFileName,
          confFolderPath + FILESEPARATOR + KEYSTORE_FOLDER_NAME + FILESEPARATOR + "node100.cer",
          buildInstallFilePath("conf" + FILESEPARATOR + KEYSTORE_FOLDER_NAME + FILESEPARATOR + "node100.cer"));
  RSync.upload(userName, hostname, keyFileName,
          confFolderPath + FILESEPARATOR + TRUSTSTORE_FOLDER_NAME + FILESEPARATOR + "node100.jks",
          buildInstallFilePath("conf" + FILESEPARATOR + TRUSTSTORE_FOLDER_NAME + FILESEPARATOR + "node100.jks"));
  RSync.upload(userName, hostname, keyFileName,
          confFolderPath + FILESEPARATOR + TRUSTSTORE_FOLDER_NAME + FILESEPARATOR + "node100.cer",
          buildInstallFilePath("conf" + FILESEPARATOR + TRUSTSTORE_FOLDER_NAME + FILESEPARATOR + "node100.cer"));
}*/

/*private static void copyHostsFiles(String hostname, String lnsHostsFile, String nsHostsFile) {
File keyFileName = getKeyFile();
System.out.println("Copying hosts files");
RSync.upload(userName, hostname, keyFileName, nsHostsFile,
        buildInstallFilePath("conf" + FILESEPARATOR + NS_HOSTS_FILENAME));
if (lnsHostsFile != null) {
  RSync.upload(userName, hostname, keyFileName, lnsHostsFile,
          buildInstallFilePath("conf" + FILESEPARATOR + LNS_HOSTS_FILENAME));
}
}*/

/**
* Starts a noop test server.
* @param nsId
* @param hostname
* @param runAsRoot
*/
/*private static void startNoopServers(String nsId, String hostname, boolean runAsRoot) {
File keyFileName = getKeyFile();
if (nsId != null) {
  System.out.println("Starting noop server");
  ExecuteBash.executeBashScriptNoSudo(userName, hostname, keyFileName, buildInstallFilePath("runNoop.sh"),
          "#!/bin/bash\n"
          + CHANGETOINSTALLDIR
          + "if [ -f Nooplogfile ]; then\n"
          + "mv --backup=numbered Nooplogfile Nooplogfile.save\n"
          + "fi\n"
          + ((runAsRoot) ? "sudo " : "")
          + "nohup " + javaCommand + " -cp " + gnsJarFileName + " " + StartNoopClass + " "
          + nsId.toString() + " "
          + NS_HOSTS_FILENAME + " "
          + " > Nooplogfile 2>&1 &");
}
System.out.println("Noop server started");
}*/
  
  /**
   * Removes log files on the remote host.
   * @param id
   * @param hostname
   */
  /*private static void removeLogFiles(String hostname, boolean runAsRoot) {
    System.out.println("Removing log files");
    ExecuteBash.executeBashScriptNoSudo(userName, hostname, getKeyFile(),
            //runAsRoot,
            buildInstallFilePath("removelogs.sh"),
            "#!/bin/bash\n"
            + CHANGETOINSTALLDIR
            + ((runAsRoot) ? "sudo " : "")
            + "rm NSlogfile*\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm Nooplogfile*\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm LNSlogfile*\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm -rf log\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm -rf derby.log\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm -rf paxos_logs\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm -rf reconfiguration_DB\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm -rf paxos_large_checkpoints\n"
            + ((runAsRoot) ? "sudo " : "")
            + "rm -rf paxoslog");
  }*/
}