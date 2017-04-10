package edu.umass.cs.contextservice.nodeApp;

import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.ContextServiceNode;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;



public class StartContextServiceWithConfig {

	//COMMAND LINE STUFF
	private static HelpFormatter formatter = new HelpFormatter();
	private static Options commandLineOptions;
	//private static ArrayList <CSNodeConfig> csNodeConfigs = new ArrayList<CSNodeConfig>();

	/**
	 * Utility function to validate attributes mentioned in properties file
	 */
	public static int ValidateAttribute(Properties prop, int index) {

		return 1;
	}

	/**
	 * Utility function to validate cns instance mentioned in properties file
	 */
	public static int ValidateCnsInstanceConfig(Properties prop, int index) {

		return 1;
	}
	 /**
	  *  Helper function to load node specifc Configuration.
	  */

	public static CSNodeConfig readNodeInfo (Properties prop, int index) throws UnknownHostException{
		
		CSNodeConfig nodeconfig = new CSNodeConfig();
		String cns_ip = ContextServiceConfig.CNS_INDEX + Integer.toString(index) + "." + ContextServiceConfig.CNS_IP_ADDR_STRING;
		String cns_port = ContextServiceConfig.CNS_INDEX + Integer.toString(index) + "." + ContextServiceConfig.CNS_PORT_STRING;

		// Read ip address and port.
		InetAddress ipAddress = InetAddress.getByName(prop.getProperty(cns_ip));
		int port = Integer.parseInt(prop.getProperty(cns_port));

		nodeconfig.add(index, new InetSocketAddress(ipAddress, port));
		return nodeconfig;
	}

	/**
	 * check if entry is a local instance of CNS 
	 */
	public static boolean checklocalInstance(int index, Properties prop) {
		return true;
	}
	
	/**
	 * Launch cns instances using the configuration provided in properties file
	 * 
	 */
	private static int LaunchCnsInstances(Properties prop) throws Exception {
		int cns_instance_count = Integer.parseInt(
    		prop.getProperty(ContextServiceConfig.CNS_INSTANCE_COUNT_STRING, Integer.toString(0)) );
		
		for (int i = 0; i < cns_instance_count; i++) {
			if (checklocalInstance(i, prop)) {
				StartNodeApp nd = new StartNodeApp(i, readNodeInfo(prop, i));
			}
			else {
				// Copy jars to the remote machine, start nodes 
			}
		}
		
		return 1;
	}

  /**
   * Validate the configuration provided in properties file.
   * @param prop
   * @return 1 on success, 0 on failure
   */
	public static int ValidateConfiguration(Properties prop) {
		
		int attribute_count = Integer.parseInt(
    		prop.getProperty(ContextServiceConfig.ATTRIBUTE_COUNT_STRING, Integer.toString(0)) );
		
		// Validate whether for every attribute, all parameters are given
		for (int i = 0; i < attribute_count; i++ ){
			if ( ValidateAttribute(prop, i) == 0)
				return 0;
		}

		int cns_instance_count = Integer.parseInt(
    		prop.getProperty(ContextServiceConfig.CNS_INSTANCE_COUNT_STRING, Integer.toString(0)) );

		// Validate whether for every cns_instance, all parameters are given
		for (int i = 0; i < cns_instance_count; i++ ){
			if ( ValidateCnsInstanceConfig(prop, i) == 0)
				return 0;
		}

		// Validate other parameters

		return 1;
	}


	/**
	 * Print valid usage  starting context service node with config
	 *  	 
	 */
	private static void printUsage() {
		formatter.printHelp("java -cp contextService.jar edu.umass.cs.contextservice.nodeApp.StartContextServiceNodeWithConfig <options>", commandLineOptions);
	}

	/**Returns parser by parsing commandline Option
	 * 
	 * @param args
	 * @return parser 
	 */
	private static CommandLine initializeOptions(String[] args) throws ParseException {
		Option help = new Option("help", "Prints Usage");

		@SuppressWarnings("static-access")
		Option propfile = OptionBuilder.withArgName("propfile").hasArg()
	         .withDescription("propfile")
	         .create("propfile");

		commandLineOptions = new Options();
		commandLineOptions.addOption(propfile);
		commandLineOptions.addOption(help);
		
		// create a GNU parser object and intialize with command line options
		CommandLineParser parser = new GnuParser();
		return parser.parse(commandLineOptions, args);
	}

	/** Driver function for reading, validating and pushing configuration
	 * 
	 */
	public static void main(String[] args) throws NumberFormatException, 
									UnknownHostException, IOException, ParseException{

		// Create a properties object to read configuration
		Properties prop = new Properties();

		
		try {
			// Get GNU parser object with help and usage Options
			CommandLine parser = initializeOptions(args);

			// check the commandline options for help if found print usage and exit
			// if no arguments are specified exit
			if (parser.hasOption("help") || args.length == 0) {
				printUsage();
				System.exit(1);
			}

			// read file name from command line arguments
			String fileName = parser.getOptionValue("propfile");
			System.out.println("Reading configuration from properties file" + fileName);

			InputStream inputStream = new FileInputStream(fileName);
			prop.load(inputStream);

			// ValidateConfiguration
			if (ValidateConfiguration(prop) == 0) {
				System.out.println("Configuration provided in properties file is not valid");
				System.exit(1);
			}

			// Launch cns instances
			if (LaunchCnsInstances(prop) == 0) {
				System.out.println("Unable to launch cns instance");
				System.exit(1);
			}

		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
		catch(Error er) {
			er.printStackTrace();
		}
	}
}
