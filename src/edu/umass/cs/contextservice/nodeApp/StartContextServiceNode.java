package edu.umass.cs.contextservice.nodeApp;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import edu.umass.cs.contextservice.ContextServiceNode;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.config.CSConfigFileLoader;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;

/**
 * Planetlab context service example with 10 nodes, with simple
 * conjunction query.
 * @author adipc
 */
public class StartContextServiceNode extends ContextServiceNode
{
	public static final int HYPERSPACE_HASHING							= 1;
	
	private static CSNodeConfig csNodeConfig					= null;
	
	public StartContextServiceNode(Integer id, CSNodeConfig nc)
			throws Exception
	{
		super(id, nc);
	}
	
	private static void readNodeInfo() throws NumberFormatException, UnknownHostException, IOException
	{
		csNodeConfig = new CSNodeConfig();
		
		BufferedReader reader = new BufferedReader(new FileReader(
				ContextServiceConfig.configFileDirectory+"/"+ContextServiceConfig.nodeSetupFileName));
		String line = null;
		while ((line = reader.readLine()) != null)
		{
			String [] parsed = line.split(" ");
			int readNodeId = Integer.parseInt(parsed[0]);
			InetAddress readIPAddress = InetAddress.getByName(parsed[1]);
			int readPort = Integer.parseInt(parsed[2]);
			
			csNodeConfig.add(readNodeId, new InetSocketAddress(readIPAddress, readPort));
		}
		reader.close();
	}
	
	public static void main(String[] args) throws NumberFormatException, 
									UnknownHostException, IOException, ParseException
	{
		// we want to catch everything
		// because things run through executor service don't print exceptions on terminals
		try
		{
			CommandLine parser = initializeOptions(args);
			if (parser.hasOption("help") || args.length == 0) {
				printUsage();
		        System.exit(1);
		    }
			
			String nodeId = parser.getOptionValue("id");
			String csConfDir = parser.getOptionValue("csConfDir");
			
			System.out.println("StartContextServiceNode starting with nodeId "
					+nodeId+" csConfDir "+csConfDir);
			
			assert(nodeId != null);
			assert(csConfDir != null);
			
			ContextServiceConfig.configFileDirectory = csConfDir;
			
			Integer myID = Integer.parseInt(nodeId);
			
			ContextServiceConfig.SCHEME_TYPE = ContextServiceConfig.SchemeTypes.HYPERSPACE_HASHING;
			
			new CSConfigFileLoader(
					ContextServiceConfig.configFileDirectory+"/"+ContextServiceConfig.csConfigFileName);
			
			readNodeInfo();
			
			ContextServiceLogger.getLogger().fine("Number of nodes in the system "
														+csNodeConfig.getNodeIDs().size());
			
			
			ContextServiceLogger.getLogger().fine("Starting context service");
			
			new StartContextServiceNode(myID, csNodeConfig);
			
			System.out.println("############# StartContextServiceNode ID "+myID+" completed. "+
					csNodeConfig.getNodes().size() +"total. ############");
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		catch(Error ex)
		{
			ex.printStackTrace();
		}
	}

	//COMMAND LINE STUFF
	private static HelpFormatter formatter = new HelpFormatter();
	private static Options commandLineOptions;

	private static CommandLine initializeOptions(String[] args) throws ParseException 
	{
		Option help = new Option("help", "Prints Usage");
		Option nodeid = OptionBuilder.withArgName("node id").hasArg()
	         .withDescription("node id")
	         .create("id");
		Option csConfDir = OptionBuilder.withArgName("cs Conf directory").hasArg()
	         .withDescription("conf directory path relative to top level dir")
	         .create("csConfDir");
		
//		Option basicSubspaceConfig = new Option("basicSubspaceConfig", "This option enables basic subspace "
//				+ "config, which assigns equal nodes to all subsapces, with out any subspace replcation for high performance,"
//				+ "according to the model. If this option is not set then subspaces will be replicated if needed for performance");
		
		commandLineOptions = new Options();
		commandLineOptions.addOption(nodeid);
		commandLineOptions.addOption(csConfDir);
		commandLineOptions.addOption(help);
		
		
		CommandLineParser parser = new GnuParser();
		return parser.parse(commandLineOptions, args);
	}
	
	private static void printUsage()
	{
		formatter.printHelp("java -cp contextService.jar edu.umass.cs.contextservice.nodeApp.StartContextServiceNode <options>", commandLineOptions);
	}
}