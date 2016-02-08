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
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.nio.interfaces.NodeConfig;

/**
 * Planetlab context service example with 10 nodes, with simple
 * conjunction query.
 * @author adipc
 */
public class StartContextServiceNode extends ContextServiceNode<Integer>
{
	public static final int HYPERSPACE_HASHING							= 1;
	
	private static CSNodeConfig<Integer> csNodeConfig					= null;
	
	private static StartContextServiceNode myObj						= null;
	
	public StartContextServiceNode(Integer id, NodeConfig<Integer> nc)
			throws IOException
	{
		super(id, nc);
	}
	
	private static void readNodeInfo() throws NumberFormatException, UnknownHostException, IOException
	{
		csNodeConfig = new CSNodeConfig<Integer>();
		
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
	
	private static class NumMessagesPerSec implements Runnable
	{
		private final StartContextServiceNode csObj;
		
		public NumMessagesPerSec(StartContextServiceNode csObj)
		{
			this.csObj = csObj;
		}
		
		@Override
		public void run() 
		{
			long lastNumMesgs = 0;
			while(true)
			{
				try 
				{
					Thread.sleep(10000);
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
				
				long curr = csObj.contextservice.getNumMesgInSystem();
				long number = (curr-lastNumMesgs);
				lastNumMesgs = curr;
				if(number!=0)
				{
					ContextServiceLogger.getLogger().fine("ID "+csObj.myID+" NUM MESSAGES PER SEC "+number/10+" ");
				}
				//this.csObj.contextservice.getContextServiceDB().getDatabaseSize();
			}
		}
	}
	
	public static void main(String[] args) throws NumberFormatException, UnknownHostException, IOException, ParseException
	{
		CommandLine parser = initializeOptions(args);
		if (parser.hasOption("help") || args.length == 0) {
			printUsage();
	        System.exit(1);
	    }
		
		String nodeId = parser.getOptionValue("id");
		String csConfDir = parser.getOptionValue("csConfDir");
		if(parser.hasOption("enableTrigger"))
		{
			ContextServiceConfig.TRIGGER_ENABLED = true;
		}
		
		if(parser.hasOption("basicSubspaceConfig"))
		{
			ContextServiceConfig.basicSubspaceConfig = true;
		}
		
		System.out.println("StartContextServiceNode starting with nodeId "
				+nodeId+" csConfDir "+csConfDir);
		
		assert(nodeId != null);
		assert(csConfDir != null);
		
		ContextServiceConfig.configFileDirectory = csConfDir;
		
		Integer myID = Integer.parseInt(nodeId);
		
		ContextServiceConfig.SCHEME_TYPE = ContextServiceConfig.SchemeTypes.HYPERSPACE_HASHING;
		
		readNodeInfo();
		
		ContextServiceLogger.getLogger().fine("Number of nodes in the system "+csNodeConfig.getNodeIDs().size());
		
		//nodes = new StartContextServiceNode[csNodeConfig.getNodeIDs().size()];
		
		ContextServiceLogger.getLogger().fine("Starting context service");
		//new Thread(new StartNode(myID)).start();
		
		myObj = new StartContextServiceNode(myID, csNodeConfig);
		new Thread(new NumMessagesPerSec(myObj)).start();
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
		
		Option enableTrigger = new Option("enableTrigger", "triggers are enabled");
		
		Option basicSubspaceConfig = new Option("basicSubspaceConfig", "This option enables basic subspace "
				+ "config, which assigns equal nodes to all subsapces, with out any subspace replcation for high performance,"
				+ "according to the model. If this option is not set then subspaces will be replicated if needed for performance");
		
		commandLineOptions = new Options();
		commandLineOptions.addOption(nodeid);
		commandLineOptions.addOption(csConfDir);
		commandLineOptions.addOption(help);
		commandLineOptions.addOption(enableTrigger);
		commandLineOptions.addOption(basicSubspaceConfig);
		
		
		CommandLineParser parser = new GnuParser();
		return parser.parse(commandLineOptions, args);
	}
	
	private static void printUsage()
	{
		formatter.printHelp("java -cp contextService.jar edu.umass.cs.contextservice.nodeApp.StartContextServiceNode <options>", commandLineOptions);
	}
}