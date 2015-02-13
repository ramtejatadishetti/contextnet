package edu.umass.cs.contextservice.test;

import edu.umass.cs.contextservice.CSNodeConfig;

import edu.umass.cs.gns.nio.JSONNIOTransport;
import junit.framework.TestCase;

/**
 * Test the query processing system.
 * Sends the query and waits for the reply.
 * @author adipc
 *
 */
public class QueryProcessingTest extends TestCase
{
	private CSNodeConfig<Integer> csNodeConfig										= null;
	private static JSONNIOTransport<Integer> nioTransport							= null;
	
	
	public void setUp() throws Exception
	{
		String query = "1 < ATT0 < 10 && 1 < ATT1 < 10 && 1 < ATT2 < 10";
		//QueryMessage queryMesg = new QueryMessage(query);	
	}
	
	public void tearDown() throws Exception
	{
		
	}
	
	/*public void setupNIO()
	{
		int dport = 2222;

		csNodeConfig = new CSNodeConfig<Integer>(dport);
		
		BufferedReader reader = new BufferedReader(new FileReader("nodesInfo.txt"));
		String line = null;
		while ((line = reader.readLine()) != null)
		{
			String [] parsed = line.split(" ");
			int readNodeId = Integer.parseInt(parsed[0]);
			InetAddress readIPAddress = InetAddress.getByName(parsed[1]);
			int readPort = Integer.parseInt(parsed[2]);
			
			csNodeConfig.add(readNodeId, new InetSocketAddress(readIPAddress, readPort));
		}
		
		ContextServicePacketDemultiplexer csDemultiplexer = new ContextServicePacketDemultiplexer();
		JSONMessageExtractor worker = new JSONMessageExtractor(csDemultiplexer);
	    nioTransport = new JSONNIOTransport<Integer>(myNodeId, (InterfaceNodeConfig<Integer>) csNodeConfig, worker);
	    new Thread(nioTransport).start();
		
		// initialize attribute types
		AttributeTypes.initialize();
		
		// find out which Attribute meta data I am responsible for
		// initialize meta data and value nodes
		StartContextServiceNode.initializeMetadataObjects();

		// starting query processing
		queryProcessing = new QueryProcessing();
	}*/
}