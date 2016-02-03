package edu.umass.cs.contextservice.test;


import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.common.ContextServiceDemultiplexer;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;

/**
 * Test the query processing system.
 * Sends the query and waits for the reply.
 * @author adipc
 *
 */
public class QueryProcessingTest implements PacketDemultiplexer<JSONObject>
{	
	public static final String DUMMYGUID = "0B3C3AC6E25FF553BE3DC9176889E927C14CEA2A";
	public static final int NUMATTRs = 100;
	private  CSNodeConfig<Integer> csNodeConfig;
	private  JSONNIOTransport<Integer> niot;
	private int myID = 0;
	private String sourceIP;
	private int sourcePort;
	private JSONObject attrValueObject;
	private boolean testCheck;
	
	@Before
	public void setUp()
	{
		try
		{
		String[] args = {"1", NUMATTRs+""};
		
		FourNodeCSSetup.main(args);
		
		Thread.sleep(5000);
		
		sourcePort = 5000;
		
		csNodeConfig = new CSNodeConfig<Integer>();
		
		sourceIP =  "127.0.0.1";
		
		attrValueObject = new JSONObject();
		
		for(int i=0 ; i<NUMATTRs ; i++)
		{
			// equivalent to not set
			attrValueObject.put("contextATT"+i, Double.MIN_VALUE);
		}
		System.out.println("Source IP address "+sourceIP);
		
		csNodeConfig.add(myID, new InetSocketAddress(sourceIP, sourcePort));
        
        AbstractJSONPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		
        System.out.println("\n\n node IP "+csNodeConfig.getNodeAddress(myID) +
				" node Port "+csNodeConfig.getNodePort(myID)+" nodeID "+myID);
		
		niot = new JSONNIOTransport<Integer>(myID, csNodeConfig, pd , true);
		
		JSONMessenger<Integer> messenger = 
			new JSONMessenger<Integer>(niot);
		
		pd.register(ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY, this);
		pd.register(ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY, this);
		
		messenger.addPacketDemultiplexer(pd);
		System.out.println("setup done");
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	/**
	   * Test a simple connection to a local IP without GNS resolution
	   */
	  @Test
	  public void basicQueryUpdaetTest()
	  {
		  try
		  {
			  Thread.sleep(10000);
			  System.out.println("Starting basicQueryUpdaetTest");
		  testCheck = false;
		  sendUpdateToContextService(0, "contextATT0", Double.MIN_VALUE+"", 100.0+"");
		  Thread.sleep(5000);
		  sendQueryToContextService("1 <= contextATT0 <= 200", 1);
		  
		  //wait for 10 sec for reply;
		  Thread.sleep(10000);
		  if(!testCheck)
		  {
			  fail("basicQueryUpdaetTest test failed");
		  }

		  } catch(Exception ex)
		  {
			  ex.printStackTrace();
		      fail("basicQueryUpdaetTest test failed");
		  }
		  
	  }
	
	
	@After
	public void tearDown() throws Exception
	{
		
	}

	@Override
	public boolean handleMessage(JSONObject jsonObject) 
	{
		try
		{
			//ContextServiceLogger.getLogger().fine("handleJSONObject called. "+jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
			//		+" "+ jsonObject);
			if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE) 
					== ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY.getInt() )
			{
				handleQueryReply(jsonObject);
			} else if( jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
					== ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY.getInt() )
			{
				handleUpdateReply(jsonObject);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return true;
	}
	
	public void handleUpdateReply(JSONObject jso)
	{
		ValueUpdateFromGNSReply<Integer> vur;
		try
		{
			vur = new ValueUpdateFromGNSReply<Integer>(jso);
			long currReqID = vur.getVersionNum();
			
			System.out.println
			("Update reply recvd tillContextTime");
			
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	private void handleQueryReply(JSONObject jso)
	{
		try
		{
			QueryMsgFromUserReply<Integer> qmur;
			qmur = new QueryMsgFromUserReply<Integer>(jso);
			
			long reqID = qmur.getUserReqNum();
			
			System.out.println( "MSOCKETWRITERINTERNAL from CS "+qmur);
			testCheck = true;
			
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	
	private void sendQueryToContextService(String query, long userReqNum) throws IOException, JSONException
	{
		QueryMsgFromUser<Integer> qmesgU 
			= new QueryMsgFromUser<Integer>(myID, query, userReqNum, 300000, sourceIP, sourcePort);
		
		InetSocketAddress sockAddr = new InetSocketAddress("127.0.0.1", 8001);
		//ContextServiceLogger.getLogger().fine("Sending query to "+sockAddr);
		niot.sendToAddress(sockAddr, qmesgU.toJSONObject());
	}
	
	public void sendUpdateToContextService(long versionNum, String attrName, String oldVal, String newVal)
	{
		/*try
		{
			ValueUpdateFromGNS<Integer> valUpdFromGNS = 
					new ValueUpdateFromGNS<Integer>(myID, versionNum, DUMMYGUID, attrName, oldVal, newVal, 
							attrValueObject, sourceIP, sourcePort );
			
			InetSocketAddress sockAddr = new InetSocketAddress("127.0.0.1", 8001);
			niot.sendToAddress(sockAddr, valUpdFromGNS.toJSONObject());
		} catch (JSONException e)
		{
			e.printStackTrace();
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}*/
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