package edu.umass.cs.contextservice.querying;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Random;
import java.util.Set;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.CSNodeConfig;
import edu.umass.cs.contextservice.ContextServiceDemultiplexer;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.examples.basic.CSTestConfig;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gns.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.gns.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.JSONNIOTransport;

/**
 * Class is used to send queries to context service.
 * The node where this class runs doesn't need
 * to be part of the contextservice mesh (nodes specified in nodeConfig).
 * It is basic because it takes input from the user, doesn't use any workload.
 * @author adipc
 * 
 */
public class BasicContextQuerySendExp<NodeIDType> implements InterfacePacketDemultiplexer
{	
	public static final String configFileName						= "100nodesSetup.txt";
	
	public static final int START_PORT								= 9189;
	
	private static final Random rand = new Random();
	
	private static final HashMap<Integer, InetSocketAddress> nodeMap			
																		= new HashMap<Integer, InetSocketAddress>();
	
	// trigger mesg type
	//public static final int QUERY_MSG_FROM_USER 		= 2;
	//private enum Keys {QUERY};
	
	private final NodeIDType myID;
	private final CSNodeConfig<NodeIDType> csNodeConfig;
	private final JSONNIOTransport<NodeIDType> niot;
	private final String sourceIP;
	private final int listenPort;
	
	private int requestCounter									= 0;
	
	public static int QUERY_RATE								= 5000;
	
	public BasicContextQuerySendExp(NodeIDType id) throws IOException
	{
		readNodeInfo();
		
		myID = id;
		listenPort = START_PORT+Integer.parseInt(myID.toString());
		
		csNodeConfig = new CSNodeConfig<NodeIDType>();
		
		sourceIP =  Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress();
		
		System.out.println("Source IP address "+sourceIP);
		
		csNodeConfig.add(myID, new InetSocketAddress(sourceIP, listenPort));
        
        AbstractPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		//ContextServicePacketDemultiplexer pd;
		
		System.out.println("\n\n node IP "+csNodeConfig.getNodeAddress(this.myID)+
				" node Port "+csNodeConfig.getNodePort(this.myID)+" nodeID "+this.myID);
		
		niot = new JSONNIOTransport<NodeIDType>(this.myID,  csNodeConfig, pd , true);
		
		JSONMessenger<NodeIDType> messenger = 
			new JSONMessenger<NodeIDType>(niot.enableStampSenderInfo());
		
		pd.register(ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY, this);
		messenger.addPacketDemultiplexer(pd);
	}
	
	public void stopThis()
	{
		this.niot.stop();
	}
	
	public void sendQueryToContextService(String query, int numAttr)
	{
		try
		{
			int userReqNum = requestCounter++;
			System.out.println("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSER REQUEST ID "
					+ userReqNum +" NUMATTR "+numAttr+" AT "+System.currentTimeMillis()
					+" "+"contextATT0"+" QueryStart "+System.currentTimeMillis());
			
			QueryMsgFromUser<NodeIDType> qmesgU 
				= new QueryMsgFromUser<NodeIDType>(myID, query, sourceIP, listenPort, userReqNum);
			
			Set<Integer> keySet= nodeMap.keySet();
			
			int randIndex = rand.nextInt(keySet.size());
			
			InetSocketAddress toMe = nodeMap.get(keySet.toArray()[randIndex]);
			
			
	        niot.sendToAddress(toMe, qmesgU.toJSONObject());
	        
			System.out.println("Query sent "+query+" to "+toMe.getAddress()+" port "+toMe.getPort());
		} catch (JSONException e)
		{
			e.printStackTrace();
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
	}
	
	public void handleQueryReply(JSONObject jso)
	{
		try
		{
			long time = System.currentTimeMillis();
			QueryMsgFromUserReply<NodeIDType> qmur;
			qmur = new QueryMsgFromUserReply<NodeIDType>(jso);
			System.out.println("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSERREPLY REQUEST ID "
					+qmur.getUserReqNum()+" NUMATTR "+0+" AT "+time+" EndTime "
					+time+ " QUERY ANSWER "+qmur.getResultGUIDs());
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException
	{
		Integer clientID = Integer.parseInt(args[0]);
		QUERY_RATE = Integer.parseInt(args[1]);
		
		BasicContextQuerySendExp<Integer> basicObj = new BasicContextQuerySendExp<Integer>(clientID);
		
		while(true)
		{
			int startNumAttr = 1;
			for(int i=0;i<15;i++)
			{
				String query = getQueryOfSize(startNumAttr);
				
				basicObj.sendQueryToContextService(query, startNumAttr);
				
		    	try
		    	{
		    		Thread.sleep(1000/QUERY_RATE);
				} catch (InterruptedException e)
		    	{
		    		e.printStackTrace();
		    	}
		    	startNumAttr = startNumAttr+2;
			}
		}
	}
	
	public static String getQueryOfSize(int queryLength)
	{
		String query="";
	    for(int i=0;i<queryLength;i++)
	    {
	    	int attrNum = rand.nextInt(ContextServiceConfig.NUM_ATTRIBUTES);
	    	
	    	String predicate = "1 <= contextATT"+attrNum+" <= 5";
	    	if(i==0)
	    	{
	    		query = predicate;
	    	}
	    	else
	    	{
	    		query = query + " && "+predicate;
	    	}
	    }
	    return query;
	}
	
	
	@Override
	public boolean handleJSONObject(JSONObject jsonObject) 
	{
		System.out.println("QuerySourceDemux JSON packet recvd "+jsonObject);
		try
		{
			if(jsonObject.getInt(ContextServicePacket.PACKET_TYPE) 
					== ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY.getInt())
			{
				System.out.println("JSON packet recvd "+jsonObject);
				handleQueryReply(jsonObject);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return true;
	}
	
	private void readNodeInfo() throws NumberFormatException, UnknownHostException, IOException
	{		
		BufferedReader reader = new BufferedReader(new FileReader(configFileName));
		String line = null;
		while ((line = reader.readLine()) != null)
		{
			String [] parsed = line.split(" ");
			int readNodeId = Integer.parseInt(parsed[0])+CSTestConfig.startNodeID;
			InetAddress readIPAddress = InetAddress.getByName(parsed[1]);
			int readPort = Integer.parseInt(parsed[2]);
			
			nodeMap.put(readNodeId, new InetSocketAddress(readIPAddress, readPort));
		}
	}
	
	/**
	 * sends 10 queries 10 seconds apart, with increasing number of attributes
	 * @param basicObj
	 */
	/*public static void randomizedQueryWorkload(BasicContextQuerySendExp<Integer> basicObj)
	{
		int startNumAttr = 1;
		for(int i=0;i<15;i++)
		{
			String query = getQueryOfSize(startNumAttr);
			basicObj.sendQueryToContextService(query, startNumAttr);
			
	    	try
	    	{
	    		Thread.sleep(3000);
	    	} catch (InterruptedException e)
	    	{
	    		e.printStackTrace();
	    	}
	    	startNumAttr = startNumAttr+2;
		}
	}*/
	
}