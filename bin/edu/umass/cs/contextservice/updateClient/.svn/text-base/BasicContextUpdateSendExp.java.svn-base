package edu.umass.cs.contextservice.updateClient;

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

import edu.umass.cs.contextservice.AttributeTypes;
import edu.umass.cs.contextservice.CSNodeConfig;
import edu.umass.cs.contextservice.ContextServiceDemultiplexer;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.examples.basic.CSTestConfig;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
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
 */
public class BasicContextUpdateSendExp<NodeIDType> implements InterfacePacketDemultiplexer
{
	
	//public static String csServerName 										= "ananas.cs.umass.edu";
	//public static int csPort 													= 5000;
	
	public static final String configFileName								= "100nodesSetup.txt";
	
	private static final Random rand 										= new Random();
	
	public static final String CLIENT_GUID_PREFIX							= "clientGUID";
	
	private static final HashMap<String, Double> attrValueMap				= new HashMap<String, Double>();
	
	private static final HashMap<Integer, InetSocketAddress> nodeMap			= new HashMap<Integer, InetSocketAddress>();
	
	private static final int START_PORT										= 9189;
	
	// per 1000msec
	
	//public static final int NUMGUIDs											= 1;
	// prefix for client GUIDs clientGUID1, client GUID2, ...
	
	// stores the current values
	private final NodeIDType myID;
	private final CSNodeConfig<NodeIDType> csNodeConfig;
	private final JSONNIOTransport<NodeIDType> niot;
	private final String sourceIP;
	private final int listenPort;
	
	private int versionNum														= 0;
	
	public static int ATTR_UPDATE_RATE										= 5000;
	
	public BasicContextUpdateSendExp(NodeIDType id) throws IOException
	{
		readNodeInfo();
		
		myID = id;
		
		listenPort = START_PORT+Integer.parseInt(myID.toString());
		
		csNodeConfig = new CSNodeConfig<NodeIDType>();
		
		sourceIP =  Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress();
		
		System.out.println("Source IP address "+sourceIP);
		
		csNodeConfig.add(myID, new InetSocketAddress(sourceIP, listenPort));
        
        AbstractPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		
		System.out.println("\n\n node IP "+csNodeConfig.getNodeAddress(this.myID)+
				" node Port "+csNodeConfig.getNodePort(this.myID)+" nodeID "+this.myID);
		
		niot = new JSONNIOTransport<NodeIDType>(this.myID,  csNodeConfig, pd , true);
		
		JSONMessenger<NodeIDType> messenger = 
			new JSONMessenger<NodeIDType>(niot.enableStampSenderInfo());
		
		pd.register(ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY, this);
		messenger.addPacketDemultiplexer(pd);
	}
	
	public void stopThis()
	{
		this.niot.stop();
	}
	
	public void handleUpdateReply(JSONObject jso)
	{
		try
		{
			long time = System.currentTimeMillis();
			ValueUpdateFromGNSReply<NodeIDType> vur;
			vur = new ValueUpdateFromGNSReply<NodeIDType>(jso);
			System.out.println("CONTEXTSERVICE EXPERIMENT: UPDATEFROMUSERREPLY REQUEST ID "
					+vur.getVersionNum()+" NUMATTR "+0+" AT "+time+" EndTime "
					+time);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	@Override
	public boolean handleJSONObject(JSONObject jsonObject) 
	{
		System.out.println("QuerySourceDemux JSON packet recvd "+jsonObject);
		try
		{
			if(jsonObject.getInt(ContextServicePacket.PACKET_TYPE)
					== ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY.getInt())
			{
				System.out.println("JSON packet recvd "+jsonObject);
				handleUpdateReply(jsonObject);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return true;
	}
	
	/**
	 * creates GUID and adds all attributes
	 */
	public static String createGUID(int clientID)
	{
		String guidAlias = CLIENT_GUID_PREFIX+clientID;
		String guidString = Utils.getSHA1(guidAlias).substring(0, 20);
		
		for(int i=0;i<ContextServiceConfig.NUM_ATTRIBUTES;i++)
    	{
    		attrValueMap.put(ContextServiceConfig.CONTEXT_ATTR_PREFIX+"ATT"+i, (double) 100);
    	}
		return guidString;
	}
	
	/**
	 * does the attribute updates at a rate.
	 * @throws JSONException 
	 * @throws IOException 
	 * @throws UnknownHostException 
	 */
	public void doAttributeUpdates(String guidString, String guidAlias) throws JSONException, UnknownHostException, IOException
	{
		for(int i=0;i<ContextServiceConfig.NUM_ATTRIBUTES;i++)
		{
			//System.out.println("doAttributeUpdates called "+i);
			String attName = ContextServiceConfig.CONTEXT_ATTR_PREFIX+"ATT"+i;
			double nextVal = 1+rand.nextInt((int)(AttributeTypes.MAX_VALUE-AttributeTypes.MIN_VALUE));
			
			double oldValue = attrValueMap.get(attName);
			attrValueMap.put(attName, nextVal);
			JSONObject allAttr = new JSONObject();
			
			for (String key : attrValueMap.keySet())
			{
				//System.out.println("doAttributeUpdates called "+i+" key "+key);
				allAttr.put(key, attrValueMap.get(key));
			}
			
			ValueUpdateFromGNS<NodeIDType> valMsg = new ValueUpdateFromGNS<NodeIDType>(myID, versionNum++, guidString, attName, 
					oldValue+"", nextVal+"", allAttr, sourceIP, listenPort);
			
			System.out.println("CONTEXTSERVICE EXPERIMENT: UPDATEFROMUSER REQUEST ID "
					+ valMsg.getVersionNum() +" AT "+System.currentTimeMillis());
			
			Set<Integer> keySet= nodeMap.keySet();
			
			int randIndex = rand.nextInt(keySet.size());
			
			InetSocketAddress toMe = nodeMap.get(keySet.toArray()[randIndex]);
			
			niot.sendToAddress(toMe, valMsg.toJSONObject());
			
			try
			{
				Thread.sleep(1000/ATTR_UPDATE_RATE);
			} catch (InterruptedException e) 
			{
				e.printStackTrace();
			}
		}
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
	
	public static void main(String[] args) throws IOException
	{
		Integer clientID = Integer.parseInt(args[0]);
		ATTR_UPDATE_RATE = Integer.parseInt(args[1]);
		
		BasicContextUpdateSendExp<Integer> basicObj = new BasicContextUpdateSendExp<Integer>(clientID);
		
		String guidAlias = CLIENT_GUID_PREFIX+clientID;
		String guidString = createGUID(clientID);
		
		while(true)
		{
			try
			{
				basicObj.doAttributeUpdates(guidString, guidAlias);
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
	}
	
}