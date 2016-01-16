package edu.umass.cs.contextservice.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.storage.GetStorage;
import edu.umass.cs.contextservice.client.storage.SearchQueryStorage;
import edu.umass.cs.contextservice.client.storage.UpdateStorage;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.common.ContextServiceDemultiplexer;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;

public abstract class AbstractContextServiceClient<NodeIDType> implements PacketDemultiplexer<JSONObject>
{	
	protected final JSONNIOTransport<NodeIDType> niot;
	protected final JSONMessenger<NodeIDType> messenger;
	protected final String sourceIP;
	protected final int sourcePort;
	protected final Random rand;
	// local client config
	protected CSNodeConfig<NodeIDType> clientNodeConfig							= null;
	
	// context service node config
	// this is read from the file already included in jar
	//protected CSNodeConfig<Integer> csNodeConfig								= null;
	
	protected List<InetSocketAddress> csNodeAddresses							= null;
	// hashmap of attributes, incoming updates are checked 
	// if they are for context attributes only then they are forwarded to context service
	protected HashMap<String, Boolean> attributeHashMap							= null;
	
	//long is the request num
	protected HashMap<Long, SearchQueryStorage<NodeIDType>> pendingSearches		= null;
	protected HashMap<Long, UpdateStorage<NodeIDType>> pendingUpdate			= null;
	protected HashMap<Long, GetStorage<NodeIDType>> pendingGet					= null;
	
	protected final Object searchIdLock											= new Object();
	protected final Object updateIdLock											= new Object();
	protected final Object getIdLock											= new Object();
	protected final Object configLock											= new Object();
	
	
	protected long searchReqId													= 0;
	protected long updateReqId													= 0;
	protected long getReqId														= 0;
	
	protected NodeIDType nodeid													= null;
	
	protected final String configHost;
	protected final int configPort;
	
	public AbstractContextServiceClient(String hostname, int port) throws IOException
	{
		this.configHost = hostname;
		this.configPort = port;
		csNodeAddresses  = new LinkedList<InetSocketAddress>();
		attributeHashMap = new HashMap<String, Boolean>();
		
		//readNodeInfo();
		//readAttributeInfo();
		
		pendingSearches = new HashMap<Long, SearchQueryStorage<NodeIDType>>();
		pendingUpdate = new HashMap<Long, UpdateStorage<NodeIDType>>();
		pendingGet = new HashMap<Long, GetStorage<NodeIDType>>();
		
		rand = new Random();
		
		sourcePort = 2000+rand.nextInt(50000);
		sourceIP =  Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress();
		
		ContextServiceLogger.getLogger().fine("Context service client IP "+sourceIP);
		clientNodeConfig =  new CSNodeConfig<NodeIDType>();
		Integer id = 0;
		nodeid = (NodeIDType) id;
		clientNodeConfig.add(nodeid, new InetSocketAddress(sourceIP, sourcePort));
        
        AbstractJSONPacketDemultiplexer pd = new ContextServiceDemultiplexer();
		
        //System.out.println("\n\n node IP "+localNodeConfig.getNodeAddress(myID) +
		//		" node Port "+localNodeConfig.getNodePort(myID)+" nodeID "+myID);
		
		niot = new JSONNIOTransport<NodeIDType>(nodeid,  clientNodeConfig, pd , true);
		
		messenger = 
			new JSONMessenger<NodeIDType>(niot);
		
		pd.register(ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY, this);
		pd.register(ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY, this);
		pd.register(ContextServicePacket.PacketType.REFRESH_TRIGGER, this);
		pd.register(ContextServicePacket.PacketType.GET_REPLY_MESSAGE, this);
		pd.register(ContextServicePacket.PacketType.CONFIG_REPLY, this);
		
		messenger.addPacketDemultiplexer(pd);
		
		
	}
	
	
	
	private void readNodeInfo() throws NumberFormatException, UnknownHostException, IOException
	{
		InputStream input = getClass().getResourceAsStream("/"+
				ContextServiceConfig.configFileDirectory+"/"+ContextServiceConfig.nodeSetupFileName);
		//FileReader fread = new FileReader(configFileName);
		BufferedReader reader = new BufferedReader(new InputStreamReader(input));
		String line = null;
		
		while ( (line = reader.readLine()) != null )
		{
			String [] parsed = line.split(" ");
			int nodeId = Integer.parseInt(parsed[0]);
			InetAddress readIPAddress = InetAddress.getByName(parsed[1]);
			int readPort = Integer.parseInt(parsed[2]);
			ContextServiceLogger.getLogger().fine("Contextservice nodes address info nodeId "
					+ nodeId+" readIPAddress "+readIPAddress+" readPort "+readPort);
			csNodeAddresses.add(new InetSocketAddress(readIPAddress, readPort));
			//csNodeConfig.add(nodeId, new InetSocketAddress(readIPAddress, readPort));
			//nodeList.add(new InetSocketAddress(readIPAddress, readPort));
		}
		reader.close();
		input.close();
	}
	
	private void readAttributeInfo() throws IOException
	{
		//FileReader freader 	  = new FileReader(ContextServiceConfig.attributeInfoFileName);
		InputStream input = getClass().getResourceAsStream("/"+ContextServiceConfig.attributeInfoFileName);
		BufferedReader reader = new BufferedReader( new InputStreamReader(input));
		String line 		  = null;
		
		while ( (line = reader.readLine()) != null )
		{
			line = line.trim();
			if(line.startsWith("#"))
			{
				// ignore comments
			}
			else
			{
				String [] parsed = line.split(",");
				String attrName = parsed[0].trim();
				//String minValue = parsed[1].trim();
				//String maxValue = parsed[2].trim();
				//String defaultValue = parsed[3].trim();
				//String dataType = parsed[4].trim();
				ContextServiceLogger.getLogger().fine("Contextservice attribute info attrName "
						+ attrName);
				attributeHashMap.put(attrName, true);
			}
		}
		reader.close();
		input.close();
	}
	
	// non blocking call
	public abstract void sendUpdate(String GUID, JSONObject attrValuePairs, long versionNum);
	
	//blocking call
	public abstract JSONArray sendSearchQuery(String searchQuery);
	// blocking call
	public abstract JSONObject sendGetRequest(String GUID);
	
	// non blocking call
	//public abstract void expireSearchQuery(String searchQuery);
}