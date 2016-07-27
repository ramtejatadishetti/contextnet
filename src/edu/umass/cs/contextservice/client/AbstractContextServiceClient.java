package edu.umass.cs.contextservice.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.storage.GetStorage;
import edu.umass.cs.contextservice.client.storage.SearchQueryStorage;
import edu.umass.cs.contextservice.client.storage.UpdateStorage;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.common.ContextServiceDemultiplexer;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.nio.AbstractJSONPacketDemultiplexer;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.JSONNIOTransport;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;

public abstract class AbstractContextServiceClient<NodeIDType> 
								implements PacketDemultiplexer<JSONObject>
{
	protected final JSONNIOTransport<NodeIDType> niot;
	protected final JSONMessenger<NodeIDType> messenger;
	protected final String sourceIP;
	protected final int sourcePort;
	protected final Random rand;
	// local client config
	protected CSNodeConfig<NodeIDType> clientNodeConfig									= null;
	
	// context service node config
	// this is read from the file already included in jar
	//protected CSNodeConfig<Integer> csNodeConfig										= null;
	
	protected List<InetSocketAddress> csNodeAddresses									= null;
	// hashmap of attributes, incoming updates are checked 
	// if they are for context attributes only then they are forwarded to context service
	protected HashMap<String, Boolean> attributeHashMap									= null;
	
	protected HashMap<Integer, JSONArray> subspaceAttrMap								= null;
	
	//long is the request num
	protected ConcurrentHashMap<Long, SearchQueryStorage<NodeIDType>> pendingSearches 	= null;
	protected ConcurrentHashMap<Long, UpdateStorage<NodeIDType>> pendingUpdate		  	= null;
	protected ConcurrentHashMap<Long, GetStorage<NodeIDType>> pendingGet			  	= null;
	
	protected final Object searchIdLock													= new Object();
	protected final Object updateIdLock													= new Object();
	protected final Object getIdLock													= new Object();
	protected final Object configLock													= new Object();
	
//	protected final Object privacyUpdateIdLock											= new Object();
	
	protected long searchReqId															= 0;
	protected long updateReqId															= 0;
	protected long getReqId																= 0;
	
	protected NodeIDType nodeid															= null;
	
	protected final String configHost;
	protected final int configPort;
	
	public AbstractContextServiceClient(String hostname, int port) throws IOException
	{
		this.configHost = hostname;
		this.configPort = port;
		
		csNodeAddresses  = new LinkedList<InetSocketAddress>();
		attributeHashMap = new HashMap<String, Boolean>();
		subspaceAttrMap  = new HashMap<Integer, JSONArray>();
		
		//readNodeInfo();
		//readAttributeInfo();
		
		pendingSearches = new ConcurrentHashMap<Long, SearchQueryStorage<NodeIDType>>();
		pendingUpdate = new ConcurrentHashMap<Long, UpdateStorage<NodeIDType>>();
		pendingGet = new ConcurrentHashMap<Long, GetStorage<NodeIDType>>();
		
//		rand = new Random
//			(Utils.getActiveInterfaceInetAddresses().get(0).getHostAddress().hashCode());
		
		rand = new Random(System.currentTimeMillis());
		
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
}