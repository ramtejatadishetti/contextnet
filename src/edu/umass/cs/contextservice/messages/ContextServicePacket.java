package edu.umass.cs.contextservice.messages;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.protocoltask.json.ProtocolPacket;
import edu.umass.cs.utils.IntegerPacketTypeMap;


/**
 * @author adipc
 * @param <NodeIDType>
 */
public abstract class ContextServicePacket<NodeIDType> extends ProtocolPacket<NodeIDType, ContextServicePacket.PacketType>
{
	public static final String PACKET_TYPE = JSONPacket.PACKET_TYPE;
	
	// these are internal nio fields to get the sender ip and port
	// sender ip and port when a sender is behind NAT can only be obtained
	// like this, sender cannot send it in the packet.
	//private final static String SENDERADDRESS = MessageNIOTransport.SNDR_IP_FIELD;
	//private final static String SENDERPORT 	 = MessageNIOTransport.SNDR_PORT_FIELD;
	//public final static String SENDERADDRESS = JSONNIOTransport.DEFAULT_IP_FIELD;
	//public final static String SENDERPORT = JSONNIOTransport.DEFAULT_PORT_FIELD;
	
	public static final String HANDLER_METHOD_PREFIX = "handle";
	
	//private String sourceIP;
	//private int sourcePort;
	
	/********************************* End of ContextServicePacket ***********************/
	public enum PacketType implements IntegerPacketType
	{
		// query mesg
		QUERY_MSG_FROM_USER(1),
		VALUE_UPDATE_MSG_FROM_GNS(2),     		// value update trigger from GNS
		QUERY_MSG_FROM_USER_REPLY(3),     		// reply to the query mesg from user, reply goes back to the original querier
		VALUE_UPDATE_MSG_FROM_GNS_REPLY(4),  	// reply that goes back to GNS or whoever issues this message.
		REFRESH_TRIGGER(5),   					// trigger sent to the querier to refresh
		//QUERIER_TO_RELAYSERVICE(6), 			// queries sends this message to relay service, for relay service to communicate with users.
		//RELAY_TO_RELAY_MSG(7),      			// message sent between relay service nodes.
		QUERY_MESG_TO_SUBSPACE_REGION(8),
		QUERY_MESG_TO_SUBSPACE_REGION_REPLY(9),
		VALUEUPDATE_TO_SUBSPACE_REGION_MESSAGE(10),
		GET_MESSAGE(11),
		GET_REPLY_MESSAGE(12),
		VALUEUPDATE_TO_SUBSPACE_REGION_REPLY_MESSAGE(13),
		QUERY_TRIGGER_MESSAGE(14),
		UPDATE_TRIGGER_MESSAGE(15),
		UPDATE_TRIGGER_REPLY_MESSAGE(16),
		CONFIG_REQUEST(17),
		CONFIG_REPLY(18),
		ACLUPDATE_TO_SUBSPACE_REGION_MESSAGE(19),
		ACLUPDATE_TO_SUBSPACE_REGION_REPLY_MESSAGE(20),
		// just for throughput testing.
		NOOP_MEESAGE(21),
		NOOP_REPLY_MESSAGE(22);
		
		
		private final int number;
		
		PacketType(int t) {this.number = t;}
		public int getInt() {return number;}
		
		public static final IntegerPacketTypeMap<PacketType> intToType = 
				new IntegerPacketTypeMap<PacketType>( PacketType.values() );
		
		//public static ContextServicePacket.PacketType[] getPacketTypes()
		public static List<ContextServicePacket.PacketType> getPacketTypes()
		{
			IntegerPacketTypeMap<PacketType> packetTypeMap = 
					ContextServicePacket.PacketType.intToType;
			LinkedList<ContextServicePacket.PacketType> packetTypeList = 
					new LinkedList<ContextServicePacket.PacketType>();
			
			packetTypeList.addAll(packetTypeMap.values());
			
			ContextServiceLogger.getLogger().info("size "+packetTypeList.size() + 
					" packetTypeList "+packetTypeList);
			
			return packetTypeList;
		}
	}
	
	/********************************* End of ContextServicePacketType ***********************/
	/**************************** Start of ContextServicePacketType class map **************/
	private static final HashMap<ContextServicePacket.PacketType, Class<?>> typeMap = 
			new HashMap<ContextServicePacket.PacketType, Class<?>>();
	static
	{
		/* This map prevents the need for laborious switch/case sequences as it automatically
		 * handles both json-to-ContextServicePacket conversion and invocation of the 
		 * corresponding handler method. We have to rely on reflection for both and the 
		 * cost of the former seems to be the bottleneck as it adds ~25us per conversion,
		 * but it seems not problematic for now. 
		 */
		
		typeMap.put(ContextServicePacket.PacketType.QUERY_MSG_FROM_USER, QueryMsgFromUser.class);
		typeMap.put(ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS, ValueUpdateFromGNS.class);
		typeMap.put(ContextServicePacket.PacketType.QUERY_MSG_FROM_USER_REPLY, QueryMsgFromUserReply.class);
		typeMap.put(ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS_REPLY, ValueUpdateFromGNSReply.class);
		typeMap.put(ContextServicePacket.PacketType.QUERY_MESG_TO_SUBSPACE_REGION, QueryMesgToSubspaceRegion.class);
		typeMap.put(ContextServicePacket.PacketType.QUERY_MESG_TO_SUBSPACE_REGION_REPLY, QueryMesgToSubspaceRegionReply.class);
		typeMap.put(ContextServicePacket.PacketType.VALUEUPDATE_TO_SUBSPACE_REGION_MESSAGE, ValueUpdateToSubspaceRegionMessage.class);
		typeMap.put(ContextServicePacket.PacketType.GET_MESSAGE, GetMessage.class);
		typeMap.put(ContextServicePacket.PacketType.GET_REPLY_MESSAGE, GetReplyMessage.class);
		typeMap.put(ContextServicePacket.PacketType.VALUEUPDATE_TO_SUBSPACE_REGION_REPLY_MESSAGE,
				ValueUpdateToSubspaceRegionReplyMessage.class);
		typeMap.put(ContextServicePacket.PacketType.QUERY_TRIGGER_MESSAGE,
				QueryTriggerMessage.class);
		typeMap.put(ContextServicePacket.PacketType.UPDATE_TRIGGER_MESSAGE,
				UpdateTriggerMessage.class);
		typeMap.put(ContextServicePacket.PacketType.UPDATE_TRIGGER_REPLY_MESSAGE,
				UpdateTriggerReply.class);
		typeMap.put(ContextServicePacket.PacketType.REFRESH_TRIGGER,
				RefreshTrigger.class);
		typeMap.put(ContextServicePacket.PacketType.CONFIG_REQUEST,
				ClientConfigRequest.class);
		typeMap.put(ContextServicePacket.PacketType.CONFIG_REPLY,
				ClientConfigReply.class);
		typeMap.put( ContextServicePacket.PacketType.ACLUPDATE_TO_SUBSPACE_REGION_MESSAGE,
				ACLUpdateToSubspaceRegionMessage.class );
		typeMap.put(ContextServicePacket.PacketType.ACLUPDATE_TO_SUBSPACE_REGION_REPLY_MESSAGE,
				ACLUpdateToSubspaceRegionReplyMessage.class);
		typeMap.put(ContextServicePacket.PacketType.NOOP_MEESAGE,
				NoopMessage.class);
		typeMap.put(ContextServicePacket.PacketType.NOOP_REPLY_MESSAGE,
				NoopReplyMessage.class);
		
		
		
		for( ContextServicePacket.PacketType type : ContextServicePacket.PacketType.intToType.values() )
		{
			assert(getPacketTypeClassName(type)!=null) : type;
		}
	}
	/**************************** End of ReconfigurationpacketType class map **************/

	protected ContextServicePacket(NodeIDType initiator)
	{
		super(initiator);
	}

	public ContextServicePacket(JSONObject json) throws JSONException
	{
		super(json);
		this.setType(getPacketType(json));
	}

	@Override
	public JSONObject toJSONObjectImpl() throws JSONException 
	{
		JSONObject json = new JSONObject();
		return json;
	}
	
	@Override
	public Object getMessage() 
	{
		return this;
	}

	@Override
	public PacketType getPacketType(JSONObject json)
			throws JSONException 
	{
		return getContextServicePacketType(json);
	}

	@Override
	public void putPacketType(JSONObject json, PacketType type)
			throws JSONException 
	{
		json.put(PACKET_TYPE, type.getInt());
	}
	
	public String toString() 
	{
		try 
		{
			return this.toJSONObject().toString();
		} catch(JSONException je) 
		{
			je.printStackTrace();
		}
		return null;
	}
	
	public static final ContextServicePacket.PacketType getContextServicePacketType(JSONObject json) throws JSONException
	{
		if( json.has(ContextServicePacket.PACKET_TYPE) )
			return ContextServicePacket.PacketType.intToType.get(json.getInt(PACKET_TYPE));
		else return null;
	}
	
	public static final String getPacketTypeClassName(ContextServicePacket.PacketType type) 
	{
		return typeMap.get(type)!=null ? typeMap.get(type).getSimpleName() : null;
	}

	public static BasicContextServicePacket<?> getContextServicePacket(JSONObject json, 
		Map<ContextServicePacket.PacketType,Class<?>> typeMap) throws JSONException 
	{
		BasicContextServicePacket<?> csPacket = null;
		
		try
		{
			ContextServicePacket.PacketType csType = 
					ContextServicePacket.PacketType.intToType.get(JSONPacket.getPacketType(json)); 
			
			if(csType!=null && getPacketTypeClassName(csType)!=null) 
			{
				csPacket = (BasicContextServicePacket<?>)(Class.forName(
						"edu.umass.cs.contextservice.messages." + 
				getPacketTypeClassName(csType)).getConstructor(JSONObject.class).newInstance(json));
			}
		}
		catch(NoSuchMethodException nsme) {nsme.printStackTrace();} 
		catch(InvocationTargetException ite) {ite.printStackTrace();} 
		catch(IllegalAccessException iae) {iae.printStackTrace();} 
		catch(ClassNotFoundException cnfe) {cnfe.printStackTrace();}
		catch(InstantiationException ie) {ie.printStackTrace();}
		
		return csPacket;
	}
	
	public static BasicContextServicePacket<?> getContextServicePacket(JSONObject json) throws JSONException
	{
		return getContextServicePacket(json, typeMap);
	}
	
	public PacketType getAllPacketTypes(JSONObject json)
			throws JSONException 
	{
		return getContextServicePacketType(json);
	}
	
	/************************* Start of assertion methods **************************************************/ 
	/* The assertion methods below are just convenience methods to let protocoltasks 
	 * assert that they have set up handlers for all packet types for which they
	 * are responsible.
	 */
	public static void assertPacketTypeChecks(ContextServicePacket.PacketType[] types, Class<?> target, String handlerMethodPrefix) 
	{
		for(ContextServicePacket.PacketType type : types) 
		{
			assertPacketTypeChecks(type, getPacketTypeClassName(type), target, handlerMethodPrefix);			
		}
	}
	
	public static void assertPacketTypeChecks(Map<ContextServicePacket.PacketType,Class<?>> typeMap, Class<?> target, 
			String handlerMethodPrefix) 
	{
		// Assertions ensure that method name changes do not break code.
		for(ContextServicePacket.PacketType type : typeMap.keySet()) 
		{
			assertPacketTypeChecks(type, getPacketTypeClassName(type), target, handlerMethodPrefix);
		}
	}
	
	public static void assertPacketTypeChecks(ContextServicePacket.PacketType type, String packetName, 
			Class<?> target, String handlerMethodPrefix) 
	{
		String errMsg = "Method " + handlerMethodPrefix+packetName +
				" does not exist in ReconfiguratorProtocolTask";
		try
		{
			ContextServiceLogger.getLogger().fine(type + " : " + packetName);
			if(packetName!=null)
				assert(target.getMethod(handlerMethodPrefix+packetName, 
					ProtocolEvent.class, ProtocolTask[].class)!=null) : 
						errMsg;
		} catch(NoSuchMethodException nsme) 
		{
			System.err.println(errMsg);
			nsme.printStackTrace();
		}
	}
	
	public static void assertPacketTypeChecks(Map<ContextServicePacket.PacketType,Class<?>> typeMap, Class<?> target) 
	{
		assertPacketTypeChecks(typeMap, target, HANDLER_METHOD_PREFIX);
	}
	
	public static ContextServicePacket.PacketType[] concatenate(ContextServicePacket.PacketType[]... types)
	{
		int size=0;
		for(ContextServicePacket.PacketType[] tarray : types) size += tarray.length;
		ContextServicePacket.PacketType[] allTypes = new ContextServicePacket.PacketType[size];
		int i=0;
		for(ContextServicePacket.PacketType[] tarray : types) 
		{
			for(ContextServicePacket.PacketType type : tarray) 
			{
				allTypes[i++] = type;
			}
		}
		return allTypes;
	}
	/************************* End of assertion methods **************************************************/
	
	public static void main(String[] args)
	{
		ContextServiceLogger.getLogger().fine( ContextServicePacket.PacketType.intToType.get(225).toString() );
	}
}