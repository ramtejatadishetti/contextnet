package edu.umass.cs.contextservice;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.json.JSONException;
import org.json.JSONObject;


import edu.umass.cs.contextservice.messages.BasicContextServicePacket;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.schemes.AbstractScheme;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;

/**
 * Protocol task for the whole context service
 * @author adipc
 *
 * @param <NodeIDType>
 */
public class ContextServiceProtocolTask<NodeIDType> implements 
ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String> 
{
	private static final String HANDLER_METHOD_PREFIX = ContextServicePacket.HANDLER_METHOD_PREFIX; // could be any String as scope is local
	
	private static final List<ContextServicePacket.PacketType> types =
				ContextServicePacket.PacketType.getPacketTypes();
	
	private String key = "contextserviceKey";
	private final AbstractScheme<NodeIDType> csNode;

	public ContextServiceProtocolTask(NodeIDType id, AbstractScheme<NodeIDType> csNode)
	{
		this.csNode = csNode;
	}

	@Override
	public String getKey()
	{
		return this.key;
	}
	
	/*@Override
	public String refreshKey()
	{
		return (this.key =
				(this.myID.toString() + (int) (Math.random() * Integer.MAX_VALUE)));
	}*/
	
	@Override
	public Set<ContextServicePacket.PacketType> getEventTypes()
	{
		//return new HashSet<ContextServicePacket.PacketType>(Arrays.asList(types));
		return new HashSet<ContextServicePacket.PacketType>(types);
	}
	
	/*public Set<ReconfigurationPacket.PacketType> getDefaultTypes() 
	{
		return new HashSet<ReconfigurationPacket.PacketType>(Arrays.asList(localTypes));
	}*/

	@SuppressWarnings("unchecked")
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleEvent(
		ProtocolEvent<ContextServicePacket.PacketType, String> event,
		ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		ContextServicePacket.PacketType type = event.getType();
		Object returnValue = null;
		try
		{
			//System.out.println("ContextServicePacket.getPacketTypeClassName(type) "+ContextServicePacket.getPacketTypeClassName(type));
			returnValue = this.csNode.getClass().getMethod(HANDLER_METHOD_PREFIX+
				ContextServicePacket.getPacketTypeClassName(type), ProtocolEvent.class, 
				ProtocolTask[].class).invoke(this.csNode, 
					(BasicContextServicePacket<?>)event, ptasks);
		} catch(NoSuchMethodException nsme)
		{
			nsme.printStackTrace();
		} catch(InvocationTargetException ite)
		{
			ite.printStackTrace();
		} catch(IllegalAccessException iae)
		{
			iae.printStackTrace();
		}
		return (GenericMessagingTask<NodeIDType, ?>[])returnValue;
	}
	
	@SuppressWarnings("unchecked")
	public BasicContextServicePacket<NodeIDType> getContextServicePacket(JSONObject json) throws JSONException
	{
		return (BasicContextServicePacket<NodeIDType>)ContextServicePacket.getContextServicePacket(json);
	}
	
	public static void main(String[] args)
	{
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] start() 
	{
		/**
		 * In the beginning of the protocol, each node finds for what 
		 * attributes it is a meta data node and sends the MetadataMsgToValueNode.
		 * assignValueRanges() returns the messaging tasks that need to run.
		 */
		return csNode.initializeScheme();
	}
}