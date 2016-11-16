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
 * @param <Integer>
 */
public class ContextServiceProtocolTask implements ProtocolTask<Integer, ContextServicePacket.PacketType, String> 
{
	private static final String HANDLER_METHOD_PREFIX = ContextServicePacket.HANDLER_METHOD_PREFIX; // could be any String as scope is local
	
	private static final List<ContextServicePacket.PacketType> types =
				ContextServicePacket.PacketType.getPacketTypes();
	
	private String key = "contextserviceKey";
	private final AbstractScheme csNode;

	public ContextServiceProtocolTask(Integer id, AbstractScheme csNode)
	{
		this.csNode = csNode;
	}

	@Override
	public String getKey()
	{
		return this.key;
	}
	
	@Override
	public Set<ContextServicePacket.PacketType> getEventTypes()
	{
		//return new HashSet<ContextServicePacket.PacketType>(Arrays.asList(types));
		return new HashSet<ContextServicePacket.PacketType>(types);
	}

	@SuppressWarnings("unchecked")
	@Override
	public GenericMessagingTask<Integer, ?>[] handleEvent(
		ProtocolEvent<ContextServicePacket.PacketType, String> event,
		ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		ContextServicePacket.PacketType type = event.getType();
		Object returnValue = null;
		try
		{
			//ContextServiceLogger.getLogger().fine("ContextServicePacket.getPacketTypeClassName(type) "+ContextServicePacket.getPacketTypeClassName(type));
			returnValue = this.csNode.getClass().getMethod(HANDLER_METHOD_PREFIX+
				ContextServicePacket.getPacketTypeClassName(type), ProtocolEvent.class, 
				ProtocolTask[].class).invoke(this.csNode, 
					(BasicContextServicePacket)event, ptasks);
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
		return (GenericMessagingTask<Integer, ?>[])returnValue;
	}
	
	public BasicContextServicePacket getContextServicePacket(JSONObject json) throws JSONException
	{
		return (BasicContextServicePacket)ContextServicePacket.getContextServicePacket(json);
	}
	
	public static void main(String[] args)
	{
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] start() 
	{
		return null;
	}
}