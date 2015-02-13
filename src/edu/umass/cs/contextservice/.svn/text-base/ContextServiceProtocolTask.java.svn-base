package edu.umass.cs.contextservice;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.hash.Hashing;


import edu.umass.cs.contextservice.database.records.AttributeMetaObjectRecord;
import edu.umass.cs.contextservice.database.records.AttributeMetadataInfoRecord;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.BasicContextServicePacket;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.MetadataMsgToValuenode;
import edu.umass.cs.contextservice.schemes.AbstractScheme;
import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolTask;

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
	
	/*private static final ContextServicePacket.PacketType[] localTypes = {
		ContextServicePacket.PacketType.DEMAND_REPORT,
		ContextServicePacket.PacketType.CREATE_SERVICE_NAME,
		ContextServicePacket.PacketType.DELETE_SERVICE_NAME,
	};*/
	
	//private static final ContextServicePacket.PacketType[] types =
	//	ContextServicePacket.PacketType.getPacketTypes();
	private static final List<ContextServicePacket.PacketType> types =
				ContextServicePacket.PacketType.getPacketTypes();
		//(ContextServicePacket.PacketType[]) ContextServicePacket.PacketType.intToType.values().toArray();
		/*ReconfigurationPacket.concatenate(localTypes, 
		WaitAckStopEpoch.types, 
		WaitAckStartEpoch.types, 
		WaitAckDropEpoch.type
		);*/

	//static 
	//{ // all but DEMAND_REPORT are handled by temporary protocol tasks
	//	ReconfigurationPacket.assertPacketTypeChecks(localTypes, Reconfigurator.class, HANDLER_METHOD_PREFIX); 
	//}

	private String key = null;
	private final NodeIDType myID;
	private final AbstractScheme<NodeIDType> csNode;

	public ContextServiceProtocolTask(NodeIDType id, AbstractScheme<NodeIDType> csNode)
	{
		this.myID = id;
		this.csNode = csNode;
	}

	@Override
	public String getKey()
	{
		return this.key;
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
	
	@Override
	public String refreshKey() 
	{
		return (this.key =
				(this.myID.toString() + (int) (Math.random() * Integer.MAX_VALUE)));
	}
	
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
}