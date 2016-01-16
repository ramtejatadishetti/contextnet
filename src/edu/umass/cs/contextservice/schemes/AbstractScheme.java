package edu.umass.cs.contextservice.schemes;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.ContextServiceProtocolTask;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.BasicContextServicePacket;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.updates.GUIDUpdateInfo;
import edu.umass.cs.contextservice.updates.UpdateInfo;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;


public abstract class AbstractScheme<NodeIDType> implements PacketDemultiplexer<JSONObject>
{
	protected final JSONMessenger<NodeIDType> messenger;
	protected final ProtocolExecutor<NodeIDType, ContextServicePacket.PacketType, String> protocolExecutor;
	protected final ContextServiceProtocolTask<NodeIDType> protocolTask;
	
	//protected final AbstractContextServiceDB<NodeIDType> contextserviceDB;
	
	protected final Object numMesgLock	;
	
	//private final List<AttributeMetadataInformation<NodeIDType>> attrMetaList;
	//private final List<AttributeValueInformation<NodeIDType>> attrValueList;
	protected final Set<NodeIDType> allNodeIDs;
	
	// stores the pending queries
	protected ConcurrentHashMap<Long, QueryInfo<NodeIDType>> pendingQueryRequests		= null;
	
	protected long queryIdCounter														= 0;
	
	protected final Object pendingQueryLock												= new Object();
	
	protected ConcurrentHashMap<Long, UpdateInfo<NodeIDType>> pendingUpdateRequests		= null;
	
	
	
	protected long updateIdCounter														= 0;
	
	protected final Object pendingUpdateLock											= new Object();
	
	protected  HyperspaceMySQLDB<NodeIDType> hyperspaceDB 								= null;
	// lock for synchronizing number of msg update
	protected long numMessagesInSystem													= 0;
	
	//protected  DatagramSocket client_socket;
	
	public static final Logger log = ContextServiceLogger.getLogger();
	
	
	public AbstractScheme(NodeConfig<NodeIDType> nc, JSONMessenger<NodeIDType> m)
	{
		this.numMesgLock = new Object();
		
		this.allNodeIDs = nc.getNodeIDs();
		
		pendingQueryRequests  = new ConcurrentHashMap<Long, QueryInfo<NodeIDType>>();
		
		pendingUpdateRequests = new ConcurrentHashMap<Long, UpdateInfo<NodeIDType>>();
		
		// initialize attribute types
		AttributeTypes.initialize();
		
		this.messenger = m;
		this.protocolExecutor = new ProtocolExecutor<NodeIDType, ContextServicePacket.PacketType, String>(messenger);
		this.protocolTask = new ContextServiceProtocolTask<NodeIDType>(getMyID(), this);
		this.protocolExecutor.register(this.protocolTask.getEventTypes(), this.protocolTask);
	}
	
	// public methods
	
	public Set<ContextServicePacket.PacketType> getPacketTypes()
	{
		return this.protocolTask.getEventTypes();
	}
	
	public NodeIDType getMyID()
	{
		return this.messenger.getMyID();
	}
	
	/**
	 * returns all nodeIDs
	 * @return
	 */
	public Set<NodeIDType> getAllNodeIDs()
	{
		return this.allNodeIDs;
	}
	
	public JSONMessenger<NodeIDType> getJSONMessenger()
	{
		return messenger;
	}
	
	/**
	 * java has issues converting LisnkedList.toArray(), that's why this function
	 * @return
	 */
	public GenericMessagingTask<NodeIDType, ?>[] convertLinkedListToArray(LinkedList<?> givenList)
	{
		@SuppressWarnings("unchecked")
		GenericMessagingTask<NodeIDType, ?>[] array = new GenericMessagingTask[givenList.size()];
		for(int i=0;i<givenList.size();i++)
		{
			array[i] = (GenericMessagingTask<NodeIDType, ?>) givenList.get(i);
		}
		return array;
	}
	
	@Override
	public boolean handleMessage(JSONObject jsonObject) 
	{
		BasicContextServicePacket<NodeIDType> csPacket = null;
		try
		{
			if( (csPacket = this.protocolTask.getContextServicePacket(jsonObject)) != null )
			{
				this.protocolExecutor.handleEvent(csPacket);
			}
		} catch(JSONException je)
		{
			je.printStackTrace();
		}
		return true;
	}
	
	public long getNumMesgInSystem()
	{
		return this.numMessagesInSystem;
	}
	
	protected void sendReplyBackToUser(QueryInfo<NodeIDType> qinfo, JSONArray resultList)
	{
		QueryMsgFromUserReply<NodeIDType> qmesgUR
			= new QueryMsgFromUserReply<NodeIDType>( this.getMyID(), qinfo.getQuery(), qinfo.getGroupGUID(),
					resultList, qinfo.getUserReqID(), resultList.length() );
		try
		{
			log.fine("sendReplyBackToUser "+qinfo.getUserIP()+" "+qinfo.getUserPort()+
					qmesgUR.toJSONObject());
			
			ContextServiceLogger.getLogger().fine("QUERY COMPLETE: sendReplyBackToUser "+qinfo.getUserIP()+" "+qinfo.getUserPort()+
					qmesgUR.toJSONObject());
			
			this.messenger.sendToAddress(new InetSocketAddress(InetAddress.getByName(qinfo.getUserIP()), qinfo.getUserPort())
								, qmesgUR.toJSONObject());
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	protected void sendQueryReplyBackToUser(InetSocketAddress destAddress, QueryMsgFromUserReply<NodeIDType> qmesgUR)
	{
		try
		{
			log.fine("sendReplyBackToUser "+destAddress+" "+ qmesgUR.toJSONObject());
			this.messenger.sendToAddress(destAddress, qmesgUR.toJSONObject());
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	protected void sendUpdateReplyBackToUser(String sourceIP, int sourcePort, long versioNum)
	{
		ValueUpdateFromGNSReply<NodeIDType> valUR
			= new ValueUpdateFromGNSReply<NodeIDType>(this.getMyID(), versioNum, versioNum);
		
		try
		{
			log.fine("sendUpdateReplyBackToUser "+sourceIP+" "+sourcePort+
					valUR.toJSONObject());
			
			this.messenger.sendToAddress(
					new InetSocketAddress(InetAddress.getByName(sourceIP), sourcePort)
								, valUR.toJSONObject());
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	protected void sendRefreshReplyBackToUser(InetSocketAddress destSock, RefreshTrigger<NodeIDType> valUR)
	{
		try
		{
			log.fine("sendRefreshReplyBackToUser "+destSock+
					valUR.toJSONObject());
			
			this.messenger.sendToAddress(destSock, valUR.toJSONObject());
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * spawns the protocol associated 
	 * spawning starts the start[] method
	 * of the protocol task
	 */
	public void spawnTheTask()
	{
		this.protocolExecutor.spawn(this.protocolTask);
	}
	
	// public abstract methods
	public abstract NodeIDType getResponsibleNodeId(String AttrName);
	
	
	
	public abstract GenericMessagingTask<NodeIDType,?>[] handleQueryMsgFromUser(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<NodeIDType,?>[] handleValueUpdateFromGNS(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<NodeIDType,?>[] handleQueryMesgToSubspaceRegion(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<NodeIDType,?>[] handleQueryMesgToSubspaceRegionReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<NodeIDType,?>[] handleValueUpdateToSubspaceRegionMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<NodeIDType,?>[] handleGetMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<NodeIDType,?>[] handleGetReplyMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<NodeIDType,?>[] handleValueUpdateToSubspaceRegionReplyMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<NodeIDType,?>[] handleQueryTriggerMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<NodeIDType,?>[] handleUpdateTriggerMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<NodeIDType,?>[] handleUpdateTriggerReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<NodeIDType,?>[] handleClientConfigRequest(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract void checkQueryCompletion(QueryInfo<NodeIDType> qinfo);
}