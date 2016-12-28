package edu.umass.cs.contextservice.schemes;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.ContextServiceProtocolTask;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.BasicContextServicePacket;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.updates.UpdateInfo;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.interfaces.PacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolExecutor;
import edu.umass.cs.protocoltask.ProtocolTask;


public abstract class AbstractScheme implements PacketDemultiplexer<JSONObject>
{
	protected final JSONMessenger<Integer> messenger;
	protected final ProtocolExecutor<Integer, ContextServicePacket.PacketType, String> protocolExecutor;
	protected final ContextServiceProtocolTask protocolTask;
	
	protected final Object numMesgLock	;
	
	protected final List<Integer> allNodeIDs;
	
	// stores the pending queries
	protected ConcurrentHashMap<Long, QueryInfo> pendingQueryRequests		= null;
	
	protected ConcurrentHashMap<Long, UpdateInfo> pendingUpdateRequests		= null;
	
	
	protected long updateIdCounter											= 0;
	
	protected final Object pendingUpdateLock								= new Object();
	
	
	// lock for synchronizing number of msg update
	protected long numMessagesInSystem										= 0;
	
	public static final Logger log = ContextServiceLogger.getLogger();
	
	
	public AbstractScheme(NodeConfig<Integer> nc, JSONMessenger<Integer> m)
	{
		this.numMesgLock = new Object();
		
		this.allNodeIDs = new LinkedList<Integer>();
		
		Set<Integer>	nodeIDSet = nc.getNodeIDs();
		
		Iterator<Integer> nodeIDIter = nodeIDSet.iterator();
		
		while( nodeIDIter.hasNext() )
		{
			Integer currNodeID = nodeIDIter.next();
			allNodeIDs.add(currNodeID);
		}
		
		pendingQueryRequests  = new ConcurrentHashMap<Long, QueryInfo>();
		
		pendingUpdateRequests = new ConcurrentHashMap<Long, UpdateInfo>();
		
		// initialize attribute types
		AttributeTypes.initialize();
		
		this.messenger = m;
		this.protocolExecutor = new ProtocolExecutor<Integer, ContextServicePacket.PacketType, String>(messenger);
		this.protocolTask = new ContextServiceProtocolTask(getMyID(), this);
		this.protocolExecutor.register(this.protocolTask.getEventTypes(), this.protocolTask);
	}
	
	// public methods
	public Set<ContextServicePacket.PacketType> getPacketTypes()
	{
		return this.protocolTask.getEventTypes();
	}
	
	public int getMyID()
	{
		return this.messenger.getMyID();
	}
	
	/**
	 * returns all nodeIDs
	 * @return
	 */
	public List<Integer> getAllNodeIDs()
	{
		return this.allNodeIDs;
	}
	
	public JSONMessenger<Integer> getJSONMessenger()
	{
		return messenger;
	}
	
	@Override
	public boolean handleMessage(JSONObject jsonObject, NIOHeader nioHeader) 
	{
		BasicContextServicePacket csPacket = null;
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
	
	protected void sendQueryReplyBackToUser(InetSocketAddress destAddress, QueryMsgFromUserReply qmesgUR)
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
		ValueUpdateFromGNSReply valUR
			= new ValueUpdateFromGNSReply(this.getMyID(), versioNum, versioNum);
		
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
	
	protected void sendRefreshReplyBackToUser(InetSocketAddress destSock, RefreshTrigger valUR)
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
	public abstract Integer getConsistentHashingNodeID( String stringToHash, 
												List<Integer> listOfNodesToConsistentlyHash);
	
	
	public abstract GenericMessagingTask<Integer,?>[] handleQueryMsgFromUser(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleValueUpdateFromGNS(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleQueryMesgToSubspaceRegion(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleQueryMesgToSubspaceRegionReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleValueUpdateToSubspaceRegionMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleGetMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleGetReplyMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleValueUpdateToSubspaceRegionReplyMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleQueryTriggerMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleUpdateTriggerMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleUpdateTriggerReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleClientConfigRequest(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleACLUpdateToSubspaceRegionMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
	
	public abstract GenericMessagingTask<Integer,?>[] handleACLUpdateToSubspaceRegionReplyMessage(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks);
}