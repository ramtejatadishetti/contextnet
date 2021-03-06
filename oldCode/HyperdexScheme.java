package edu.umass.cs.contextservice.schemes.old;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.hyperdex.client.Client;
import org.hyperdex.client.Deferred;
import org.hyperdex.client.HyperDexClientException;
import org.hyperdex.client.Iterator;
import org.hyperdex.client.Range;
import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.gns.GNSCalls;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ContextServicePacket.PacketType;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgToMetadataNode;
import edu.umass.cs.contextservice.messages.QueryMsgToValuenodeReply;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateMsgToValuenode;
import edu.umass.cs.contextservice.queryparsing.QueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.queryparsing.QueryParser;
import edu.umass.cs.contextservice.queryparsing.UpdateInfo;
import edu.umass.cs.contextservice.schemes.AbstractScheme;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.InterfaceNodeConfig;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;

public class HyperdexScheme<Integer>  extends AbstractScheme<Integer>
{
	// all hyperdex related constants
	public static final String HYPERDEX_IP_ADDRESS 				= "compute-0-23";
	public static final int HYPERDEX_PORT				 		= 4999;
	public static final String HYPERDEX_SPACE					= "contextnet";
	// guid is the key in hyperdex
	public static final String HYPERDEX_KEY_NAME				= "GUID";
	
	public static final int NUM_PARALLEL_CLIENTS				= 50;
	
	private final Client[] hyperdexClientArray					= new Client[NUM_PARALLEL_CLIENTS];
	
	private final ConcurrentLinkedQueue<Client> freeHClientQueue;
	
	private final Object hclientFreeMonitor						= new Object();
	
	
	public HyperdexScheme(InterfaceNodeConfig<Integer> nc,
			JSONMessenger<Integer> m)
	{
		super(nc, m);
		
		freeHClientQueue = new ConcurrentLinkedQueue<Client>();
		
		for(int i=0;i<NUM_PARALLEL_CLIENTS;i++)
		{
			hyperdexClientArray[i] = new Client(HYPERDEX_IP_ADDRESS, HYPERDEX_PORT);
			
			freeHClientQueue.add(hyperdexClientArray[i]);
		}
		//hyperdexES = Executors.newFixedThreadPool(NUM_PARALLEL_CLIENTS);
		//hyperdexClient = new Client(HYPERDEX_IP_ADDRESS, HYPERDEX_PORT);
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryMsgFromUser(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		/* Actions:
		 * - send it to query processing system, where it parses it
		 * and sends it to corresponding metadata nodes
		 */
		@SuppressWarnings("unchecked")
		QueryMsgFromUser<Integer> queryMsgFromUser = (QueryMsgFromUser<Integer>)event;
		
		GenericMessagingTask<Integer, QueryMsgToMetadataNode<Integer>>[] retMsgs =
				processQueryMsgFromUser(queryMsgFromUser);
		
		synchronized(this.numMesgLock)
		{
			if(retMsgs != null)
			{
				this.numMessagesInSystem += retMsgs.length;
			}
		}
		return retMsgs;
	}
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateFromGNS(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		@SuppressWarnings("unchecked")
		ValueUpdateFromGNS<Integer> valUpdMsgFromGNS = (ValueUpdateFromGNS<Integer>)event;
		
		GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>>[] retMsgs 
			= this.processValueUpdateFromGNS(valUpdMsgFromGNS);
		
		synchronized(this.numMesgLock)
		{
			if(retMsgs != null)
			{
				this.numMessagesInSystem+=retMsgs.length;
			}
		}
		return retMsgs;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleValueUpdateMsgToValuenodeReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		return null;
	}
	
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleMetadataMsgToValuenode(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryMsgToMetadataNode(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryMsgToValuenode(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryMsgToValuenodeReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleValueUpdateMsgToMetadataNode(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleValueUpdateMsgToValuenode(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		return null;
	}
	
	@Override
	public void checkQueryCompletion(QueryInfo<Integer> qinfo)
	{
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] initializeScheme()
	{
		return null;
	}
	
	@Override
	protected void processReplyInternally(
			QueryMsgToValuenodeReply<Integer> queryMsgToValnodeRep,
			QueryInfo<Integer> queryInfo)
	{
	}
	
	@Override
	public Integer getResponsibleNodeId(String AttrName)
	{
		return null;
	}
	
	/**
	 * Query req received here means that
	 * no group exists in the GNS
	 * @param queryMsgFromUser
	 * @return
	 */
	public GenericMessagingTask<Integer, QueryMsgToMetadataNode<Integer>>[] 
			processQueryMsgFromUser(QueryMsgFromUser<Integer> queryMsgFromUser)
	{
		String query = queryMsgFromUser.getQuery();
		long userReqID = queryMsgFromUser.getUserReqNum();
		String userIP = queryMsgFromUser.getSourceIP();
		int userPort = queryMsgFromUser.getSourcePort();
		
		ContextServiceLogger.getLogger().fine("QUERY RECVD QUERY_MSG recvd query recvd "+query);
		
		//long queryStart = System.currentTimeMillis();
		// create the empty group in GNS
		String grpGUID = GNSCalls.createQueryGroup(query);
		
		Vector<QueryComponent> qcomponents = QueryParser.parseQuery(query);
		QueryInfo<Integer> currReq = new QueryInfo<Integer>(query, getMyID()
				, grpGUID, userReqID, userIP, userPort, qcomponents);;
		
		
		synchronized( this.pendingQueryLock )
		{
			currReq.setQueryRequestID(queryIdCounter++);
			pendingQueryRequests.put(currReq.getRequestId(), currReq);
		}
		
		long startHyper = System.currentTimeMillis();
		
		/*Map<String, Object> predicates = getHyperdexPredicates(qcomponents);
		Iterator resultIterator = null;
		JSONArray queryAnswer = null;
		
		/*synchronized(hClientLock)
		{
			//hyperdex returns an iterator without blocking. But the iterator blocks later 
			// on while doing the iteration
			resultIterator = this.hyperdexClient.search(HYPERDEX_SPACE, predicates);
			ContextServiceLogger.getLogger().fine("Result iterator returns");
			queryAnswer = getGUIDsFromIterator(resultIterator);
		}*/
		
		//this.hyperdexES.execute(new HyperdexTaskClass(HyperdexTaskClass.SEARCH, currReq, null));
		new HyperdexTaskClass(HyperdexTaskClass.SEARCH, currReq, null).run();
		
		/*while( !currReq.getRequestCompl() )
		{
			synchronized(currReq)
			{
				try
				{
					currReq.wait();
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}*/
		
		long qprocessingTime = System.currentTimeMillis();
		ContextServiceLogger.getLogger().fine("getGUIDsFromIterator returns "
		+currReq.getHyperdexResults()+" HyperTime "+(qprocessingTime-startHyper));
		
		
		//FIXME: uncomment this, just for debugging
		GNSCalls.addGUIDsToGroup(currReq.getHyperdexResults(), query, grpGUID);
		
		//long queryEndTime = System.currentTimeMillis();
		
		sendReplyBackToUser(currReq, currReq.getHyperdexResults());
		
		synchronized(this.pendingQueryLock)
		{
			this.pendingQueryRequests.remove( currReq.getRequestId() );
		}
		return null;
	}
	
	@SuppressWarnings("unchecked")
	private JSONArray getGUIDsFromIterator(Iterator hyperdexResultIterator)
	{
		JSONArray guidJSON = new JSONArray();
		
		try
		{
			while( hyperdexResultIterator.hasNext() )
			{
				Map<String, Object> wholeObjectMap 
					= (Map<String, Object>) hyperdexResultIterator.next();
				String nodeGUID = wholeObjectMap.get(HYPERDEX_KEY_NAME).toString();
				guidJSON.put(nodeGUID);
			}
		} catch ( HyperDexClientException e )
		{
			e.printStackTrace();
		}
		return guidJSON;
	}
	
	private Map<String, Object> getHyperdexPredicates(Vector<QueryComponent> qcomponents)
	{
		Map<String, Object> checks = new HashMap<String, Object>();
		
		for(int i=0; i<qcomponents.size(); i++)
		{
			QueryComponent currC = qcomponents.get(i);
			double lv = currC.getLeftValue();
			double rv = currC.getRightValue();
			checks.put( currC.getAttributeName(), new Range(lv, rv) );
		}
		return checks;
	}
	
	/**
	 * adds the reply of the queryComponent
	 * @throws JSONException
	 */
	private GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>>[]
	                   processValueUpdateFromGNS(ValueUpdateFromGNS<Integer> valUpdMsgFromGNS)
	{
		ContextServiceLogger.getLogger().info("\n\n Recvd ValueUpdateFromGNS at " 
				+ this.getMyID() +" reply "+valUpdMsgFromGNS);
		
		long versionNum = valUpdMsgFromGNS.getVersionNum();
		//String GUID = valUpdMsgFromGNS.getGUID();
		//String attrName = valUpdMsgFromGNS.getAttrName();
		//String oldVal = valUpdMsgFromGNS.getOldVal();
		//String newVal = valUpdMsgFromGNS.getNewVal();
		//JSONObject allAttrs = valUpdMsgFromGNS.getAllAttrs();
		//String sourceIP = valUpdMsgFromGNS.getSourceIP();
		//int sourcePort = valUpdMsgFromGNS.getSourcePort();
		
		/*double oldValD, newValD;
		
		if( oldVal.equals("") )
		{
			oldValD = AttributeTypes.NOT_SET;
		} else
		{
			oldValD = Double.parseDouble(oldVal);
		}
		newValD = Double.parseDouble(newVal );*/
		
		long currReqID = -1;
		
		/*UpdateInfo<Integer> currReq = null;
		
		synchronized(this.pendingUpdateLock)
		{
			currReq 
				= new UpdateInfo<Integer>(valUpdMsgFromGNS, updateIdCounter++);
			currReqID = currReq.getRequestId();
			pendingUpdateRequests.put(currReqID, currReq);
		}
		
		//this.hyperdexES.execute(new HyperdexTaskClass(HyperdexTaskClass.UPDATE, null, currReq));
		
		new HyperdexTaskClass(HyperdexTaskClass.UPDATE, null, currReq).run();*/
		
		/*while( !currReq.getUpdComl() )
		{
			synchronized(currReq)
			{
				try
				{
					currReq.wait();
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}*/
		
		/*Map<String, Object> attrs = new HashMap<String, Object>();
		attrs.put(attrName, newValD);
		Deferred asyncPut = null;
		synchronized( hClientLock )
		{
			try
			{
				asyncPut = this.hyperdexClient.async_put( HYPERDEX_SPACE, GUID, attrs );
				
				if( asyncPut != null )
				{
					try
					{
						asyncPut.waitForIt();
					} catch (HyperDexClientException e)
					{
						e.printStackTrace();
					}
				}
			} catch (HyperDexClientException e)
			{
				e.printStackTrace();
			}
		}*/
		
		//send reply back
		//sendUpdateReplyBackToUser(sourceIP, sourcePort, versionNum );
		
		// send refresh trigger to a writer, just for experiment
		/*if( ContextServiceConfig.GROUP_UPDATE_TRIGGER )
		{
			sendRefreshReplyBackToUser("compute-0-23", 5000, 
				"groupQuery", "groupGUID", versionNum);
		}*/
		
		/*synchronized(this.pendingUpdateLock)
		{
			pendingUpdateRequests.remove(currReqID);
		}*/
		return null;
	}
	
	
	private class HyperdexTaskClass implements Runnable
	{
		public static final int SEARCH			= 1;
		public static final int UPDATE			= 2;
		
		//private boolean running					= true;
		
		private final int getOrUpdate;
		private final QueryInfo<Integer> queryReq;
		private final UpdateInfo<Integer> updateReq;
		
		public HyperdexTaskClass( int getOrUpdate, QueryInfo<Integer> queryReq, UpdateInfo<Integer> updateReq )
		{
			this.getOrUpdate = getOrUpdate;
			this.queryReq = queryReq;
			this.updateReq = updateReq;
		}
		
		@Override
		public void run()
		{
			Client HClinetFree = null;
			
			while( HClinetFree == null )
			{
				HClinetFree = freeHClientQueue.poll();
				
				if( HClinetFree == null )
				{
					synchronized(hclientFreeMonitor)
					{
						try
						{
							hclientFreeMonitor.wait();
						} catch (InterruptedException e)
						{
							e.printStackTrace();
						}
					}
				}
			}
			
			switch(getOrUpdate)
			{
				case SEARCH:
				{
					//hyperdex returns an iterator without blocking. But the iterator blocks later 
					// on while doing the iteration
					
					Map<String, Object> predicates = getHyperdexPredicates(queryReq.queryComponents);
					
					long searchStart = System.currentTimeMillis();
					Iterator resultIterator = HClinetFree.search(HYPERDEX_SPACE, predicates);
					long searchEnd = System.currentTimeMillis();
					
					
					JSONArray queryAnswer = getGUIDsFromIterator(resultIterator);
					long iterTime = System.currentTimeMillis();
					
					ContextServiceLogger.getLogger().fine( "Search time search "+(searchEnd-searchStart)+
							" Iter time "+(iterTime-searchEnd) );
					
					queryReq.setHyperdexResults(queryAnswer);
					
					synchronized(queryReq)
					{
						queryReq.setRequestCompl();
						queryReq.notifyAll();
					}
					
					break;
				}
				
				case UPDATE:
				{
					try
					{
						//String attrName = updateReq.getValueUpdateFromGNS().getAttrName();
						String attrName = "";
						//double newVal = Double.parseDouble(updateReq.getValueUpdateFromGNS().getNewVal());
						double newVal = Double.MIN_VALUE;
						String GUID = updateReq.getValueUpdateFromGNS().getGUID();
						
						Map<String, Object> attrs = new HashMap<String, Object>();
						attrs.put(attrName, newVal);
						Deferred asyncPut = null;
						
						asyncPut = HClinetFree.async_put( HYPERDEX_SPACE, GUID, attrs );
						
						if( asyncPut != null )
						{
							try
							{
								asyncPut.waitForIt();
							} catch (HyperDexClientException e)
							{
								e.printStackTrace();
							}
						}
					} catch (HyperDexClientException e)
					{
						e.printStackTrace();
					}
					
					synchronized(updateReq)
					{
						updateReq.setUpdCompl();
						updateReq.notifyAll();
					}
					
					break;
				}
			}
			
			synchronized(hclientFreeMonitor)
			{
				freeHClientQueue.add(HClinetFree);
				hclientFreeMonitor.notifyAll();
			}
		}
	}


	@Override
	public GenericMessagingTask<Integer, ?>[] handleBulkGet(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleBulkGetReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleConsistentStoragePut(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleConsistentStoragePutReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryMesgToSubspaceRegion(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryMesgToSubspaceRegionReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleValueUpdateToSubspaceRegionMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleGetMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleGetReplyMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleValueUpdateToSubspaceRegionReplyMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryTriggerMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleUpdateTriggerMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleUpdateTriggerReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}
}