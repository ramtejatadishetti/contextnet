package edu.umass.cs.contextservice.schemes;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import com.google.common.hash.Hashing;

import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.common.CSNodeConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.AbstractDataStorageDB;
import edu.umass.cs.contextservice.database.RegionMappingDataStorageDB;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource;
import edu.umass.cs.contextservice.database.datasource.MySQLDataSource;
import edu.umass.cs.contextservice.database.datasource.SQLiteDataSource;
import edu.umass.cs.contextservice.database.triggers.GroupGUIDInfoClass;
import edu.umass.cs.contextservice.gns.GNSCalls;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ClientConfigReply;
import edu.umass.cs.contextservice.messages.ClientConfigRequest;
import edu.umass.cs.contextservice.messages.ContextServicePacket.PacketType;
import edu.umass.cs.contextservice.messages.GetMessage;
import edu.umass.cs.contextservice.messages.GetReplyMessage;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionReplyMessage;
import edu.umass.cs.contextservice.profilers.ProfilerStatClass;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.regionmapper.AbstractRegionMappingPolicy;
import edu.umass.cs.contextservice.regionmapper.FileBasedRegionMappingPolicy;
import edu.umass.cs.contextservice.regionmapper.HyperdexBasedRegionMappingPolicy;
import edu.umass.cs.contextservice.regionmapper.SqrtNConsistentHashingPolicy;
import edu.umass.cs.contextservice.regionmapper.UniformGreedyRegionMappingPolicyWithDB;
import edu.umass.cs.contextservice.schemes.components.AbstractGUIDAttrValueProcessing;
import edu.umass.cs.contextservice.schemes.components.GUIDAttrValueProcessing;
import edu.umass.cs.contextservice.schemes.components.TriggerProcessing;
import edu.umass.cs.contextservice.schemes.components.TriggerProcessingInterface;
import edu.umass.cs.contextservice.updates.GUIDUpdateInfo;
import edu.umass.cs.contextservice.updates.UpdateInfo;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;

public class RegionMappingBasedScheme extends AbstractScheme
{
	private  AbstractDataStorageDB hyperspaceDB 							= null;
	private final ExecutorService nodeES;
	
	private HashMap<String, GUIDUpdateInfo> guidUpdateInfoMap				= null;
	
	private final AbstractRegionMappingPolicy regionMappingPolicy;
	
	private final AbstractGUIDAttrValueProcessing guidAttrValProcessing;
	
	
	private TriggerProcessingInterface triggerProcessing;
	
	private ProfilerStatClass profStats;
	
	private AbstractDataSource dataSource;
	
	private HashMap<String, Boolean> groupGUIDSyncMap;
	public static final Logger log 											= ContextServiceLogger.getLogger();
	
	
	public RegionMappingBasedScheme(CSNodeConfig nc, 
			JSONMessenger<Integer> m) throws Exception
	{
		super(nc, m);
		
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats = new ProfilerStatClass();
			new Thread(profStats).start();
		}

		if(ContextServiceConfig.sqlDBType == ContextServiceConfig.SQL_DB_TYPE.MYSQL)
		{
			dataSource = new MySQLDataSource(this.getMyID());
		}
		else if(ContextServiceConfig.sqlDBType == ContextServiceConfig.SQL_DB_TYPE.SQLITE)
		{
			dataSource = new SQLiteDataSource(this.getMyID());
		}
		
		nodeES = Executors.newFixedThreadPool(
				ContextServiceConfig.THREAD_POOL_SIZE);
		
		guidUpdateInfoMap = new HashMap<String, GUIDUpdateInfo>();
		
		switch(ContextServiceConfig.regionMappingPolicy)
		{
			case ContextServiceConfig.UNIFORM:
			{
				regionMappingPolicy = new UniformGreedyRegionMappingPolicyWithDB(
						dataSource, AttributeTypes.attributeMap, AttributeTypes.attributeInOrderList, nc);
				break;
			}
			case ContextServiceConfig.DEMAND_AWARE:
			{
				regionMappingPolicy = new FileBasedRegionMappingPolicy(
						AttributeTypes.attributeMap, nc);
				break;
			}
			case ContextServiceConfig.HYPERDEX:
			{
				regionMappingPolicy = new HyperdexBasedRegionMappingPolicy(
						AttributeTypes.attributeMap, nc, (int)ContextServiceConfig.numAttrsPerSubspace, 
						dataSource);
				break;
			}
			case ContextServiceConfig.SQRT_N_HASH:
			{
				regionMappingPolicy = new SqrtNConsistentHashingPolicy(AttributeTypes.attributeMap, nc);
				break;
			}
			default:
				regionMappingPolicy = null;
				assert(false);
		}
		
		
		regionMappingPolicy.computeRegionMapping();
		
		/*if(!ContextServiceConfig.disableOptimizer)
		{
			optimalHCalculator = new CalculateOptimalNumAttrsInSubspace(
					nc.getNodeIDs().size(),
						AttributeTypes.attributeMap.size());
			
			if( optimalHCalculator.getBasicOrReplicated() )
			{
				subspaceConfigurator 
					= new BasicSubspaceConfigurator(messenger.getNodeConfig(), 
							optimalHCalculator.getOptimalH() );
			}
			else
			{
				subspaceConfigurator 
					= new ReplicatedSubspaceConfigurator(messenger.getNodeConfig(), 
							optimalHCalculator.getOptimalH() );
			}
		}
		else
		{
			if(ContextServiceConfig.basicConfig)
			{
				subspaceConfigurator 
					= new BasicSubspaceConfigurator(messenger.getNodeConfig(), 
						(int)ContextServiceConfig.optimalH );
			}
			else
			{
				subspaceConfigurator 
					= new ReplicatedSubspaceConfigurator(messenger.getNodeConfig(), 
						(int)ContextServiceConfig.optimalH );
			}
		}*/	
//		ContextServiceLogger.getLogger().fine("configure subspace started");
//		// configure subspaces
//		subspaceConfigurator.configureSubspaceInfo();
//		ContextServiceLogger.getLogger().fine("configure subspace completed");
		
		hyperspaceDB = new RegionMappingDataStorageDB(this.getMyID(), dataSource);
		
//		ContextServiceLogger.getLogger().fine("HyperspaceMySQLDB completed");
//		
//		subspaceConfigurator.generateAndStoreSubspacePartitionsInDB
//					(nodeES, hyperspaceDB);
		
		guidAttrValProcessing 
					= new GUIDAttrValueProcessing(
				this.getMyID(), regionMappingPolicy, 
				hyperspaceDB, messenger , pendingQueryRequests , profStats);
		
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			if(ContextServiceConfig.UniqueGroupGUIDEnabled)
			{
				groupGUIDSyncMap = new HashMap<String, Boolean>();
			}
			triggerProcessing = new TriggerProcessing(this.getMyID(), 
				regionMappingPolicy, hyperspaceDB, messenger);
		}
		//new Thread(new ProfilerStatClass()).start();
	}
	
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryMsgFromUser(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementIncomingSearchRate();;
		}
		nodeES.execute(new HandleEventThread(event));
		return null;
	}
	
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleValueUpdateFromGNS(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryMesgToSubspaceRegion(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryMesgToSubspaceRegionReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleValueUpdateToSubspaceRegionMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleGetMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleGetReplyMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleValueUpdateToSubspaceRegionReplyMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryTriggerMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleUpdateTriggerMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleUpdateTriggerReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleClientConfigRequest(ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<Integer, ?>[] handleACLUpdateToSubspaceRegionMessage(
			ProtocolEvent<PacketType, String> event, 
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleACLUpdateToSubspaceRegionReplyMessage(
			ProtocolEvent<PacketType, String> event, 
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		nodeES.execute(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public Integer getConsistentHashingNodeID( String stringToHash , 
			List<Integer> listOfNodesToConsistentlyHash )
	{
		int numNodes = listOfNodesToConsistentlyHash.size();
		int mapIndex = Hashing.consistentHash(stringToHash.hashCode(), numNodes);
		return listOfNodesToConsistentlyHash.get(mapIndex);
	}
	
	
	private void processQueryMsgFromUser
		( QueryMsgFromUser queryMsgFromUser )
	{
		String query;
		String userIP;
		int userPort;
		long userReqID;
		long expiryTime;
		
		query      = queryMsgFromUser.getQuery();
		userReqID  = queryMsgFromUser.getUserReqNum();
		userIP     = queryMsgFromUser.getSourceIP();
		userPort   = queryMsgFromUser.getSourcePort();
		expiryTime = queryMsgFromUser.getExpiryTime();
		
		if(ContextServiceConfig.SEARCH_UPDATE_TRACE_ENABLE)
		{
			System.out.println("SEARCHQUERY "+query);
		}
		
		ContextServiceLogger.getLogger().fine("QUERY RECVD: QUERY_MSG recvd query recvd "+query);
		
		// create the empty group in GNS
		String grpGUID = GNSCalls.createQueryGroup(query);
		
		if( grpGUID.length() <= 0 )
		{
			ContextServiceLogger.getLogger().fine
			("Query request failed at the recieving node "+queryMsgFromUser);
			return;
		}
		
		String hashKey = grpGUID+":"+userIP+":"+userPort;
		// check for triggers, if those are enabled then forward the query to the node
		// which consistently hashes the the query:userIp:userPort string
		if( ContextServiceConfig.TRIGGER_ENABLED 
				&& ContextServiceConfig.UniqueGroupGUIDEnabled )
		{
			
			Integer respNodeId = this.getConsistentHashingNodeID(hashKey, this.allNodeIDs);
			
			// just forward the request to the node that has 
			// guid stored in primary subspace.
			if( this.getMyID() != respNodeId )
			{
				try
				{
					this.messenger.sendToID( respNodeId, queryMsgFromUser.toJSONObject() );
					return;
				} catch (IOException e)
				{
					e.printStackTrace();
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
			}
		}
		
		boolean storeQueryForTrigger = false;
		QueryInfo currReq  
			= new QueryInfo( query, this.getMyID(), grpGUID, userReqID, 
					userIP, userPort, expiryTime );
		
		
		if( ContextServiceConfig.TRIGGER_ENABLED && 
					ContextServiceConfig.UniqueGroupGUIDEnabled )
	    {
			synchronized(this.groupGUIDSyncMap)
			{
				while(groupGUIDSyncMap.get(hashKey) != null )
				{
					try {
						groupGUIDSyncMap.wait();
					} catch (InterruptedException e) 
					{
						e.printStackTrace();
					}
				}
				groupGUIDSyncMap.put(hashKey, new Boolean(true));
			}
			
	    	boolean found 
	    		= this.triggerProcessing.processTriggerOnQueryMsgFromUser(currReq);
	    	
	    	synchronized(this.groupGUIDSyncMap)
			{
				groupGUIDSyncMap.remove(hashKey);
				groupGUIDSyncMap.notify();
			}
	    	// if inserted first time then in secondary subspaces trigger info is stored for this query.
	    	storeQueryForTrigger = !found;
	    	
	    }
		else if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			storeQueryForTrigger = true;
		}
		
		guidAttrValProcessing.processQueryMsgFromUser
										(currReq, storeQueryForTrigger);
	}
	
	private void processValueUpdateFromGNS( ValueUpdateFromGNS valueUpdateFromGNS )
	{
		String GUID 			  		= valueUpdateFromGNS.getGUID();
		int respNodeId 	  			    = this.getConsistentHashingNodeID
													(GUID, this.allNodeIDs);
		
		
		// just forward the request to the node that has 
		// guid stored in primary subspace.
		if( this.getMyID() != respNodeId )
		{
//			if(respNodeId >= 128)
//			{
//				System.out.println("ID greater than 128 "+respNodeId);
//			}
			ContextServiceLogger.getLogger().fine("not primary node case souceIp "
													+ valueUpdateFromGNS.getSourceIP()
													+ " sourcePort "
													+ valueUpdateFromGNS.getSourcePort());
			try
			{
				this.messenger.sendToID( respNodeId, valueUpdateFromGNS.toJSONObject() );
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		else
		{	
//			if(respNodeId >= 128)
//			{
//				System.out.println("ID greater than 128 mesg recvd"+respNodeId);
//			}
			
			profStats.incrementIncomingUpdateRate();
			
			// this piece of code takes care of consistency. Updates to a 
			// GUID are serialized here. For a GUID only one update is outstanding at
			// time. But multiple GUIDs can be updated in parallel.
			ContextServiceLogger.getLogger().fine("primary node case souceIp "
								+valueUpdateFromGNS.getSourceIP()
								+" sourcePort "+valueUpdateFromGNS.getSourcePort());
			
			UpdateInfo updReq  				= null;
			long requestID 					= -1;
			// if no outstanding request then it is set to true
			boolean sendOutRequest 			= false;
			
			synchronized( this.pendingUpdateLock )
			{
				updReq = new UpdateInfo(valueUpdateFromGNS, updateIdCounter++);
				
				pendingUpdateRequests.put(updReq.getRequestId(), updReq);
				requestID = updReq.getRequestId();
				
				GUIDUpdateInfo guidUpdateInfo = this.guidUpdateInfoMap.get(GUID);
				
				if(guidUpdateInfo == null)
				{
					guidUpdateInfo = new GUIDUpdateInfo(GUID);
					guidUpdateInfo.addUpdateReqNumToQueue(requestID);
					this.guidUpdateInfoMap.put(GUID, guidUpdateInfo);
					sendOutRequest = true;
				}
				else
				{
					guidUpdateInfo.addUpdateReqNumToQueue(requestID);
					// no need to send out request. it will be sent once the current
					// outstanding gets completed
				}
			}
			
			if( sendOutRequest )
			{
				processUpdateSerially(updReq);
			}
		}
	}
	
	
	/**
	 * This function processes a request serially.
	 * when one outstanding request completes.
	 */
	private void processUpdateSerially(UpdateInfo updateReq)
	{
		assert(updateReq != null);
		
		try
		{
			ContextServiceLogger.getLogger().fine
					( "processUpdateSerially called "+updateReq.getRequestId() +
					" JSON"+updateReq.getValueUpdateFromGNS().toJSONObject().toString() );
		}
		catch(JSONException jso)
		{
			jso.printStackTrace();
		}
		
		guidAttrValProcessing.processUpdateFromGNS(updateReq);
	}
	
	public static JSONObject getUnsetAttrJSON(JSONObject attrValJSON)
	{
		JSONObject unsetAttrJSON = null;
		
		try
		{
			if( attrValJSON.has(RegionMappingDataStorageDB.unsetAttrsColName) )
			{
				String jsonString 
						= attrValJSON.getString( RegionMappingDataStorageDB.unsetAttrsColName );
				
				if( jsonString != null )
				{
					if( jsonString.length() > 0 )
					{
						unsetAttrJSON = new JSONObject(jsonString);
					}
				}
			}
		}
		catch(JSONException jsonException)
		{
			jsonException.printStackTrace();
		}
		return unsetAttrJSON;
	}
	
	public static boolean checkIfAnonymizedIDToGuidInfoAlreadyStored(JSONObject oldValJSON) 
					throws JSONException
	{
		boolean alreadyStored = false;
		if( oldValJSON.has(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName) )
		{
			String jsonArrayString = oldValJSON.getString
									(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName);
			
			if( jsonArrayString != null )
			{
				// We don't need to compare the actual value,
				// because this information change causes whole
				// anonymized ID to change, so anonymizedIDToGUIDMapping
				// either inserted or deleted, but never updated for 
				// an anonymized ID.
				if(jsonArrayString.length() > 0)
				{
					alreadyStored = true;
					return alreadyStored;
				}
			}
			return alreadyStored;
		}
		else
		{
			return alreadyStored;
		}
	}
	
	private void processGetMessage(GetMessage getMessage)
	{
		String GUID 			  = getMessage.getGUIDsToGet();
		Integer respNodeId 	  = this.getConsistentHashingNodeID(GUID, this.allNodeIDs);
		
		// just forward the request to the node that has 
		// guid stored in primary subspace.
		if( this.getMyID() != respNodeId )
		{
			try
			{
				this.messenger.sendToID( respNodeId, getMessage.toJSONObject() );
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			//String tableName = "primarySubspaceDataStorage";
			JSONObject valueJSON= this.hyperspaceDB.getGUIDStoredUsingHashIndex(GUID);
			
			
			GetReplyMessage getReplyMessage = new GetReplyMessage(this.getMyID(),
					getMessage.getUserReqID(), GUID, valueJSON);
			
			try
			{
				this.messenger.sendToAddress( new InetSocketAddress(getMessage.getSourceIP(), getMessage.getSourcePort()), 
						getReplyMessage.toJSONObject() );
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	
	private void processValueUpdateToSubspaceRegionMessageReply
		( ValueUpdateToSubspaceRegionReplyMessage 
		valueUpdateToSubspaceRegionReplyMessage )
	{
		long requestID  = valueUpdateToSubspaceRegionReplyMessage.getRequestID();
		
		JSONArray toBeRemovedGroups 
						= valueUpdateToSubspaceRegionReplyMessage.getToBeRemovedGroups();
		
		JSONArray toBeAddedGroups 
						= valueUpdateToSubspaceRegionReplyMessage.getToBeAddedGroups();
		
		
		UpdateInfo updInfo = pendingUpdateRequests.get(requestID);
		
		if( updInfo == null )
		{
			ContextServiceLogger.getLogger().severe( "updInfo null, update already removed from "
					+ " the pending queue before recv all replies requestID "+requestID
					+ "  valueUpdateToSubspaceRegionReplyMessage "
					+ valueUpdateToSubspaceRegionReplyMessage );
			assert(false);
		}
		boolean completion = updInfo.setUpdateReply( toBeRemovedGroups, toBeAddedGroups);
		
		if( completion )
		{
			ValueUpdateFromGNSReply valueUpdateFromGNSReply = new ValueUpdateFromGNSReply
			(this.getMyID(), updInfo.getValueUpdateFromGNS().getVersionNum(), updInfo.getValueUpdateFromGNS().getUserRequestID());
			
			ContextServiceLogger.getLogger().fine("reply IP Port "+updInfo.getValueUpdateFromGNS().getSourceIP()
					+":"+updInfo.getValueUpdateFromGNS().getSourcePort()+ " ValueUpdateFromGNSReply for requestId "+requestID
					+" "+valueUpdateFromGNSReply+" ValueUpdateToSubspaceRegionReplyMessage "+valueUpdateToSubspaceRegionReplyMessage);
			try
			{
				this.messenger.sendToAddress( new InetSocketAddress(updInfo.getValueUpdateFromGNS().getSourceIP()
						, updInfo.getValueUpdateFromGNS().getSourcePort()), 
						valueUpdateFromGNSReply.toJSONObject() );
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			
			if(ContextServiceConfig.TRIGGER_ENABLED)
			{
				try
				{
					this.triggerProcessing.sendOutAggregatedRefreshTrigger
						( updInfo.getToBeRemovedMap(), 
						  updInfo.getToBeAddedMap(), updInfo.getValueUpdateFromGNS().getGUID(), 
						  updInfo.getValueUpdateFromGNS().getVersionNum(), 
						  updInfo.getValueUpdateFromGNS().getUpdateStartTime() );
				} 
				catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
			
			
			UpdateInfo removedUpdate 
					=  pendingUpdateRequests.remove(requestID);;
			
			// starts the queues serialized updates for that guid
			if(removedUpdate != null)
			{
				startANewUpdate(removedUpdate, requestID);
			}
		}
	}
	
	private void startANewUpdate(UpdateInfo removedUpdate, long requestID)
	{
		boolean startANewUpdate = false;
		Long nextRequestID = null;
		synchronized( this.pendingUpdateLock )
		{
			// remove from guidUpdateInfo
			GUIDUpdateInfo guidUpdateInfo = 
					this.guidUpdateInfoMap.get(removedUpdate.getValueUpdateFromGNS().getGUID());
			
			assert(guidUpdateInfo!=null);
			Long currRequestID = guidUpdateInfo.removeFromQueue();
			// it must not be null
			assert(currRequestID != null);
			// it should be same as current requestID
			assert(requestID == currRequestID);
			
			// get the next requestID
			nextRequestID = guidUpdateInfo.getNextRequestID();
			if(nextRequestID == null)
			{
				// remove the guidUpdateInfo, there are no more updates for this GUID
				this.guidUpdateInfoMap.remove(removedUpdate.getValueUpdateFromGNS().getGUID());
			}
			else
			{
				// start a new update serially outside the lock
				startANewUpdate = true;
			}
		}
		
		if(startANewUpdate)
		{
			assert(nextRequestID != null);
			this.processUpdateSerially(pendingUpdateRequests.get(nextRequestID));
		}
	}
	
	private void processClientConfigRequest(ClientConfigRequest 
																clientConfigRequest)
	{
		JSONArray nodeConfigArray 		= new JSONArray();
		JSONArray attributeArray  		= new JSONArray();
		// Each element is a JSONArray of attrbutes for a subspace
		JSONArray subspaceConfigArray   = new JSONArray();
		
		Iterator<Integer> nodeIDIter = this.allNodeIDs.iterator();
		
		while( nodeIDIter.hasNext() )
		{
			Integer nodeId = nodeIDIter.next();
			InetAddress nodeAddress = this.messenger.getNodeConfig().getNodeAddress(nodeId);
			int nodePort = this.messenger.getNodeConfig().getNodePort(nodeId);
			String ipPortString = nodeAddress.getHostAddress()+":"+nodePort;
			nodeConfigArray.put(ipPortString);
		}
		
		Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
		
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			attributeArray.put(attrName);
		}
		
//		HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap = 
//				this.subspaceConfigurator.getSubspaceInfoMap();
//		
//		assert(subspaceInfoMap != null);
//		
//		Iterator<Integer> subspaceIter = subspaceInfoMap.keySet().iterator();
//		
//		while( subspaceIter.hasNext() )
//		{
//			// taking first replica, as we need distinct subspace attrs
//			Vector<SubspaceInfo> subsapceInfoVect 
//												= subspaceInfoMap.get( subspaceIter.next() );
//			
//			SubspaceInfo currSubspaceInfo = subsapceInfoVect.get(0);
//			
//			JSONArray attrsOfSubspace = new JSONArray();
//			HashMap<String, AttributePartitionInfo> currSubInfoMap = 
//								currSubspaceInfo.getAttributesOfSubspace();
//			
//			attrIter = currSubInfoMap.keySet().iterator();
//			
//			while( attrIter.hasNext() )
//			{
//				String attrName = attrIter.next();
//				attrsOfSubspace.put(attrName);
//			}
//			subspaceConfigArray.put(attrsOfSubspace);
//		}
		
		InetSocketAddress sourceSocketAddr = new InetSocketAddress
				(clientConfigRequest.getSourceIP(),
				clientConfigRequest.getSourcePort());
		ClientConfigReply configReply 
					= new ClientConfigReply( this.getMyID(), nodeConfigArray,
							attributeArray, subspaceConfigArray );
		try
		{
			this.messenger.sendToAddress( sourceSocketAddr, configReply.toJSONObject() );
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	
	private void processQueryMesgToSubspaceRegion(QueryMesgToSubspaceRegion 
														queryMesgToSubspaceRegion)
	{
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementNumSearchesAttrIndex();;
		}
		
		String groupGUID 			 = queryMesgToSubspaceRegion.getGroupGUID();
		boolean storeQueryForTrigger = queryMesgToSubspaceRegion.getStoreQueryForTrigger();
		
		JSONArray resultGUIDArray    = new JSONArray();
		
		int privacyScheme 			 = queryMesgToSubspaceRegion.getPrivacyOrdinal();
		int resultSize = -1;
		
		resultSize = this.guidAttrValProcessing.processQueryMesgToSubspaceRegion
				(queryMesgToSubspaceRegion, resultGUIDArray);
		
		
		if(storeQueryForTrigger)
		{
			assert(ContextServiceConfig.TRIGGER_ENABLED);
			this.triggerProcessing.processQuerySubspaceRegionMessageForTrigger
					(queryMesgToSubspaceRegion);	
		}
		
		QueryMesgToSubspaceRegionReply queryMesgToSubspaceRegionReply = 
		new QueryMesgToSubspaceRegionReply( this.getMyID(), 
				queryMesgToSubspaceRegion.getRequestId(), 
				groupGUID, resultGUIDArray, resultSize, privacyScheme);
		
		
		try
		{
			this.messenger.sendToID(queryMesgToSubspaceRegion.getSender(), 
					queryMesgToSubspaceRegionReply.toJSONObject());
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		ContextServiceLogger.getLogger().info("Sending queryMesgToSubspaceRegionReply "
				+ " mesg from " + this.getMyID() +" to node "
				+queryMesgToSubspaceRegion.getSender());
	}
	
	
	private void processValueUpdateToSubspaceRegionMessage(
				ValueUpdateToSubspaceRegionMessage 
				valueUpdateToSubspaceRegionMessage )
	{
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementNumUpdatesAttrIndex();
		}
		
		this.guidAttrValProcessing.processValueUpdateToSubspaceRegionMessage
					( valueUpdateToSubspaceRegionMessage);
		
		HashMap<String, GroupGUIDInfoClass> removedGroups 
							= new HashMap<String, GroupGUIDInfoClass>();
		HashMap<String, GroupGUIDInfoClass> addedGroups 
							= new HashMap<String, GroupGUIDInfoClass>();
		
		JSONArray toBeRemovedGroups = new JSONArray();
		JSONArray toBeAddedGroups = new JSONArray();
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			// sending triggers right here.
			try
			{
				this.triggerProcessing.processTriggerForValueUpdateToSubspaceRegion
							(valueUpdateToSubspaceRegionMessage, removedGroups, addedGroups);
				
				
				Iterator<String> groupGUIDIter = removedGroups.keySet().iterator();
				while( groupGUIDIter.hasNext() )
				{
					String groupGUID = groupGUIDIter.next();
					GroupGUIDInfoClass groupGUIDInfo = removedGroups.get(groupGUID);
					toBeRemovedGroups.put(groupGUIDInfo.toJSONObjectImpl());
				}
				
				groupGUIDIter = addedGroups.keySet().iterator();
				while( groupGUIDIter.hasNext() )
				{
					String groupGUID = groupGUIDIter.next();
					GroupGUIDInfoClass groupGUIDInfo = addedGroups.get(groupGUID);
					toBeAddedGroups.put(groupGUIDInfo.toJSONObjectImpl());
				}
				
				
				//this.triggerProcessing.sendOutAggregatedRefreshTrigger
				//	(removedGroups, addedGroups, updateGUID, versionNum, updateStartTime);
			} catch (InterruptedException e1)
			{
				e1.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		ValueUpdateToSubspaceRegionReplyMessage  
		valueUpdateToSubspaceRegionReplyMessage 
			= new ValueUpdateToSubspaceRegionReplyMessage(this.getMyID(), 
				valueUpdateToSubspaceRegionMessage.getVersionNum(), 
				valueUpdateToSubspaceRegionMessage.getRequestID(), 
				toBeRemovedGroups, toBeAddedGroups);
	
		try
		{
			this.messenger.sendToID(valueUpdateToSubspaceRegionMessage.getSender(), 
					valueUpdateToSubspaceRegionReplyMessage.toJSONObject());
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	private class HandleEventThread implements Runnable
	{
		private final ProtocolEvent<PacketType, String> event;
		
		public HandleEventThread(ProtocolEvent<PacketType, String> event)
		{
			this.event = event;
		}
		
		@Override
		public void run()
		{
			// this try catch is very important.
			// otherwise exception from these methods are not at all printed by executor service
			// and debugging gets very time consuming
			try
			{
				switch( event.getType() )
				{
					case  QUERY_MSG_FROM_USER:
					{
						QueryMsgFromUser queryMsgFromUser 
												= (QueryMsgFromUser)event;
						
						processQueryMsgFromUser(queryMsgFromUser);
						
						break;
					}
					case QUERY_MESG_TO_SUBSPACE_REGION:
					{	
						QueryMesgToSubspaceRegion queryMesgToSubspaceRegion = 
								(QueryMesgToSubspaceRegion) event;
						
						log.fine("CS"+getMyID()+" received " + event.getType() + ": " + event);
						
						processQueryMesgToSubspaceRegion(queryMesgToSubspaceRegion);
						
						break;
					}
					case QUERY_MESG_TO_SUBSPACE_REGION_REPLY:
					{
						QueryMesgToSubspaceRegionReply queryMesgToSubspaceRegionReply = 
								(QueryMesgToSubspaceRegionReply)event;
						
						log.fine("CS"+getMyID()+" received " + event.getType() + ": " 
																+ queryMesgToSubspaceRegionReply);
						
						guidAttrValProcessing.processQueryMesgToSubspaceRegionReply
													(queryMesgToSubspaceRegionReply);
						
						break;
					}
					case VALUE_UPDATE_MSG_FROM_GNS:
					{
						ValueUpdateFromGNS valUpdMsgFromGNS 
												= (ValueUpdateFromGNS)event;
						
						ContextServiceLogger.getLogger().fine("CS"+getMyID()
												+" received " + event.getType() + ": " + valUpdMsgFromGNS);
						
						processValueUpdateFromGNS(valUpdMsgFromGNS);
						break;
					}
					
					case VALUEUPDATE_TO_SUBSPACE_REGION_MESSAGE:
					{
						/* Actions:
						 * - send the update message to the responsible value node
						 */
						ValueUpdateToSubspaceRegionMessage 
							valueUpdateToSubspaceRegionMessage 
									= (ValueUpdateToSubspaceRegionMessage)event;
						
						processValueUpdateToSubspaceRegionMessage(valueUpdateToSubspaceRegionMessage);
						break;
					}
					
					case GET_MESSAGE:
					{
						GetMessage getMessage 
									= (GetMessage)event;
						//log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valueUpdateToSubspaceRegionMessage);
						ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " 
										+ getMessage);
						
						processGetMessage(getMessage);
						break;
					}
					
					case VALUEUPDATE_TO_SUBSPACE_REGION_REPLY_MESSAGE:
					{
						ValueUpdateToSubspaceRegionReplyMessage valueUpdateToSubspaceRegionReplyMessage 
									= (ValueUpdateToSubspaceRegionReplyMessage)event;
						
						ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " 
								+ valueUpdateToSubspaceRegionReplyMessage);
						processValueUpdateToSubspaceRegionMessageReply(valueUpdateToSubspaceRegionReplyMessage);
						break;
					}
					
					case CONFIG_REQUEST:
					{
						ClientConfigRequest configRequest 
									= (ClientConfigRequest)event;
						
						ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " 
								+ configRequest);
						processClientConfigRequest(configRequest);
						break;
					}
					
					default:
					{
						assert(false);
						break;
					}
				}
			}
			catch(Exception | Error ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	
	public static void main(String[] args)
	{
		double numPartitions = Math.ceil(Math.pow(16, 1.0/4));
		ContextServiceLogger.getLogger().fine("numPartitions "+numPartitions);
		
		double numAttr  = 5;
		//double numNodes = nodesOfSubspace.size();
		
		Integer[] partitionNumArray = new Integer[2];
		for( int j = 0; j<2; j++ )
		{
			partitionNumArray[j] = j;
			ContextServiceLogger.getLogger().fine("partitionNumArray[j] "+j+" "+partitionNumArray[j]);
		}
		
		// Create the initial vector of 2 elements (apple, orange)
		ICombinatoricsVector<Integer> originalVector = Factory.createVector(partitionNumArray);
		
	    //ICombinatoricsVector<Integer> originalVector = Factory.createVector(new String[] { "apple", "orange" });

		// Create the generator by calling the appropriate method in the Factory class. 
		// Set the second parameter as 3, since we will generate 3-elemets permutations
		Generator<Integer> gen = Factory.createPermutationWithRepetitionGenerator(originalVector, (int)numAttr);
		
		// Print the result
		for( ICombinatoricsVector<Integer> perm : gen )
		{
			ContextServiceLogger.getLogger().fine("perm.getVector() "+perm.getVector());
			ContextServiceLogger.getLogger().fine("hyperspaceDB."
					+ "insertIntoSubspacePartitionInfo complete");
		}
	}
}


/*private void updateGUIDInSecondarySubspaces( JSONObject oldValueJSON , 
boolean firstTimeInsert , JSONObject updatedAttrValJSON , 
String GUID , long requestID, long updateStartTime, 
JSONObject primarySubspaceJSON )
		throws JSONException
{
// process update at other subspaces.
HashMap<Integer, Vector<SubspaceInfo<Integer>>> subspaceInfoMap
						= this.subspaceConfigurator.getSubspaceInfoMap();

Iterator<Integer> keyIter   = subspaceInfoMap.keySet().iterator();

while( keyIter.hasNext() )
{
int subspaceId 			= keyIter.next();
Vector<SubspaceInfo<Integer>> replicasVect 
						= subspaceInfoMap.get(subspaceId);

for( int i=0; i<replicasVect.size(); i++ )
{
	SubspaceInfo<Integer> currSubInfo 
				= replicasVect.get(i);
	int replicaNum = currSubInfo.getReplicaNum();
	
	HashMap<String, AttributePartitionInfo> attrsSubspaceInfo 
										= currSubInfo.getAttributesOfSubspace();
	
	//Vector<Integer> subspaceNodes = currSubInfo.getNodesOfSubspace();
	
	this.guidAttrValProcessing.guidValueProcessingOnUpdate
		( attrsSubspaceInfo, oldValueJSON, subspaceId, replicaNum, 
				updatedAttrValJSON, GUID, requestID, firstTimeInsert, 
				updateStartTime, 
				primarySubspaceJSON );
	
}
}
}*/


/**
 * returns the replica num for a subspace
 * One nodeid should have just one replica num
 * as it can belong to just one replica of that subspace
 * @return
 */
//private int getTheReplicaNumForASubspace( int subpsaceId )
//{
//	Vector<SubspaceInfo> replicasVect 
//			= subspaceConfigurator.getSubspaceInfoMap().get(subpsaceId);
//	
//	int replicaNum = -1;
//	for( int i=0;i<replicasVect.size();i++ )
//	{
//		SubspaceInfo subInfo = replicasVect.get(i);
//		if( subInfo.checkIfSubspaceHasMyID(this.getMyID()) )
//		{
//			replicaNum = subInfo.getReplicaNum();
//			break;
//		}
//	}
//	
//	if(replicaNum == -1)
//	{
//		assert(false);
//	}
//	return replicaNum;
//}


/*private class TaskDispatcher implements Runnable
{	
	@Override
	public void run() 
	{
		while( true )
		{
			synchronized( codeRequestsQueue )
			{
				// not checking the size of the queue here, as size is not
				// constant time operation here.
				if( codeRequestsQueue.peek() == null )
				{
					try
					{
						codeRequestsQueue.wait();
					}
					catch ( InterruptedException e )
					{
						e.printStackTrace();
					}
				}
			}
			
			// not checking the size of the queue here, as size is not
			// constant time operation here.
			while( codeRequestsQueue.peek() != null )
			{
				MySQLRequestStorage currReq = codeRequestsQueue.poll();
				if(currReq != null)
				{
					
				}
			}
		}
	}
}*/