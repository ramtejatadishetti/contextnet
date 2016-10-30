package edu.umass.cs.contextservice.schemes.components;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;
import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.profilers.ProfilerStatClass;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.schemes.helperclasses.RegionInfoClass;
import edu.umass.cs.contextservice.schemes.helperclasses.SubspaceSearchReplyInfo;
import edu.umass.cs.contextservice.updates.UpdateInfo;
import edu.umass.cs.nio.JSONMessenger;

public class GUIDAttrValueProcessingWithSubspacePrivacy<NodeIDType> 
							extends AbstractGUIDAttrValueProcessing<NodeIDType>
{	
	public GUIDAttrValueProcessingWithSubspacePrivacy( NodeIDType myID, 
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> 
		subspaceInfoMap , HyperspaceMySQLDB<NodeIDType> hyperspaceDB, 
		JSONMessenger<NodeIDType> messenger , 
		ConcurrentHashMap<Long, QueryInfo<NodeIDType>> pendingQueryRequests, 
		ProfilerStatClass profStats )
	{
		super( myID, subspaceInfoMap, hyperspaceDB, messenger, 
					pendingQueryRequests, profStats);
	}
	
	public void processQueryMsgFromUser
			( QueryInfo<NodeIDType> queryInfo, boolean storeQueryForTrigger )
	{
		String query;
		String userIP;
		int userPort;
		String grpGUID;
		long expiryTime;
		
		query   = queryInfo.getQuery();
		userIP  = queryInfo.getUserIP();
		userPort   = queryInfo.getUserPort();
		grpGUID = queryInfo.getGroupGUID();
		expiryTime = queryInfo.getExpiryTime();
		
		if( grpGUID.length() <= 0 )
		{
			ContextServiceLogger.getLogger().fine
				("Query request failed at the recieving node ");
			return;
		}
		
		// key is subspaceId.
		HashMap<Integer, HashMap<String, ProcessingQueryComponent>> subsapceMatchingAttributesMap
							= new HashMap<Integer, HashMap<String, ProcessingQueryComponent>>();
		
		computeAllOverlappingSubspaces( queryInfo.getProcessingQC(), 
											subsapceMatchingAttributesMap );
		
		assert( subsapceMatchingAttributesMap.size() > 0 );
		
		HashMap<Integer, SubspaceSearchReplyInfo> searchReplyMap
				= computeRegionsInSubspacesForQuery( subsapceMatchingAttributesMap );
		
		synchronized(this.pendingQueryLock)
		{
			queryInfo.setQueryRequestID(queryIdCounter++);
		}
		
		pendingQueryRequests.put(queryInfo.getRequestId(), queryInfo);
		
		ContextServiceLogger.getLogger().fine("processQueryMsgFromUser searchReplyMap size "
				+ searchReplyMap.size()+
	    		" requestId "+queryInfo.getRequestId());
		
		queryInfo.initializeSearchQueryReplyInfo(searchReplyMap);
		
		Iterator<Integer> subspaceIter = searchReplyMap.keySet().iterator();
		
		while( subspaceIter.hasNext() )
		{
			int subspaceId = subspaceIter.next();
			SubspaceSearchReplyInfo subspaceSearchInfo = searchReplyMap.get(subspaceId);
			
			Iterator<Integer> respNodeIdIter = subspaceSearchInfo.overlappingRegionsMap.keySet().iterator();
		    
		    while( respNodeIdIter.hasNext() )
		    {
		    	Integer respNodeId = respNodeIdIter.next();
		    	
		    	QueryMesgToSubspaceRegion<NodeIDType> queryMesgToSubspaceRegion = 
						new QueryMesgToSubspaceRegion<NodeIDType>
		    			( myID, queryInfo.getRequestId(), query, grpGUID, 
		    					subspaceId, userIP, userPort, 
		    					storeQueryForTrigger, expiryTime, 
		    					PrivacySchemes.SUBSPACE_PRIVACY.ordinal() );
		    	
				try
				{
					this.messenger.sendToID( (NodeIDType)respNodeId, 
							queryMesgToSubspaceRegion.toJSONObject() );
				} catch (IOException e)
				{
					e.printStackTrace();
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
				ContextServiceLogger.getLogger().info("Sending QueryMesgToSubspaceRegion mesg from " 
						+ myID +" to node "+respNodeId);
		    }
		}
	}
	
	
	public int processQueryMesgToSubspaceRegion(QueryMesgToSubspaceRegion<NodeIDType> 
													queryMesgToSubspaceRegion, 
													JSONArray resultGUIDs)
	{
		String query 				 = queryMesgToSubspaceRegion.getQuery();
		QueryInfo<NodeIDType> qInfo	 = new QueryInfo<NodeIDType>(query);
		int subspaceId 				 = queryMesgToSubspaceRegion.getSubspaceNum();
		
		// first replica. There is always 1 replica
		HashMap<String, ProcessingQueryComponent> filterQComponents = filterQueryComponentsForASubspace(
				qInfo.getProcessingQC(), this.subspaceInfoMap.get(subspaceId).get(0));
		
		long sTime = System.currentTimeMillis();
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementIncomingForData();
		}
		int resultSize = this.hyperspaceDB.processSearchQueryInSubspaceRegion
				(subspaceId, filterQComponents, resultGUIDs);
		
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementNumRepliesFromSubspaceRegion(resultSize, 
					(System.currentTimeMillis()-sTime));
		}
		return resultSize;
	}
	
	private HashMap<String, ProcessingQueryComponent> filterQueryComponentsForASubspace(
			HashMap<String, ProcessingQueryComponent> queryComponents, SubspaceInfo<NodeIDType> subsapceInfo)
	{
		HashMap<String, ProcessingQueryComponent> filteredQComponents =
					new HashMap<String, ProcessingQueryComponent>();
		
		Iterator<String> queryAttrIter = queryComponents.keySet().iterator();
		
		while( queryAttrIter.hasNext() )
		{
			String attrName = queryAttrIter.next();
			// subspace contains this attr
			if(subsapceInfo.getAttributesOfSubspace().containsKey(attrName))
			{
				filteredQComponents.put(attrName, queryComponents.get(attrName));
			}
		}
		
		return filteredQComponents;
	}
	
	public void processQueryMesgToSubspaceRegionReply(QueryMesgToSubspaceRegionReply<NodeIDType> 
									queryMesgToSubspaceRegionReply)
	{
		NodeIDType senderID = queryMesgToSubspaceRegionReply.getSender();
		long requestId = queryMesgToSubspaceRegionReply.getRequestId();

		QueryInfo<NodeIDType> queryInfo = pendingQueryRequests.get(requestId);

		boolean allRepRecvd = 
				queryInfo.addReplyFromARegionOfASubspace(queryMesgToSubspaceRegionReply.getSubsapceId(), 
						(Integer)senderID, queryMesgToSubspaceRegionReply);

		if( allRepRecvd )
		{
			JSONArray concatResult 							 = new JSONArray();

			int totalNumReplies 							 = 0;
			HashMap<Integer, SubspaceSearchReplyInfo> searchReplyMap 
					= queryInfo.getSearchReplyMap();
			
			if( ContextServiceConfig.sendFullRepliesToClient )
			{
				Iterator<Integer> subspaceIter = searchReplyMap.keySet().iterator();
				
				while( subspaceIter.hasNext() )
				{
					int subspaceId = subspaceIter.next();
					SubspaceSearchReplyInfo subspaceSearchInfo = searchReplyMap.get(subspaceId);
					
					Iterator<Integer> nodeIdIter 
								= subspaceSearchInfo.overlappingRegionsMap.keySet().iterator();
					// subspace reply 
					JSONArray subspaceReplyArray = new JSONArray();
					
					while( nodeIdIter.hasNext() )
					{
						RegionInfoClass regInfo = subspaceSearchInfo.overlappingRegionsMap.get
																		(nodeIdIter.next());
						subspaceReplyArray.put(regInfo.replyArray);
						int currRepSize = regInfo.replyArray.length();
						totalNumReplies = totalNumReplies + currRepSize;
						//concatResult.put(currArray);
					}
					
					// adding reply of all subspaces.
					// cns client will do conjunction.
					concatResult.put(subspaceReplyArray);
				}	
			}
			else
			{	
				Iterator<Integer> subspaceIter = searchReplyMap.keySet().iterator();
				
				while( subspaceIter.hasNext() )
				{
					int subspaceId = subspaceIter.next();
					SubspaceSearchReplyInfo subspaceSearchInfo = searchReplyMap.get(subspaceId);
					
					Iterator<Integer> nodeIdIter 
								= subspaceSearchInfo.overlappingRegionsMap.keySet().iterator();

					while( nodeIdIter.hasNext() )
					{
						RegionInfoClass regInfo = subspaceSearchInfo.overlappingRegionsMap.get
																		(nodeIdIter.next());
						int currRepSize = regInfo.numReplies;
						totalNumReplies = totalNumReplies + currRepSize;
						//concatResult.put(currArray);
					}
				}
			}
			
			QueryMsgFromUserReply<NodeIDType> queryMsgFromUserReply 
				= new QueryMsgFromUserReply<NodeIDType>( myID, 
						queryInfo.getQuery(), queryInfo.getGroupGUID(), concatResult, 
						queryInfo.getUserReqID(), totalNumReplies, 
						PrivacySchemes.SUBSPACE_PRIVACY.ordinal() );
			
			try
			{
				this.messenger.sendToAddress(new InetSocketAddress(queryInfo.getUserIP(), 
						queryInfo.getUserPort()), queryMsgFromUserReply.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			ContextServiceLogger.getLogger().info("Sending queryMsgFromUserReply mesg from " 
					+ myID +" to node "+new InetSocketAddress(queryInfo.getUserIP(), 
							queryInfo.getUserPort()));

			pendingQueryRequests.remove(requestId);
		}
	}
	
	
	private void computeAllOverlappingSubspaces( 
			HashMap<String, ProcessingQueryComponent> pqueryComponents, 
			HashMap<Integer, HashMap<String, ProcessingQueryComponent>> subsapceMatchingAttributesMap )
	{
		Iterator<Integer> keyIter   	= subspaceInfoMap.keySet().iterator();
		
		while( keyIter.hasNext() )
		{
			int subspaceId = keyIter.next();
			SubspaceInfo<NodeIDType> currSubInfo 
							= subspaceInfoMap.get(subspaceId).get(0);
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo 
							= currSubInfo.getAttributesOfSubspace();
			
			int currMatch = 0;
			HashMap<String, ProcessingQueryComponent> currMatchingComponents 
							= new HashMap<String, ProcessingQueryComponent>();
			
			Iterator<String> attrIter = pqueryComponents.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				ProcessingQueryComponent pqc = pqueryComponents.get(attrName);
				if( attrsSubspaceInfo.containsKey(pqc.getAttributeName()) )
				{
					currMatch = currMatch + 1;
					currMatchingComponents.put(pqc.getAttributeName(), pqc);
				}
			}
			
			if(currMatch > 0)
			{
				subsapceMatchingAttributesMap.put(subspaceId, currMatchingComponents);
			}
		}
	}
	
	private HashMap<Integer, SubspaceSearchReplyInfo> computeRegionsInSubspacesForQuery(
			HashMap<Integer, HashMap<String, ProcessingQueryComponent>> subsapceMatchingAttributesMap)
	{
		HashMap<Integer, SubspaceSearchReplyInfo> searchReplyMap 
									= new HashMap<Integer, SubspaceSearchReplyInfo>();
		
		Iterator<Integer> subspaceIdIter 
									= subsapceMatchingAttributesMap.keySet().iterator();
		
		while( subspaceIdIter.hasNext() )
		{
			int subspaceId = subspaceIdIter.next();
			ContextServiceLogger.getLogger().fine( "Total number of "
					+ " subspaces this search goes to "
					+ subsapceMatchingAttributesMap.size() );
			// get number of nodes/or regions to send to in that subspace.
			
			// choose a replica randomly
			Vector<SubspaceInfo<NodeIDType>> subspaceReplicas 
								= subspaceInfoMap.get(subspaceId);
			
			int replicaNum = subspaceReplicas.get
					( this.replicaChoosingRand.nextInt
							(subspaceReplicas.size())).getReplicaNum();
			
			HashMap<Integer, RegionInfoClass> overlappingRegionsInfo 
				= this.hyperspaceDB.getOverlappingRegionsInSubspace(subspaceId, 
						replicaNum, subsapceMatchingAttributesMap.get(subspaceId));
			
			SubspaceSearchReplyInfo subspaceSearchInfo = new SubspaceSearchReplyInfo();
			
			subspaceSearchInfo.subspaceId 				= subspaceId;
			subspaceSearchInfo.overlappingRegionsMap 	= overlappingRegionsInfo;
			
			searchReplyMap.put(subspaceId, subspaceSearchInfo);
		}
		
		return searchReplyMap;
	}
	
	
	public void processUpdateFromGNS( UpdateInfo<NodeIDType> updateReq )
	{
		String GUID 	 					= updateReq.getValueUpdateFromGNS().getGUID();
		JSONObject attrValuePairs 
						 					= updateReq.getValueUpdateFromGNS().getAttrValuePairs();
		long requestID 	 					= updateReq.getRequestId();
		long updateStartTime				= updateReq.getValueUpdateFromGNS().getUpdateStartTime();
		
		// this could be null, in no privacy case, and also in privacy case
		// , as anonymizedIDtoGuidMapping is not set in every message just in the beginning 
		// or on acl changes.
		JSONArray anonymizedIDToGuidMapping 
											= updateReq.getValueUpdateFromGNS().getAnonymizedIDToGuidMapping();
		
		
		JSONArray attrSetOfAnonymizedID 	= updateReq.getValueUpdateFromGNS().getAttrSetArray();
		
		assert(attrSetOfAnonymizedID != null);
		assert(attrSetOfAnonymizedID.length() > 0);
		
		
		// get the old value and process the update in primary subspace and other subspaces.
		
		try
		{
			boolean firstTimeInsert = false;
			
			long start 	 = System.currentTimeMillis();
			// FIXME: fetch only those attributes which are specified 
			// in the updated attrs.
			JSONObject oldValueJSON 	
						 = this.hyperspaceDB.getGUIDStoredInPrimarySubspace(GUID);
			
			long end 	 = System.currentTimeMillis();
			
			if( ContextServiceConfig.DEBUG_MODE )
			{
				System.out.println("getGUIDStoredInPrimarySubspace time "+(end-start)
							+" since upd start"+(end-updateStartTime));
			}
			
			int updateOrInsert 			= -1;
			
			if( oldValueJSON.length() == 0 )
			{
				firstTimeInsert = true;
				updateOrInsert = HyperspaceMySQLDB.INSERT_REC;
			}
			else
			{
				firstTimeInsert = false;
				updateOrInsert = HyperspaceMySQLDB.UPDATE_REC;
			}
			
			JSONObject jsonToWrite = getJSONToWriteInPrimarySubspace( oldValueJSON, 
					attrValuePairs, anonymizedIDToGuidMapping );
			
			this.hyperspaceDB.storeGUIDInPrimarySubspace
								(GUID, jsonToWrite, updateOrInsert);
			
			
			if(ContextServiceConfig.DEBUG_MODE)
			{
				long now = System.currentTimeMillis();
				System.out.println("primary subspace update complete "+(now-updateStartTime));
			}
			
			// process update at secondary subspaces.
			//FIXME: anonymizedIDToGuidMapping can be null if not present in the message.
			// set it by reading from primarysubspace storage.
			updateGUIDInSecondarySubspaces( oldValueJSON , 
					firstTimeInsert , attrValuePairs , GUID , 
					requestID, updateStartTime, jsonToWrite, attrSetOfAnonymizedID);
		}
		catch ( JSONException e )
		{
			e.printStackTrace();
		}
	}
	
	
	private void updateGUIDInSecondarySubspaces( JSONObject oldValueJSON , 
			boolean firstTimeInsert , JSONObject updatedAttrValJSON , 
			String GUID , long requestID, long updateStartTime, 
			JSONObject primarySubspaceJSON, JSONArray attrSetOfAnonymizedID )
					throws JSONException
	{
		Iterator<Integer> keyIter   = subspaceInfoMap.keySet().iterator();
		
		while( keyIter.hasNext() )
		{
			int subspaceId 			= keyIter.next();
			Vector<SubspaceInfo<NodeIDType>> replicasVect 
									= subspaceInfoMap.get(subspaceId);
			
			assert(replicasVect.size() > 0);
			
			boolean overlapWithSubspace = 
					checkOverlapWithSubspaceAttrs( replicasVect.get(0).getAttributesOfSubspace(), 
						attrSetOfAnonymizedID );
			
			if( overlapWithSubspace )
			{
				for( int i=0; i<replicasVect.size(); i++ )
				{
					SubspaceInfo<NodeIDType> currSubInfo 
								= replicasVect.get(i);
					int replicaNum = currSubInfo.getReplicaNum();
					
					HashMap<String, AttributePartitionInfo> attrsSubspaceInfo 
														= currSubInfo.getAttributesOfSubspace();
					
					guidValueProcessingOnUpdate
							( attrsSubspaceInfo, oldValueJSON, subspaceId, replicaNum, 
								updatedAttrValJSON, GUID, requestID, firstTimeInsert, 
								updateStartTime, 
								primarySubspaceJSON );
				}
			}
		}
	}
}