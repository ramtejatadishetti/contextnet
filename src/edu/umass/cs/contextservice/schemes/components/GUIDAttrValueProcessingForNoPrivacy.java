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
import edu.umass.cs.contextservice.database.HyperspaceDB;
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

public class GUIDAttrValueProcessingForNoPrivacy
								extends AbstractGUIDAttrValueProcessing 
{
	public GUIDAttrValueProcessingForNoPrivacy( Integer myID, HashMap<Integer, Vector<SubspaceInfo>> 
		subspaceInfoMap , HyperspaceDB hyperspaceDB, 
		JSONMessenger<Integer> messenger , 
		ConcurrentHashMap<Long, QueryInfo> pendingQueryRequests, 
		ProfilerStatClass profStats )
	{
		super(myID, subspaceInfoMap , hyperspaceDB, messenger , 
				pendingQueryRequests,  profStats);
	}
	
	public void processQueryMsgFromUser
		( QueryInfo queryInfo, boolean storeQueryForTrigger )
	{
		String query;
		long userReqID;
		String userIP;
		int userPort;
		String grpGUID;
		long expiryTime;
		
		query   = queryInfo.getQuery();
		userReqID = queryInfo.getUserReqID();
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
		
		HashMap<String, ProcessingQueryComponent> matchingQueryComponents 
										= new HashMap<String, ProcessingQueryComponent>();
		
		int maxMatchingSubspaceId = getMaxOverlapSubspace
					(queryInfo.getProcessingQC(), matchingQueryComponents);
		
		ContextServiceLogger.getLogger().fine("userReqID "+userReqID
				+" maxMatchingSubspaceNum "
				+maxMatchingSubspaceId+" matchingQueryComponents "
				+matchingQueryComponents.size()+" query "+query);
		
		// get number of nodes/or regions to send to in that subspace.
		
		// choose a replica randomly
		Vector<SubspaceInfo> maxMatchingSubspaceReplicas 
			= subspaceInfoMap.get(maxMatchingSubspaceId);
		int replicaNum = maxMatchingSubspaceReplicas.get
				(this.replicaChoosingRand.nextInt
						(maxMatchingSubspaceReplicas.size())).getReplicaNum();
		
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementIncomingForOverlap();
		}
		long sTime = System.currentTimeMillis();
	    HashMap<Integer, RegionInfoClass> overlappingRegionsMap 
	    		= this.hyperspaceDB.getOverlappingRegionsInSubspace
	    				(maxMatchingSubspaceId, 
	    				replicaNum, matchingQueryComponents);
	    
	    if(ContextServiceConfig.PROFILER_THREAD)
	    {
	    	profStats.incrementNumSearches(overlappingRegionsMap.size(), 
	    			(System.currentTimeMillis()-sTime));
	    }
	    
		synchronized(this.pendingQueryLock)
		{
			queryInfo.setQueryRequestID(queryIdCounter++);
		}
		
		pendingQueryRequests.put(queryInfo.getRequestId(), queryInfo);
		
		ContextServiceLogger.getLogger().fine("processQueryMsgFromUser respNodeIdList"
				+ " size "
				+ overlappingRegionsMap.size()+
	    		" requestId "+queryInfo.getRequestId() +" maxMatchingSubspaceNum "
				+ maxMatchingSubspaceId);
		
		SubspaceSearchReplyInfo subspaceReplyInfo = new SubspaceSearchReplyInfo();
		subspaceReplyInfo.subspaceId = maxMatchingSubspaceId;
		subspaceReplyInfo.overlappingRegionsMap = overlappingRegionsMap;
		
		HashMap<Integer, SubspaceSearchReplyInfo> searchReplyMap 
				= new HashMap<Integer, SubspaceSearchReplyInfo>();
		
		searchReplyMap.put(maxMatchingSubspaceId, subspaceReplyInfo);
		
		queryInfo.initializeSearchQueryReplyInfo(searchReplyMap);
		
	    Iterator<Integer> respNodeIdIter = overlappingRegionsMap.keySet().iterator();
	    
	    while( respNodeIdIter.hasNext() )
	    {
	    	Integer respNodeId = respNodeIdIter.next();
	    	
	    	QueryMesgToSubspaceRegion queryMesgToSubspaceRegion = 
					new QueryMesgToSubspaceRegion
	    			(myID, queryInfo.getRequestId(), query, grpGUID, 
	    					maxMatchingSubspaceId, userIP, userPort, 
	    					storeQueryForTrigger, expiryTime, PrivacySchemes.NO_PRIVACY.ordinal());
	    	
	    	
			try
			{
				this.messenger.sendToID( (Integer)respNodeId, 
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
	
	public int processQueryMesgToSubspaceRegion(QueryMesgToSubspaceRegion 
													queryMesgToSubspaceRegion, 
													JSONArray resultGUIDs)
	{
		String query 				 = queryMesgToSubspaceRegion.getQuery();
		QueryInfo qInfo	 			 = new QueryInfo(query);
		int subspaceId 				 = queryMesgToSubspaceRegion.getSubspaceNum();
		
		long sTime = System.currentTimeMillis();
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementIncomingForData();
		}
		int resultSize = this.hyperspaceDB.processSearchQueryInSubspaceRegion
				(subspaceId, qInfo.getProcessingQC(), resultGUIDs);
		
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementNumRepliesFromSubspaceRegion(resultSize, 
					(System.currentTimeMillis()-sTime));
		}
		return resultSize;
	}
	
	public void processQueryMesgToSubspaceRegionReply(
			QueryMesgToSubspaceRegionReply 
									queryMesgToSubspaceRegionReply)
	{
		Integer senderID = queryMesgToSubspaceRegionReply.getSender();
		long requestId = queryMesgToSubspaceRegionReply.getRequestId();
		
		QueryInfo queryInfo = pendingQueryRequests.get(requestId);
		
		boolean allRepRecvd = 
				queryInfo.addReplyFromARegionOfASubspace(queryMesgToSubspaceRegionReply.getSubsapceId(), 
						(Integer)senderID, queryMesgToSubspaceRegionReply);
		
		if( allRepRecvd )
		{
			JSONArray concatResult 							 = new JSONArray();

			int totalNumReplies 							 = 0;
			
			HashMap<Integer,SubspaceSearchReplyInfo> searchReplyMap 
											= queryInfo.getSearchReplyMap();
		
			// in no privacy case , the search query processing happens in only one subspace.
			assert(searchReplyMap.size() == 1);
		
			int subspaceId = searchReplyMap.keySet().iterator().next();
			
			SubspaceSearchReplyInfo subspaceSearchInfo   = searchReplyMap.get(subspaceId);
			
			if( ContextServiceConfig.sendFullRepliesToClient )
			{	
				Iterator<Integer> nodeIdIter = subspaceSearchInfo.overlappingRegionsMap.keySet().iterator();

				while( nodeIdIter.hasNext() )
				{
					RegionInfoClass regInfo = subspaceSearchInfo.overlappingRegionsMap.get(nodeIdIter.next());
					concatResult.put(regInfo.replyArray);
					totalNumReplies = totalNumReplies + regInfo.replyArray.length();
				}
			}
			else
			{
				Iterator<Integer> nodeIdIter = subspaceSearchInfo.overlappingRegionsMap.keySet().iterator();

				while( nodeIdIter.hasNext() )
				{
					int currRepSize = subspaceSearchInfo.overlappingRegionsMap.get( nodeIdIter.next() ).numReplies;
					totalNumReplies = totalNumReplies + currRepSize;
				}
			}
			
			QueryMsgFromUserReply queryMsgFromUserReply 
				= new QueryMsgFromUserReply( myID, 
						queryInfo.getQuery(), queryInfo.getGroupGUID(), concatResult, 
						queryInfo.getUserReqID(), totalNumReplies, 
						PrivacySchemes.NO_PRIVACY.ordinal() );
			
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
					+ myID +" to node "+new InetSocketAddress(queryInfo.getUserIP(), queryInfo.getUserPort()));

			pendingQueryRequests.remove(requestId);
		}
	}
	
	/**
	 * This function processes a request serially.
	 * when one outstanding request completes.
	 */
	public void processUpdateFromGNS(UpdateInfo updateReq)
	{
		assert(updateReq != null);
		
		String GUID 	 		= updateReq.getValueUpdateFromGNS().getGUID();
		JSONObject attrValuePairs 
						 		= updateReq.getValueUpdateFromGNS().getAttrValuePairs();
		long requestID 	 		= updateReq.getRequestId();
		long updateStartTime	= updateReq.getValueUpdateFromGNS().getUpdateStartTime();
		
		
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
				updateOrInsert = HyperspaceDB.INSERT_REC;
			}
			else
			{
				firstTimeInsert = false;
				updateOrInsert = HyperspaceDB.UPDATE_REC;
			}
			
			JSONObject jsonToWrite = getJSONToWriteInPrimarySubspace( oldValueJSON, 
					attrValuePairs, null );
			
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
					requestID, updateStartTime, jsonToWrite  );
		}
		catch ( JSONException e )
		{
			e.printStackTrace();
		}
	}
	
	private void updateGUIDInSecondarySubspaces( JSONObject oldValueJSON , 
			boolean firstTimeInsert , JSONObject updatedAttrValJSON , 
			String GUID , long requestID, long updateStartTime, 
			JSONObject primarySubspaceJSON )
					throws JSONException
	{
		Iterator<Integer> keyIter   = subspaceInfoMap.keySet().iterator();
		
		while( keyIter.hasNext() )
		{
			int subspaceId 			= keyIter.next();
			Vector<SubspaceInfo> replicasVect 
									= subspaceInfoMap.get(subspaceId);
			
			for( int i=0; i<replicasVect.size(); i++ )
			{
				SubspaceInfo currSubInfo 
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