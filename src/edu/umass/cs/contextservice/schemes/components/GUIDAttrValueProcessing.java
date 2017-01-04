package edu.umass.cs.contextservice.schemes.components;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;
import edu.umass.cs.contextservice.database.AbstractDataStorageDB;
import edu.umass.cs.contextservice.database.RegionMappingDataStorageDB;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.profilers.ProfilerStatClass;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.queryparsing.QueryParser;
import edu.umass.cs.contextservice.regionmapper.AbstractRegionMappingPolicy;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.schemes.helperclasses.SearchReplyInfo;
import edu.umass.cs.contextservice.updates.UpdateInfo;
import edu.umass.cs.nio.JSONMessenger;

public class GUIDAttrValueProcessing
								extends AbstractGUIDAttrValueProcessing 
{
	public GUIDAttrValueProcessing( Integer myID, 
			AbstractRegionMappingPolicy regionMappingPolicy, 
			AbstractDataStorageDB hyperspaceDB, 
		JSONMessenger<Integer> messenger , 
		ConcurrentHashMap<Long, QueryInfo> pendingQueryRequests, 
		ProfilerStatClass profStats )
	{
		super(myID, regionMappingPolicy, 
				hyperspaceDB, messenger , 
				pendingQueryRequests,  profStats);
	}
	
	public void processQueryMsgFromUser
		( QueryInfo queryInfo, boolean storeQueryForTrigger )
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
	    
		synchronized(this.pendingQueryLock)
		{
			queryInfo.setQueryRequestID(queryIdCounter++);
		}
		pendingQueryRequests.put(queryInfo.getRequestId(), queryInfo);
		
//		ValueSpaceInfo searchQValSpace = ValueSpaceInfo.getAllAttrsValueSpaceInfo
//										(queryInfo.getSearchQueryAttrValMap(), 
//												AttributeTypes.attributeMap);
		
		List<Integer> nodeList 
				= regionMappingPolicy.getNodeIDsForSearch
					(queryInfo.getSearchQueryAttrValMap());
		
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementNumSearches(nodeList.size());
		}
		
		
		queryInfo.initializeSearchQueryReplyInfo(nodeList);
		
		for(int i=0; i< nodeList.size(); i++)
		{
			int nodeid = nodeList.get(i);
			
			QueryMesgToSubspaceRegion queryMesgToSubspaceRegion = 
					new QueryMesgToSubspaceRegion
	    			(myID, queryInfo.getRequestId(), query, grpGUID, 
	    					userIP, userPort, storeQueryForTrigger, 
	    					expiryTime, PrivacySchemes.NO_PRIVACY.ordinal());
			
			try
			{
				this.messenger.sendToID( nodeid, 
						queryMesgToSubspaceRegion.toJSONObject() );
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			ContextServiceLogger.getLogger().info("Sending QueryMesgToSubspaceRegion mesg from " 
					+ myID +" to node "+nodeid);
		}
	}
	
	public int processQueryMesgToSubspaceRegion(QueryMesgToSubspaceRegion 
													queryMesgToSubspaceRegion, 
													JSONArray resultGUIDs)
	{
		String query 				 		= queryMesgToSubspaceRegion.getQuery();
		// we don't evaluate the query over full value space on all attributes here.
		// because in privacy some attributes may not be specified.
		// so a query should only be evaluated on attributes that are specified 
		// in the query. Attributes that are not spcfied are stored with Double.MIN
		// value, which is outside the Min max value corresponding to an attribute.
		HashMap<String, AttributeValueRange> searchAttrValRange	 = QueryParser.parseQuery(query);
		
		int resultSize = this.hyperspaceDB.processSearchQueryUsingAttrIndex
				(searchAttrValRange, resultGUIDs);
		
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
				queryInfo.addReplyFromANode( senderID, queryMesgToSubspaceRegionReply);
		
		if( allRepRecvd )
		{
			JSONArray concatResult 							 = new JSONArray();

			int totalNumReplies 							 = 0;
			
			HashMap<Integer, SearchReplyInfo> searchReplyMap 
											= queryInfo.getSearchReplyMap();
			
			if( ContextServiceConfig.sendFullRepliesToClient )
			{	
				Iterator<Integer> nodeIdIter = searchReplyMap.keySet().iterator();

				while( nodeIdIter.hasNext() )
				{
					int nodeid = nodeIdIter.next();
					SearchReplyInfo replyInfo = searchReplyMap.get(nodeid);
					concatResult.put(replyInfo.replyArray);
					totalNumReplies = totalNumReplies + replyInfo.replyArray.length();
				}
			}
			else
			{
				Iterator<Integer> nodeIdIter = searchReplyMap.keySet().iterator();

				while( nodeIdIter.hasNext() )
				{
					int nodeid = nodeIdIter.next();
					SearchReplyInfo replyInfo = searchReplyMap.get(nodeid);
					int currRepSize = replyInfo.numReplies;
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
		JSONArray anonymizedIDToGuidMapping 
								= updateReq.getValueUpdateFromGNS().getAnonymizedIDToGuidMapping();
		
		// get the old value and process the update in primary subspace and other subspaces.
		
		try
		{	
			boolean firstTimeInsert = false;
			
			long start 	 = System.currentTimeMillis();
			
			JSONObject oldValueJSON 
						 = this.hyperspaceDB.getGUIDStoredUsingHashIndex(GUID);
			
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
				updateOrInsert = RegionMappingDataStorageDB.INSERT_REC;
			}
			else
			{
				firstTimeInsert = false;
				updateOrInsert = RegionMappingDataStorageDB.UPDATE_REC;
			}
			
			JSONObject jsonToWrite = getJSONToWriteInPrimarySubspace( oldValueJSON, 
					attrValuePairs, anonymizedIDToGuidMapping );
			
			this.hyperspaceDB.storeGUIDUsingHashIndex
								(GUID, jsonToWrite, updateOrInsert);
			
			
			if(ContextServiceConfig.DEBUG_MODE)
			{
				long now = System.currentTimeMillis();
				System.out.println("primary subspace update complete "+(now-updateStartTime));
			}
			
			// process update at secondary subspaces.
			updateGUIDInAttrIndexes( oldValueJSON , 
					firstTimeInsert , attrValuePairs , GUID , 
					requestID, updateStartTime, jsonToWrite, updateReq  );
		}
		catch ( JSONException e )
		{
			e.printStackTrace();
		}
	}
	
	private void updateGUIDInAttrIndexes( JSONObject oldValueJSON , 
			boolean firstTimeInsert , JSONObject updatedAttrValJSON , 
			String GUID , long requestID, long updateStartTime, 
			JSONObject primarySubspaceJSON, UpdateInfo updateReq )
					throws JSONException
	{
		guidValueProcessingOnUpdate
		( oldValueJSON,  updatedAttrValJSON, GUID, requestID, firstTimeInsert, 
				updateStartTime, primarySubspaceJSON, updateReq );
	}
}