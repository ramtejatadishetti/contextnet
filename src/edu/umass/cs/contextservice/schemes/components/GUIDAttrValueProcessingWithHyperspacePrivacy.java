package edu.umass.cs.contextservice.schemes.components;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig.PrivacySchemes;
import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.profilers.ProfilerStatClass;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.schemes.HyperspaceHashing;
import edu.umass.cs.contextservice.updates.UpdateInfo;
import edu.umass.cs.nio.JSONMessenger;

public class GUIDAttrValueProcessingWithHyperspacePrivacy<NodeIDType> implements 
								GUIDAttrValueProcessingInterface<NodeIDType>
{
	private final HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> 
					subspaceInfoMap;
	
	private final HyperspaceMySQLDB<NodeIDType> hyperspaceDB;
	
	private final Random replicaChoosingRand;
	
	private final NodeIDType myID;
	
	private final JSONMessenger<NodeIDType> messenger;
	
	private final ConcurrentHashMap<Long, QueryInfo<NodeIDType>> pendingQueryRequests;
	
	private final Object pendingQueryLock												= new Object();
	
	private long queryIdCounter															= 0;
	
	private final ProfilerStatClass profStats;
	
	private final Random defaultAttrValGenerator;
	
	public GUIDAttrValueProcessingWithHyperspacePrivacy( NodeIDType myID, 
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> 
		subspaceInfoMap , HyperspaceMySQLDB<NodeIDType> hyperspaceDB, 
		JSONMessenger<NodeIDType> messenger , 
		ConcurrentHashMap<Long, QueryInfo<NodeIDType>> pendingQueryRequests, 
		ProfilerStatClass profStats )
	{
		this.myID = myID;
		this.messenger = messenger;
		replicaChoosingRand = new Random(myID.hashCode());

		this.subspaceInfoMap = subspaceInfoMap;
		this.hyperspaceDB = hyperspaceDB;
		
		this.profStats = profStats;
		
		this.pendingQueryRequests = pendingQueryRequests;
		
		defaultAttrValGenerator = new Random(myID.hashCode());
	}
	
	public void processQueryMsgFromUser
		( QueryInfo<NodeIDType> queryInfo, boolean storeQueryForTrigger )
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
		
		//Vector<QueryComponent> qcomponents = QueryParser.parseQueryNew(query);
		//FIXME: for conflicting queries , need to be handled sometime
		/*Vector<QueryComponent> matchingQueryComponents = new Vector<QueryComponent>();
		int maxMatchingSubspaceNum = getMaxOverlapSubspace(qcomponents, matchingQueryComponents);
		ContextServiceLogger.getLogger().fine("userReqID "+userReqID+" maxMatchingSubspaceNum "+maxMatchingSubspaceNum+" matchingQueryComponents "
				+matchingQueryComponents.size()+" query "+query);
		
		// get number of nodes/or regions to send to in that subspace.
	    HashMap<Integer, OverlappingInfoClass> respNodeIdList 
	    		= this.hyperspaceDB.getOverlappingRegionsInSubspace(maxMatchingSubspaceNum, matchingQueryComponents);
	    
	    // query is conflicting, like same attribute has conflicting ranges
	    // in conjunction. 1 <= contextATT0 <= 5 && 10 <= contextATT0 <= 15,
	    // in current query patterns this query can be generated.
	    if( respNodeIdList.size() == 0 )
	    {
	    	QueryMsgFromUserReply<NodeIDType> queryMsgFromUserReply = new QueryMsgFromUserReply<NodeIDType>(this.getMyID(),
					query, grpGUID, new JSONArray(), userReqID, 0);
			try
			{
				this.messenger.sendToAddress(new InetSocketAddress( userIP, userPort), 
						queryMsgFromUserReply.toJSONObject() );
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			return;
	    }*/
		
		Vector<ProcessingQueryComponent> matchingQueryComponents 
										= new Vector<ProcessingQueryComponent>();
		int maxMatchingSubspaceId = getMaxOverlapSubspace
					(queryInfo.getProcessingQC(), matchingQueryComponents);
		
		ContextServiceLogger.getLogger().fine("userReqID "+userReqID+" maxMatchingSubspaceNum "
				+maxMatchingSubspaceId+" matchingQueryComponents "
				+matchingQueryComponents.size()+" query "+query);
		
		// get number of nodes/or regions to send to in that subspace.
		
		// choose a replica randomly
		Vector<SubspaceInfo<NodeIDType>> maxMatchingSubspaceReplicas 
			= subspaceInfoMap.get(maxMatchingSubspaceId);
		int replicaNum = maxMatchingSubspaceReplicas.get
				(this.replicaChoosingRand.nextInt(maxMatchingSubspaceReplicas.size())).getReplicaNum();
		
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementIncomingForOverlap();
		}
		long sTime = System.currentTimeMillis();
	    HashMap<Integer, OverlappingInfoClass> respNodeIdList 
	    		= this.hyperspaceDB.getOverlappingRegionsInSubspace(maxMatchingSubspaceId, 
	    				replicaNum, matchingQueryComponents);
	    
	    if(ContextServiceConfig.PROFILER_THREAD)
	    {
	    	profStats.incrementNumSearches(respNodeIdList.size(), (System.currentTimeMillis()-sTime));
	    }
	    
		synchronized(this.pendingQueryLock)
		{
			queryInfo.setQueryRequestID(queryIdCounter++);
		}
		
		pendingQueryRequests.put(queryInfo.getRequestId(), queryInfo);
		
		ContextServiceLogger.getLogger().fine("processQueryMsgFromUser respNodeIdList size "
				+ respNodeIdList.size()+
	    		" requestId "+queryInfo.getRequestId() +" maxMatchingSubspaceNum "
				+ maxMatchingSubspaceId);
		
		queryInfo.initializeRegionalReplies(respNodeIdList);
		
	    Iterator<Integer> respNodeIdIter = respNodeIdList.keySet().iterator();
	    
	    while( respNodeIdIter.hasNext() )
	    {
	    	Integer respNodeId = respNodeIdIter.next();
	    	
	    	QueryMesgToSubspaceRegion<NodeIDType> queryMesgToSubspaceRegion = 
					new QueryMesgToSubspaceRegion<NodeIDType>
	    			(myID, queryInfo.getRequestId(), query, grpGUID, 
	    					maxMatchingSubspaceId, userIP, userPort, 
	    					storeQueryForTrigger, expiryTime , 
	    					PrivacySchemes.HYPERSPACE_PRIVACY.ordinal() );
	    	
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
	
	
	public int processQueryMesgToSubspaceRegion(QueryMesgToSubspaceRegion<NodeIDType> 
													queryMesgToSubspaceRegion, 
													JSONArray resultGUIDs)
	{
		String query 				 = queryMesgToSubspaceRegion.getQuery();
		int subspaceId 				 = queryMesgToSubspaceRegion.getSubspaceNum();
		
		long sTime = System.currentTimeMillis();
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementIncomingForData();
		}
		int resultSize = this.hyperspaceDB.processSearchQueryInSubspaceRegion
				(subspaceId, query, resultGUIDs);
		
		if(ContextServiceConfig.PROFILER_THREAD)
		{
			profStats.incrementNumRepliesFromSubspaceRegion(resultSize, 
					(System.currentTimeMillis()-sTime));
		}
		return resultSize;
	}
	
	public void processQueryMesgToSubspaceRegionReply(QueryMesgToSubspaceRegionReply<NodeIDType> 
									queryMesgToSubspaceRegionReply)
	{
		NodeIDType senderID = queryMesgToSubspaceRegionReply.getSender();
		long requestId = queryMesgToSubspaceRegionReply.getRequestId();

		QueryInfo<NodeIDType> queryInfo = pendingQueryRequests.get(requestId);

		boolean allRepRecvd = 
				queryInfo.setRegionalReply((Integer)senderID, queryMesgToSubspaceRegionReply);

		if( allRepRecvd )
		{
			JSONArray concatResult 							 = new JSONArray();

			int totalNumReplies 							 = 0;

			if( ContextServiceConfig.sendFullRepliesToClient )
			{
				HashMap<Integer, OverlappingInfoClass> repliesHashMap 
					= queryInfo.getRepliesHashMap();

				Iterator<Integer> nodeIdIter 				 = repliesHashMap.keySet().iterator();

				while( nodeIdIter.hasNext() )
				{
					OverlappingInfoClass currArray 			 = repliesHashMap.get(nodeIdIter.next());
					concatResult.put(currArray.replyArray);
					totalNumReplies = totalNumReplies + currArray.replyArray.length();
				}
			}
			else
			{
				HashMap<Integer, Integer> repliesSizeHashMap = queryInfo.getRepliesSizeHashMap();
				Iterator<Integer> nodeIdIter = repliesSizeHashMap.keySet().iterator();

				while( nodeIdIter.hasNext() )
				{
					int currRepSize = repliesSizeHashMap.get( nodeIdIter.next() );
					totalNumReplies = totalNumReplies + currRepSize;
					//concatResult.put(currArray);
				}
			}

			QueryMsgFromUserReply<NodeIDType> queryMsgFromUserReply 
				= new QueryMsgFromUserReply<NodeIDType>(myID, 
						queryInfo.getQuery(), queryInfo.getGroupGUID(), concatResult, 
						queryInfo.getUserReqID(), totalNumReplies);
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
	
	
	@Override
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
	
	
	private boolean checkOverlapWithSubspaceAttrs(
			HashMap<String, AttributePartitionInfo> attributesOfSubspace, 
				JSONArray attrSetOfAnonymizedID )
	{
		boolean overlapWithSubspace = false;
		
		
		for(int i=0; i < attrSetOfAnonymizedID.length(); i++)
		{
			try 
			{
				String attrName = attrSetOfAnonymizedID.getString(i);
				
				if( attributesOfSubspace.containsKey(attrName) )
				{
					overlapWithSubspace = true;
					break;
				}
				
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
		
		return overlapWithSubspace;
	}
	
	
	private JSONObject getJSONToWriteInPrimarySubspace( JSONObject oldValJSON, 
			JSONObject updateValJSON, JSONArray anonymizedIDToGuidMapping )
	{
		JSONObject jsonToWrite = new JSONObject();
		
		// set the attributes.
		try
		{
			// attributes which are not set should be set to default value
			// for attribute-space hashing
			Iterator<String> attrIter 
							= AttributeTypes.attributeMap.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
				String attrVal = "";
				
				if( updateValJSON.has(attrName) )
				{
					attrVal = updateValJSON.getString(attrName);
					jsonToWrite.put(attrName, attrVal);
				}
				else if( !oldValJSON.has(attrName) )
				{
					attrVal = attrMetaInfo.getARandomValue
									(this.defaultAttrValGenerator);
					jsonToWrite.put(attrName, attrVal);
				}
			}
		
			// update unset attributes
			JSONObject unsetAttrs = HyperspaceHashing.getUnsetAttrJSON(oldValJSON);
			
			if( unsetAttrs != null )
			{
				Iterator<String> updateAttrIter = updateValJSON.keys();
				
				while( updateAttrIter.hasNext() )
				{
					String updateAttr = updateAttrIter.next();
					// just removing attr that is set in this update.
					unsetAttrs.remove(updateAttr);					
				}
			}
			else
			{
				unsetAttrs = new JSONObject();
			
				attrIter = AttributeTypes.attributeMap.keySet().iterator();
			
				while( attrIter.hasNext() )
				{
					String attrName = attrIter.next();
					
					if( !updateValJSON.has(attrName) )
					{
						// just "" string for minium space usage.
						unsetAttrs.put(attrName, "");
					}
				}
			}
			assert(unsetAttrs != null);
			jsonToWrite.put(HyperspaceMySQLDB.unsetAttrsColName, unsetAttrs);
			
		
			// now anonymizedIDToGUIDmapping
			
			boolean alreadyStored 
					= HyperspaceHashing.checkIfAnonymizedIDToGuidInfoAlreadyStored(oldValJSON);
			
			if( !alreadyStored )
			{
				if(anonymizedIDToGuidMapping != null)
				{
					jsonToWrite.put(HyperspaceMySQLDB.anonymizedIDToGUIDMappingColName, 
						anonymizedIDToGuidMapping);
				}
			}
			
			return jsonToWrite;
		}
		catch( Error | Exception ex )
		{
			ex.printStackTrace();
		}
		return null;
	}
	
	
	private void guidValueProcessingOnUpdate( 
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo , 
			JSONObject oldValueJSON , int subspaceId , int  replicaNum ,
			JSONObject updatedAttrValJSON ,
			String GUID , long requestID , boolean firstTimeInsert,
			long updateStartTime, 
			JSONObject primarySubspaceJSON ) throws JSONException
	{
		if( firstTimeInsert )
		{
			processFirstTimeInsertIntoSecondarySubspace( 
					attrsSubspaceInfo, subspaceId , replicaNum ,
					GUID , requestID, updateStartTime, primarySubspaceJSON,  updatedAttrValJSON);
		}
		else
		{
			processUpdateIntoSecondarySubspace( 
					attrsSubspaceInfo, oldValueJSON, subspaceId, replicaNum,
					updatedAttrValJSON, GUID , requestID ,firstTimeInsert, 
					updateStartTime, primarySubspaceJSON );
		}
	}
	
	private void processFirstTimeInsertIntoSecondarySubspace( 
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo,
			int subspaceId , int  replicaNum ,
			String GUID , long requestID,
			long updateStartTime, JSONObject primarySubspaceJSON, JSONObject updateAttrJSON) 
					throws JSONException
	{
		ContextServiceLogger.getLogger().fine
			("processFirstTimeInsertIntoSecondarySubspace "+primarySubspaceJSON);
		Vector<ProcessingQueryComponent> newQueryComponents 
								= new Vector<ProcessingQueryComponent>();
		
		Iterator<String> subspaceAttrIter
								= attrsSubspaceInfo.keySet().iterator();

		// the attribtues that are not set by the user.
		// for those random value set by primary subspace is used for indexing
		while( subspaceAttrIter.hasNext() )
		{
			String attrName = subspaceAttrIter.next();
			
			String value;
			assert(primarySubspaceJSON.has(attrName));
			
			value = primarySubspaceJSON.getString(attrName);
			
			ProcessingQueryComponent qcomponent 
				= new ProcessingQueryComponent(attrName, value, value );
										newQueryComponents.add(qcomponent);
		}

		HashMap<Integer, OverlappingInfoClass> newOverlappingRegion = 
				this.hyperspaceDB.getOverlappingRegionsInSubspace( subspaceId, replicaNum, 
								newQueryComponents );
		
		if(newOverlappingRegion.size() != 1)
		{
			ContextServiceLogger.getLogger().fine("Not 1 Assertion fail primarySubspaceJSON "
						+primarySubspaceJSON +" select query print " );
			this.hyperspaceDB.getOverlappingRegionsInSubspace( subspaceId, replicaNum, 
					newQueryComponents );
			
		}
		assert(newOverlappingRegion.size() == 1);
		
		NodeIDType newRespNodeId 
						= (NodeIDType)newOverlappingRegion.keySet().iterator().next();

		ContextServiceLogger.getLogger().fine
					(" newRespNodeId "+newRespNodeId);
	
		// compute the JSONToWrite
		JSONObject jsonToWrite = new JSONObject();
		
		
		JSONObject unsetAttrsJSON = primarySubspaceJSON.getJSONObject
											(HyperspaceMySQLDB.unsetAttrsColName);
		
		Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
		
		// unset attributes are set to default values, so that they don't 
		// satisfy query constraints. But they are set some random value in primary subspace
		// and they are indexed based on these random values in secondary subspaces.
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			assert(primarySubspaceJSON.has(attrName));
			String attrVal = "";
			if(unsetAttrsJSON.has(attrName))
			{
				attrVal = AttributeTypes.attributeMap.get(attrName).getDefaultValue();
			}
			else
			{
				attrVal = primarySubspaceJSON.getString(attrName);
			}
			
			jsonToWrite.put(attrName, attrVal);
		}
		
		// add the anonymizedIDToGUID mapping if it is there
		if(primarySubspaceJSON.has
				(HyperspaceMySQLDB.anonymizedIDToGUIDMappingColName))
		{
			JSONArray decodingArray 
				= primarySubspaceJSON.getJSONArray(HyperspaceMySQLDB.anonymizedIDToGUIDMappingColName);
			
			assert(decodingArray != null);
			jsonToWrite.put(HyperspaceMySQLDB.anonymizedIDToGUIDMappingColName, 
					decodingArray);
		}
		
		// add entry for reply
		// 1 reply as both old and new goes to same node
		// NOTE: doing this here was a bug, as we are also sending the message out
		// and sometimes replies were coming back quickly before initialization for all 
		// subspaces and the request completion code was assuming that the request was complte
		// before recv replies from all subspaces.
		//updateReq.initializeSubspaceEntry(subspaceId, replicaNum);

		ValueUpdateToSubspaceRegionMessage<NodeIDType>  
						valueUpdateToSubspaceRegionMessage = null;
		
		// need to send oldValueJSON as it is performed as insert
		valueUpdateToSubspaceRegionMessage 
				= new ValueUpdateToSubspaceRegionMessage<NodeIDType>( myID, 
							-1, GUID, jsonToWrite, 
							ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY, 
							subspaceId, requestID, true, updateStartTime, new JSONObject(),
							unsetAttrsJSON, updateAttrJSON, PrivacySchemes.HYPERSPACE_PRIVACY.ordinal());
		
		try
		{
			this.messenger.sendToID
				(newRespNodeId, valueUpdateToSubspaceRegionMessage.toJSONObject());
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	
	private void processUpdateIntoSecondarySubspace( 
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo , 
			JSONObject oldValueJSON , int subspaceId , int  replicaNum ,
			JSONObject updatedAttrValJSON ,
			String GUID , long requestID , boolean firstTimeInsert,
			long updateStartTime, JSONObject primarySubspaceJSON ) throws JSONException
	{
		NodeIDType oldRespNodeId = null, newRespNodeId = null;
		
		// processes the first time insert
		Vector<ProcessingQueryComponent> oldQueryComponents 
											= new Vector<ProcessingQueryComponent>();
		
		Iterator<String> subspaceAttrIter 	= attrsSubspaceInfo.keySet().iterator();
		
		while( subspaceAttrIter.hasNext() )
		{
			String attrName = subspaceAttrIter.next();
			//( String attributeName, String leftOperator, double leftValue, 
			//		String rightOperator, double rightValue )
			String attrVal = oldValueJSON.getString(attrName);
			ProcessingQueryComponent qcomponent = new ProcessingQueryComponent
										( attrName, attrVal, attrVal );
			oldQueryComponents.add(qcomponent);
		}

		HashMap<Integer, OverlappingInfoClass> overlappingRegion = 
				this.hyperspaceDB.getOverlappingRegionsInSubspace
								(subspaceId, replicaNum, oldQueryComponents);

		assert(overlappingRegion.size() == 1);
		
		oldRespNodeId = (NodeIDType)overlappingRegion.keySet().iterator().next();
		

		// for new value
		Vector<ProcessingQueryComponent> newQueryComponents 
						= new Vector<ProcessingQueryComponent>();
		Iterator<String> subspaceAttrIter1 
						= attrsSubspaceInfo.keySet().iterator();

		while( subspaceAttrIter1.hasNext() )
		{
			String attrName = subspaceAttrIter1.next();

			String value;
			if( updatedAttrValJSON.has(attrName) )
			{
				value = updatedAttrValJSON.getString(attrName);
			}
			else
			{
				value = oldValueJSON.getString(attrName);
			}
			ProcessingQueryComponent qcomponent 
					= new ProcessingQueryComponent(attrName, value, value );
			newQueryComponents.add(qcomponent);
		}
		
		HashMap<Integer, OverlappingInfoClass> newOverlappingRegion = 
				this.hyperspaceDB.getOverlappingRegionsInSubspace( subspaceId, replicaNum, 
						newQueryComponents );

		assert(newOverlappingRegion.size() == 1);
		
		newRespNodeId = (NodeIDType)newOverlappingRegion.keySet().iterator().next();
		

		ContextServiceLogger.getLogger().fine
							("oldNodeId "+oldRespNodeId
							+" newRespNodeId "+newRespNodeId);
		JSONObject unsetAttrsJSON = primarySubspaceJSON.getJSONObject
				(HyperspaceMySQLDB.unsetAttrsColName);
		
		// send messages to the subspace region nodes
		if( oldRespNodeId == newRespNodeId )
		{
			// update case
			// add entry for reply
			// 1 reply as both old and new goes to same node
			// NOTE: doing this here was a bug, as we are also sending the message out
			// and sometimes replies were coming back quickly before initialization for all 
			// subspaces and the request completion code was assuming that the request was complte
			// before recv replies from all subspaces.
			//updateReq.initializeSubspaceEntry(subspaceId, replicaNum);

			ValueUpdateToSubspaceRegionMessage<NodeIDType>  
							valueUpdateToSubspaceRegionMessage = null;

			// just need to send update attr values
			valueUpdateToSubspaceRegionMessage 
					= new ValueUpdateToSubspaceRegionMessage<NodeIDType>( myID, 
							-1, GUID, updatedAttrValJSON, 
							ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY, 
							subspaceId, requestID, firstTimeInsert, updateStartTime, 
							oldValueJSON, unsetAttrsJSON, updatedAttrValJSON, PrivacySchemes.HYPERSPACE_PRIVACY.ordinal());
			
			try
			{
				this.messenger.sendToID
					(oldRespNodeId, valueUpdateToSubspaceRegionMessage.toJSONObject());
			}
			catch (IOException e)
			{
				e.printStackTrace();
			}
			catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		else
		{
			// add entry for reply
			// 2 reply as both old and new goes to different node
			//updateReq.initializeSubspaceEntry(subspaceId, replicaNum);
			// it is a remove, so no need for update and old JSON.
			ValueUpdateToSubspaceRegionMessage<NodeIDType>  oldValueUpdateToSubspaceRegionMessage 
					= new ValueUpdateToSubspaceRegionMessage<NodeIDType>( myID, -1, 
							GUID, updatedAttrValJSON, 
							ValueUpdateToSubspaceRegionMessage.REMOVE_ENTRY, 
							subspaceId, requestID, firstTimeInsert, updateStartTime,
							oldValueJSON, unsetAttrsJSON, updatedAttrValJSON, PrivacySchemes.HYPERSPACE_PRIVACY.ordinal() );
			
			try
			{
				this.messenger.sendToID(oldRespNodeId, 
							oldValueUpdateToSubspaceRegionMessage.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			
			// FIXME: send full JSON, so that a new entry can be inserted 
			// involving attributes which were not updated.
			// need to send old JSON here, so that full guid entry can be added 
			// not just the updated attrs
			
			JSONObject jsonToWrite = new JSONObject();
			
			
			// attr values
			Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				String attrVal = "";
				
				if(updatedAttrValJSON.has(attrName))
				{
					attrVal = updatedAttrValJSON.getString(attrName);
				}
				else
				{
					if(unsetAttrsJSON.has(attrName))
					{
						attrVal = AttributeTypes.attributeMap.get(attrName).getDefaultValue();
					}
					else
					{
						assert(oldValueJSON.has(attrName));
						attrVal = oldValueJSON.getString(attrName);
					}
				}
				jsonToWrite.put(attrName, attrVal);
			}
			
			// anonymizedIDToGUID mapping
			if(oldValueJSON.has(HyperspaceMySQLDB.anonymizedIDToGUIDMappingColName))
			{
				JSONArray decodingArray = oldValueJSON.getJSONArray
							(HyperspaceMySQLDB.anonymizedIDToGUIDMappingColName);
				
				assert(decodingArray!= null);
				jsonToWrite.put(HyperspaceMySQLDB.anonymizedIDToGUIDMappingColName
						, decodingArray);
			}
			
			ValueUpdateToSubspaceRegionMessage<NodeIDType>  
				newValueUpdateToSubspaceRegionMessage = 
						new ValueUpdateToSubspaceRegionMessage<NodeIDType>( myID, -1, 
								GUID, jsonToWrite,
								ValueUpdateToSubspaceRegionMessage.ADD_ENTRY, 
								subspaceId, requestID, false, updateStartTime,
								oldValueJSON, unsetAttrsJSON, updatedAttrValJSON, 
								PrivacySchemes.HYPERSPACE_PRIVACY.ordinal());
			
			try
			{
				this.messenger.sendToID(newRespNodeId, 
						newValueUpdateToSubspaceRegionMessage.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public int processValueUpdateToSubspaceRegionMessage( 
			ValueUpdateToSubspaceRegionMessage<NodeIDType> valueUpdateToSubspaceRegionMessage, 
			int replicaNum )
	{
		int subspaceId  		= valueUpdateToSubspaceRegionMessage.getSubspaceNum();
		String GUID 			= valueUpdateToSubspaceRegionMessage.getGUID();
		JSONObject jsonToWrite  = valueUpdateToSubspaceRegionMessage.getJSONToWrite();
		int operType 			= valueUpdateToSubspaceRegionMessage.getOperType();
		boolean firstTimeInsert = valueUpdateToSubspaceRegionMessage.getFirstTimeInsert();
		
		long udpateStartTime 	= valueUpdateToSubspaceRegionMessage.getUpdateStartTime();
		
		String tableName 		= "subspaceId"+subspaceId+"DataStorage";
		
		int numRep = 1;
		
		try 
		{
			switch(operType)
			{
				case ValueUpdateToSubspaceRegionMessage.ADD_ENTRY:
				{
					numRep = 2;
					//if(!ContextServiceConfig.DISABLE_SECONDARY_SUBSPACES_UPDATES)
					{
						this.hyperspaceDB.storeGUIDInSecondarySubspace
							(tableName, GUID, jsonToWrite, HyperspaceMySQLDB.INSERT_REC, 
								subspaceId);
					}
					break;
				}
				case ValueUpdateToSubspaceRegionMessage.REMOVE_ENTRY:
				{
					numRep = 2;
					//if(!ContextServiceConfig.DISABLE_SECONDARY_SUBSPACES_UPDATES)
					{
						this.hyperspaceDB.deleteGUIDFromSubspaceRegion
											(tableName, GUID, subspaceId);
					}
					break;
				}
				case ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY:
				{
					numRep = 1;
					//if(!ContextServiceConfig.DISABLE_SECONDARY_SUBSPACES_UPDATES)
					{
						if( firstTimeInsert )
						{
							this.hyperspaceDB.storeGUIDInSecondarySubspace
								(tableName, GUID, jsonToWrite, HyperspaceMySQLDB.INSERT_REC, 
									subspaceId);
						}
						else
						{
							this.hyperspaceDB.storeGUIDInSecondarySubspace(tableName, GUID, 
									jsonToWrite, HyperspaceMySQLDB.UPDATE_REC, 
									subspaceId);
						}
					}
					break;
				}
			}
			
			if(ContextServiceConfig.DEBUG_MODE)
			{
				long now = System.currentTimeMillis();
				System.out.println("processValueUpdateToSubspaceRegionMessage completes "
						+(now-udpateStartTime));
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return numRep;
	}
	
	/**
	 * Returns subspace number of the maximum overlapping
	 * subspace. Used in processing search query.
	 * @return
	 */
	private int getMaxOverlapSubspace( HashMap<String, ProcessingQueryComponent> pqueryComponents, 
			Vector<ProcessingQueryComponent> matchingAttributes )
	{
		// first the maximum matching subspace is found and then any of its replica it chosen
		Iterator<Integer> keyIter   	= subspaceInfoMap.keySet().iterator();
		int maxMatchingAttrs 			= 0;
		
		HashMap<Integer, Vector<MaxAttrMatchingStorageClass>> matchingSubspaceHashMap = 
				new HashMap<Integer, Vector<MaxAttrMatchingStorageClass>>();
		
		while( keyIter.hasNext() )
		{
			int subspaceId = keyIter.next();
			SubspaceInfo<NodeIDType> currSubInfo = subspaceInfoMap.get(subspaceId).get(0);
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo = currSubInfo.getAttributesOfSubspace();
			
			int currMaxMatch = 0;
			Vector<ProcessingQueryComponent> currMatchingComponents = new Vector<ProcessingQueryComponent>();
			
			Iterator<String> attrIter = pqueryComponents.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				ProcessingQueryComponent pqc = pqueryComponents.get(attrName);
				if( attrsSubspaceInfo.containsKey(pqc.getAttributeName()) )
				{
					currMaxMatch = currMaxMatch + 1;
					currMatchingComponents.add(pqc);
				}
			}
			
			if(currMaxMatch >= maxMatchingAttrs)
			{
				maxMatchingAttrs = currMaxMatch;
				MaxAttrMatchingStorageClass maxAttrMatchObj = new MaxAttrMatchingStorageClass();
				maxAttrMatchObj.currMatchingComponents = currMatchingComponents;
				maxAttrMatchObj.subspaceId = subspaceId;
				
				if(matchingSubspaceHashMap.containsKey(currMaxMatch))
				{
					matchingSubspaceHashMap.get(currMaxMatch).add(maxAttrMatchObj);
				}
				else
				{
					Vector<MaxAttrMatchingStorageClass> currMatchingSubspaceNumVector 
																= new Vector<MaxAttrMatchingStorageClass>();
					currMatchingSubspaceNumVector.add(maxAttrMatchObj);
					matchingSubspaceHashMap.put(currMaxMatch, currMatchingSubspaceNumVector);
				}
			}
		}
		
		Vector<MaxAttrMatchingStorageClass> maxMatchingSubspaceNumVector 
			= matchingSubspaceHashMap.get(maxMatchingAttrs);
		
		int returnIndex = replicaChoosingRand.nextInt( maxMatchingSubspaceNumVector.size() );
		matchingAttributes.clear();
		matchingAttributes.addAll(maxMatchingSubspaceNumVector.get(returnIndex).currMatchingComponents);
		
		String print = "size "+maxMatchingSubspaceNumVector.size()+" ";
		for(int i=0;i<maxMatchingSubspaceNumVector.size();i++)
		{
			print = print + maxMatchingSubspaceNumVector.get(i).subspaceId+" ";
		}
		print = print + " chosen "+maxMatchingSubspaceNumVector.get(returnIndex).subspaceId;
		
		return maxMatchingSubspaceNumVector.get(returnIndex).subspaceId;
	}
	
	private class MaxAttrMatchingStorageClass
	{
		public int subspaceId;
		public Vector<ProcessingQueryComponent> currMatchingComponents;
	}
}