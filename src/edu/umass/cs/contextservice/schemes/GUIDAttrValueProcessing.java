package edu.umass.cs.contextservice.schemes;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
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
//import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;
//import edu.umass.cs.contextservice.messages.dataformat.ParsingMethods;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.nio.JSONMessenger;

public class GUIDAttrValueProcessing<NodeIDType> implements 
								GUIDAttrValueProcessingInterface<NodeIDType>
{
	private final HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> 
					subspaceInfoMap;
	
	private final HyperspaceMySQLDB<NodeIDType> hyperspaceDB;
	
	private final Random replicaChoosingRand;
	
	private final NodeIDType myID;
	
	private final JSONMessenger<NodeIDType> messenger;
	
	private final Object subspacePartitionInsertLock									= new Object();
	private final ExecutorService nodeES;
	
	private final ConcurrentHashMap<Long, QueryInfo<NodeIDType>> pendingQueryRequests;
	
	// this can be a huge number, it is exponential in numebr of attributes.
	private long subspacePartitionInsertSent											= 0;
	private long subspacePartitionInsertCompl											= 0;
	
	private final Object pendingQueryLock												= new Object();
	
	private long queryIdCounter															= 0;
	
	private final ProfilerStatClass profStats;
	
	public GUIDAttrValueProcessing( NodeIDType myID, HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> 
		subspaceInfoMap , HyperspaceMySQLDB<NodeIDType> hyperspaceDB, 
		JSONMessenger<NodeIDType> messenger , ExecutorService nodeES ,
		ConcurrentHashMap<Long, QueryInfo<NodeIDType>> pendingQueryRequests, 
		ProfilerStatClass profStats )
	{
		this.nodeES = nodeES;
		this.myID = myID;
		this.messenger = messenger;
		replicaChoosingRand = new Random(myID.hashCode());

		this.subspaceInfoMap = subspaceInfoMap;
		this.hyperspaceDB = hyperspaceDB;
		
		this.profStats = profStats;
		
		this.pendingQueryRequests = pendingQueryRequests;
		
		generateSubspacePartitions();
	}
	
	/**
	 * recursive function to generate all the
	 * subspace regions/partitions.
	 */
	public void generateSubspacePartitions()
	{
		ContextServiceLogger.getLogger().fine
								(" generateSubspacePartitions() entering " );
		
		Iterator<Integer> subspaceIter = subspaceInfoMap.keySet().iterator();
		
		while( subspaceIter.hasNext() )
		{
			int subspaceId = subspaceIter.next();
			Vector<SubspaceInfo<NodeIDType>> replicaVect 
								= subspaceInfoMap.get(subspaceId);
			
			for( int i=0; i<replicaVect.size(); i++ )
			{
				SubspaceInfo<NodeIDType> subspaceInfo = replicaVect.get(i);
				HashMap<String, AttributePartitionInfo> attrsOfSubspace 
										= subspaceInfo.getAttributesOfSubspace();
				
				Vector<NodeIDType> nodesOfSubspace = subspaceInfo.getNodesOfSubspace();
				
				double numAttr  = attrsOfSubspace.size();
				//double numNodes = nodesOfSubspace.size();
				ContextServiceLogger.getLogger().fine(" NumPartitions "
												+subspaceInfo.getNumPartitions() );
				
				Integer[] partitionNumArray = new Integer[subspaceInfo.getNumPartitions()];
				for(int j = 0; j<partitionNumArray.length; j++)
				{
					partitionNumArray[j] = new Integer(j);
					//ContextServiceLogger.getLogger().fine("partitionNumArray[j] "+j+" "+partitionNumArray[j]);
				}
				
				// Create the initial vector of 2 elements (apple, orange)
				ICombinatoricsVector<Integer> originalVector = Factory.createVector(partitionNumArray);
				
			    //ICombinatoricsVector<Integer> originalVector = Factory.createVector(new String[] { "apple", "orange" });

				// Create the generator by calling the appropriate method in the Factory class. 
				// Set the second parameter as 3, since we will generate 3-elemets permutations
				Generator<Integer> gen = Factory.createPermutationWithRepetitionGenerator(originalVector, (int)numAttr);
				
				// Print the result
				int nodeIdCounter = 0;
				int sizeOfNumNodes = nodesOfSubspace.size();
				List<List<Integer>> subspaceVectList = new LinkedList<List<Integer>>();
				List<NodeIDType> respNodeIdList = new LinkedList<NodeIDType>();
				long counter = 0;
				for( ICombinatoricsVector<Integer> perm : gen )
				{
					NodeIDType respNodeId = nodesOfSubspace.get(nodeIdCounter%sizeOfNumNodes);
					//ContextServiceLogger.getLogger().fine("perm.getVector() "+perm.getVector());
					counter++;
					
					if(counter % ContextServiceConfig.SUBSPACE_PARTITION_INSERT_BATCH_SIZE == 0)
					{
						subspaceVectList.add(perm.getVector());
						respNodeIdList.add(respNodeId);
						
						synchronized(this.subspacePartitionInsertLock)
						{
							this.subspacePartitionInsertSent++;
						}
						
						DatabaseOperationClass dbOper = new DatabaseOperationClass(subspaceInfo.getSubspaceId(), subspaceInfo.getReplicaNum(), 
								subspaceVectList, respNodeIdList);
						//dbOper.run();
						
						nodeES.execute(dbOper);
						
						// repointing it to a new list, and the pointer to the old list is passed to the DatabaseOperation class
						subspaceVectList = new LinkedList<List<Integer>>();
						respNodeIdList = new LinkedList<NodeIDType>();
						
						
						nodeIdCounter++;
					}
					else
					{
						subspaceVectList.add(perm.getVector());
						respNodeIdList.add(respNodeId);
						nodeIdCounter++;
					}
				}
				// adding the remaning ones
				if(subspaceVectList.size() > 0)
				{
					synchronized(this.subspacePartitionInsertLock)
					{
						this.subspacePartitionInsertSent++;
					}
					
					DatabaseOperationClass dbOper = new DatabaseOperationClass(subspaceInfo.getSubspaceId(), subspaceInfo.getReplicaNum(), 
							subspaceVectList, respNodeIdList);
					
					nodeES.execute(dbOper);
					
					// repointing it to a new list, and the pointer to the old list is passed to the DatabaseOperation class
					subspaceVectList = new LinkedList<List<Integer>>();
					respNodeIdList = new LinkedList<NodeIDType>();
				}
			}
		}
		
		
		synchronized(this.subspacePartitionInsertLock)
		{
			while(this.subspacePartitionInsertSent != this.subspacePartitionInsertCompl)
			{
				try 
				{
					this.subspacePartitionInsertLock.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		ContextServiceLogger.getLogger().fine
							(" generateSubspacePartitions() completed " );
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
	    					storeQueryForTrigger, expiryTime);
	    	
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
	public void guidValueProcessingOnUpdate( 
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
							unsetAttrsJSON, updateAttrJSON);
		
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
							oldValueJSON, unsetAttrsJSON, updatedAttrValJSON);
			
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
							oldValueJSON, unsetAttrsJSON, updatedAttrValJSON );
			
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
								oldValueJSON, unsetAttrsJSON, updatedAttrValJSON);
			
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
	
	private class DatabaseOperationClass implements Runnable
	{
		private final int subspaceId;
		private final int replicaNum;
		private final List<List<Integer>> permVectorList;
		private final List<NodeIDType> respNodeIdList;
		
		public DatabaseOperationClass(int subspaceId, int replicaNum, 
				List<List<Integer>> permVectorList
				, List<NodeIDType> respNodeIdList)
		{
			this.subspaceId = subspaceId;
			this.replicaNum = replicaNum;
			this.permVectorList = permVectorList;
			this.respNodeIdList = respNodeIdList;
		}
		
		@Override
		public void run() 
		{
			try
			{
				hyperspaceDB.bulkInsertIntoSubspacePartitionInfo(subspaceId, replicaNum, 
						permVectorList, respNodeIdList);
				synchronized(subspacePartitionInsertLock)
				{
					subspacePartitionInsertCompl++;
					if(subspacePartitionInsertCompl == subspacePartitionInsertSent)
					{
						subspacePartitionInsertLock.notify();
					}
				}
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
			catch(Error ex)
			{
				ex.printStackTrace();
			}
		}
	}
}