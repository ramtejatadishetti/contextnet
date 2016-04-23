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

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.gns.GNSCalls;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionReplyMessage;
import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;
import edu.umass.cs.contextservice.messages.dataformat.ParsingMethods;
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
	
	public GUIDAttrValueProcessing(NodeIDType myID, HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> 
		subspaceInfoMap , HyperspaceMySQLDB<NodeIDType> hyperspaceDB, 
		JSONMessenger<NodeIDType> messenger , ExecutorService nodeES ,
		ConcurrentHashMap<Long, QueryInfo<NodeIDType>> pendingQueryRequests )
	{
		this.nodeES = nodeES;
		this.myID = myID;
		this.messenger = messenger;
		replicaChoosingRand = new Random(myID.hashCode());

		this.subspaceInfoMap = subspaceInfoMap;
		this.hyperspaceDB = hyperspaceDB;
		
		this.pendingQueryRequests = pendingQueryRequests;
		
		generateSubspacePartitions();
	}
	
	
	/**
	 * recursive function to generate all the
	 * subspace regions/partitions.
	 */
	public void generateSubspacePartitions()
	{
		ContextServiceLogger.getLogger().fine(" generateSubspacePartitions() entering " );
		
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
				try {
					this.subspacePartitionInsertLock.wait();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
		ContextServiceLogger.getLogger().fine(" generateSubspacePartitions() completed " );
	}
	
	public QueryInfo<NodeIDType> processQueryMsgFromUser
		(QueryMsgFromUser<NodeIDType> queryMsgFromUser)
	{

		String query;
		long userReqID;
		String userIP;
		int userPort;
		
		query   = queryMsgFromUser.getQuery();
		userReqID = queryMsgFromUser.getUserReqNum();
		userIP  = queryMsgFromUser.getSourceIP();
		userPort   = queryMsgFromUser.getSourcePort();
		
		ContextServiceLogger.getLogger().fine("QUERY RECVD: QUERY_MSG recvd query recvd "+query);
		
		// create the empty group in GNS
		String grpGUID = GNSCalls.createQueryGroup(query);
		
		if( grpGUID.length() <= 0 )
		{
			ContextServiceLogger.getLogger().fine
			("Query request failed at the recieving node "+queryMsgFromUser);
			return null;
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
		
		QueryInfo<NodeIDType> currReq  
			= new QueryInfo<NodeIDType>( query, myID, grpGUID, userReqID, 
					userIP, userPort);
		
		
		Vector<ProcessingQueryComponent> matchingQueryComponents = new Vector<ProcessingQueryComponent>();
		int maxMatchingSubspaceId = getMaxOverlapSubspace(currReq.getProcessingQC(), matchingQueryComponents);
		
		ContextServiceLogger.getLogger().fine("userReqID "+userReqID+" maxMatchingSubspaceNum "+maxMatchingSubspaceId+" matchingQueryComponents "
				+matchingQueryComponents.size()+" query "+query);
		/*for(int i=0;i<matchingQueryComponents.size();i++)
		{
			ProcessingQueryComponent pqc = matchingQueryComponents.get(i);
			ContextServiceLogger.getLogger().fine("matching Comp "+pqc.getAttributeName()+" "+pqc.getLowerBound()+" "+pqc.getUpperBound());
		}*/
		
		// get number of nodes/or regions to send to in that subspace.
		
		// choose a replica randomly
		Vector<SubspaceInfo<NodeIDType>> maxMatchingSubspaceReplicas 
			= subspaceInfoMap.get(maxMatchingSubspaceId);
		int replicaNum = maxMatchingSubspaceReplicas.get(this.replicaChoosingRand.nextInt(maxMatchingSubspaceReplicas.size())).getReplicaNum();
		
	    HashMap<Integer, OverlappingInfoClass> respNodeIdList 
	    		= this.hyperspaceDB.getOverlappingRegionsInSubspace(maxMatchingSubspaceId, replicaNum, matchingQueryComponents);
	    
		synchronized(this.pendingQueryLock)
		{
			currReq.setQueryRequestID(queryIdCounter++);
		}
		
		pendingQueryRequests.put(currReq.getRequestId(), currReq);
		
		ContextServiceLogger.getLogger().fine("processQueryMsgFromUser respNodeIdList size "+respNodeIdList.size()+
	    		" requestId "+currReq.getRequestId() +" maxMatchingSubspaceNum "+maxMatchingSubspaceId);
		
	    currReq.initializeRegionalReplies(respNodeIdList);
		
	    Iterator<Integer> respNodeIdIter = respNodeIdList.keySet().iterator();
	    
	    while( respNodeIdIter.hasNext() )
	    {
	    	Integer respNodeId = respNodeIdIter.next();
	    	OverlappingInfoClass overlapInfo = respNodeIdList.get(respNodeId);
	    	
	    	QueryMesgToSubspaceRegion<NodeIDType> queryMesgToSubspaceRegion = 
					new QueryMesgToSubspaceRegion<NodeIDType>
	    (myID, currReq.getRequestId(), query, grpGUID, maxMatchingSubspaceId, userIP, userPort, overlapInfo.hashCode);
	    	
			try
			{
				this.messenger.sendToID( (NodeIDType)respNodeId, queryMesgToSubspaceRegion.toJSONObject() );
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
	    return currReq;
	}
	
	
	public void processQueryMesgToSubspaceRegion(QueryMesgToSubspaceRegion<NodeIDType> 
													queryMesgToSubspaceRegion)
	{
		String query 			= queryMesgToSubspaceRegion.getQuery();
		String groupGUID 		= queryMesgToSubspaceRegion.getGroupGUID();
		int subspaceId 			= queryMesgToSubspaceRegion.getSubspaceNum();
		JSONArray resultGUIDs = new JSONArray();
		
		int resultSize = this.hyperspaceDB.processSearchQueryInSubspaceRegion
				(subspaceId, query, resultGUIDs);
		
		QueryMesgToSubspaceRegionReply<NodeIDType> queryMesgToSubspaceRegionReply = 
				new QueryMesgToSubspaceRegionReply<NodeIDType>( myID, queryMesgToSubspaceRegion.getRequestId(), 
						groupGUID, resultGUIDs, resultSize);
		
		try
		{
			this.messenger.sendToID(queryMesgToSubspaceRegion.getSender(), queryMesgToSubspaceRegionReply.toJSONObject());
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		ContextServiceLogger.getLogger().info("Sending queryMesgToSubspaceRegionReply mesg from " 
				+ myID +" to node "+queryMesgToSubspaceRegion.getSender());
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

			if( ContextServiceConfig.sendFullReplies )
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
		
		Random rand = new Random();
		
		int returnIndex = rand.nextInt( maxMatchingSubspaceNumVector.size() );
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
	
	public void processValueUpdateToSubspaceRegionMessage( 
			ValueUpdateToSubspaceRegionMessage<NodeIDType> valueUpdateToSubspaceRegionMessage )
	{
		int subspaceId = valueUpdateToSubspaceRegionMessage.getSubspaceNum();
		String GUID = valueUpdateToSubspaceRegionMessage.getGUID();
		JSONObject updateValPairs = valueUpdateToSubspaceRegionMessage.getUpdateAttrValuePairs();
		int operType = valueUpdateToSubspaceRegionMessage.getOperType();
		int replicaNum = getTheReplicaNumForASubspace(subspaceId);
		boolean firstTimeInsert = valueUpdateToSubspaceRegionMessage.getFirstTimeInsert();
		// format of this json is different than updateValPairs, this json is created after 
		// reading attribute value pairs and ACL<attrName> info from primary subspace
		JSONObject oldValJSON = valueUpdateToSubspaceRegionMessage.getOldAttrValuePairs();
		
		String tableName 	= "subspaceId"+subspaceId+"DataStorage";
		try 
		{
			
			HashMap<String, AttrValueRepresentationJSON> attrValMap =
					ParsingMethods.getAttrValueMap(updateValPairs);
			
			int numRep = 1;
			switch(operType)
			{
				case ValueUpdateToSubspaceRegionMessage.ADD_ENTRY:
				{
					numRep = 2;
					//if(!ContextServiceConfig.DISABLE_SECONDARY_SUBSPACES_UPDATES)
					{
						
						this.hyperspaceDB.storeGUIDInSecondarySubspace
						(tableName, GUID, attrValMap, HyperspaceMySQLDB.INSERT_REC, 
								subspaceId, oldValJSON);
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
						if(firstTimeInsert)
						{
							this.hyperspaceDB.storeGUIDInSecondarySubspace
							(tableName, GUID, attrValMap, HyperspaceMySQLDB.INSERT_REC, 
									subspaceId, oldValJSON);
						}
						else
						{
							this.hyperspaceDB.storeGUIDInSecondarySubspace(tableName, GUID, attrValMap, 
									HyperspaceMySQLDB.UPDATE_REC, subspaceId, oldValJSON);
						}
					}
					break;
				}
			}
			

			//ContextServiceLogger.getLogger().fine("Sending valueUpdateToSubspaceRegionReplyMessage to "
			//				+valueUpdateToSubspaceRegionMessage.getSender()+" from "+this.getMyID());
			
			ValueUpdateToSubspaceRegionReplyMessage<NodeIDType>  valueUpdateToSubspaceRegionReplyMessage 
				= new ValueUpdateToSubspaceRegionReplyMessage<NodeIDType>(myID, 
						valueUpdateToSubspaceRegionMessage.getVersionNum(), numRep, 
						valueUpdateToSubspaceRegionMessage.getRequestID(), subspaceId, replicaNum);
			
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
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	
	/**
	 * returns the replica num for a subspace
	 * One nodeid should have just one replica num
	 * as it can belong to just one replica of that subspace
	 * @return
	 */
	private int getTheReplicaNumForASubspace( int subpsaceId )
	{
		Vector<SubspaceInfo<NodeIDType>> replicasVect 
				= subspaceInfoMap.get(subpsaceId);
		
		int replicaNum = -1;
		for( int i=0;i<replicasVect.size();i++ )
		{
			SubspaceInfo<NodeIDType> subInfo = replicasVect.get(i);
			if( subInfo.checkIfSubspaceHasMyID(myID) )
			{
				replicaNum = subInfo.getReplicaNum();
				break;
			}
		}
		return replicaNum;
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
//				hyperspaceDB.insertIntoSubspacePartitionInfo(subspaceId, replicaNum, 
//						permVector, respNodeId);
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