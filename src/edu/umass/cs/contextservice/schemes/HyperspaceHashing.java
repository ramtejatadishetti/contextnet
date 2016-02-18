package edu.umass.cs.contextservice.schemes;


import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;
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

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.configurator.AbstractSubspaceConfigurator;
import edu.umass.cs.contextservice.configurator.BasicSubspaceConfigurator;
import edu.umass.cs.contextservice.configurator.SubspaceConfigurator;
import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.gns.GNSCalls;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ClientConfigReply;
import edu.umass.cs.contextservice.messages.ClientConfigRequest;
import edu.umass.cs.contextservice.messages.ContextServicePacket.PacketType;
import edu.umass.cs.contextservice.messages.GetMessage;
import edu.umass.cs.contextservice.messages.GetReplyMessage;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.QueryTriggerMessage;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.contextservice.messages.UpdateTriggerMessage;
import edu.umass.cs.contextservice.messages.UpdateTriggerReply;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNSReply;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionReplyMessage;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.updates.GUIDUpdateInfo;
import edu.umass.cs.contextservice.updates.UpdateInfo;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;

public class HyperspaceHashing<NodeIDType> extends AbstractScheme<NodeIDType>
{
	// because d710 Exp nodes have 8 processors with 4 cores each
	public static final int THREAD_POOL_SIZE											= 32;
	
	private final ExecutorService nodeES;
	
	private long numberOfQueryFromUser													= 0;
	private long numberOfQueryFromUserDepart											= 0;
	private long numberOfQuerySubspaceRegion											= 0;
	private long numberOfQuerySubspaceRegionReply										= 0;
	
	private HashMap<String, GUIDUpdateInfo<NodeIDType>> guidUpdateInfoMap				= null;
	
	private final AbstractSubspaceConfigurator<NodeIDType> subspaceConfigurator;
	
	private final Random replicaChoosingRand;
	
	public static final Logger log 														= ContextServiceLogger.getLogger();
	
	public HyperspaceHashing(NodeConfig<NodeIDType> nc,
			JSONMessenger<NodeIDType> m)
	{
		super(nc, m);
		
		replicaChoosingRand = new Random();
		guidUpdateInfoMap = new HashMap<String, GUIDUpdateInfo<NodeIDType>>();
			
		if( ContextServiceConfig.basicSubspaceConfig )
		{
			subspaceConfigurator = new BasicSubspaceConfigurator<NodeIDType>(messenger.getNodeConfig());
		}
		else
		{
			subspaceConfigurator = new SubspaceConfigurator<NodeIDType>(messenger.getNodeConfig());
		}
		
		ContextServiceLogger.getLogger().fine("configure subspace started");
		// configure subspaces
		subspaceConfigurator.configureSubspaceInfo();
		ContextServiceLogger.getLogger().fine("configure subspace completed");
		
		try
		{
			hyperspaceDB = new HyperspaceMySQLDB<NodeIDType>(this.getMyID(), subspaceConfigurator.getSubspaceInfoMap());
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		//ContextServiceLogger.getLogger().fine("HyperspaceMySQLDB completed");
		
		generateSubspacePartitions();
		//nodeES = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		//ContextServiceLogger.getLogger().fine("generateSubspacePartitions completed");
		nodeES = Executors.newCachedThreadPool();
		
		new Thread(new ProfilerStatClass()).start();
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryMsgFromUser(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
	{
		//ContextServiceLogger.getLogger().fine("handleQueryMsgFromUser");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateFromGNS(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		//ContextServiceLogger.getLogger().fine("handleValueUpdateFromGNS");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryMesgToSubspaceRegion(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		//ContextServiceLogger.getLogger().fine("handleQueryMesgToSubspaceRegion");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryMesgToSubspaceRegionReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
	{
		//ContextServiceLogger.getLogger().fine("handleQueryMesgToSubspaceRegionReply");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateToSubspaceRegionMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
	{
		//ContextServiceLogger.getLogger().fine("handleValueUpdateToSubspaceRegionMessage");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleGetMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		//ContextServiceLogger.getLogger().fine("handleValueUpdateToSubspaceRegionMessage");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleGetReplyMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		//ContextServiceLogger.getLogger().fine("handleValueUpdateToSubspaceRegionMessage");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateToSubspaceRegionReplyMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		//ContextServiceLogger.getLogger().fine("handleValueUpdateToSubspaceRegionMessage");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryTriggerMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleUpdateTriggerMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleUpdateTriggerReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleClientConfigRequest(ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public NodeIDType getResponsibleNodeId(String AttrName)
	{
		int numNodes = this.allNodeIDs.size();
		//String attributeHash = Utils.getSHA1(attributeName);
		int mapIndex = Hashing.consistentHash(AttrName.hashCode(), numNodes);
		@SuppressWarnings("unchecked")
		NodeIDType[] allNodeIDArr = (NodeIDType[]) this.allNodeIDs.toArray();
		
		return allNodeIDArr[mapIndex];
	}
	
	/**
	 * recursive function to generate all the
	 * subspace regions/partitions.
	 */
	private void generateSubspacePartitions()
	{
		HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap 
			= this.subspaceConfigurator.getSubspaceInfoMap();
		
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
				for( ICombinatoricsVector<Integer> perm : gen )
				{
					NodeIDType respNodeId = nodesOfSubspace.get(nodeIdCounter%sizeOfNumNodes);
					//ContextServiceLogger.getLogger().fine("perm.getVector() "+perm.getVector());
					hyperspaceDB.insertIntoSubspacePartitionInfo(subspaceInfo.getSubspaceId(), subspaceInfo.getReplicaNum(), 
							perm.getVector(), respNodeId);
					//ContextServiceLogger.getLogger().fine("hyperspaceDB.insertIntoSubspacePartitionInfo complete");
					nodeIdCounter++;
				}
			}
		}
	}
	
	
	@SuppressWarnings("unchecked")
	private void processQueryMsgFromUser
		(QueryMsgFromUser<NodeIDType> queryMsgFromUser)
	{
		this.numberOfQueryFromUser++;
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
			ContextServiceLogger.getLogger().fine("Query request failed at the recieving node "+queryMsgFromUser);
			return;
		}
		
		//Vector<QueryComponent> qcomponents = QueryParser.parseQueryNew(query);
		//FIXME: for conflicting queries 
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
			= new QueryInfo<NodeIDType>( query, getMyID(), grpGUID, userReqID, userIP, userPort);
		
		
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
			= this.subspaceConfigurator.getSubspaceInfoMap().get(maxMatchingSubspaceId);
		int replicaNum = maxMatchingSubspaceReplicas.get(this.replicaChoosingRand.nextInt(maxMatchingSubspaceReplicas.size())).getReplicaNum();
		
	    HashMap<Integer, OverlappingInfoClass> respNodeIdList 
	    		= this.hyperspaceDB.getOverlappingRegionsInSubspace(maxMatchingSubspaceId, replicaNum, matchingQueryComponents);
	    
		synchronized(this.pendingQueryLock)
		{
			currReq.setQueryRequestID(queryIdCounter++);
		}
		
		pendingQueryRequests.put(currReq.getRequestId(), currReq);
		
		log.fine("processQueryMsgFromUser respNodeIdList size "+respNodeIdList.size()+
	    		" requestId "+currReq.getRequestId() +" maxMatchingSubspaceNum "+maxMatchingSubspaceId);
		
	    currReq.initializeRegionalReplies(respNodeIdList);
		
	    Iterator<Integer> respNodeIdIter = respNodeIdList.keySet().iterator();
	    
	    while( respNodeIdIter.hasNext() )
	    {
	    	Integer respNodeId = respNodeIdIter.next();
	    	OverlappingInfoClass overlapInfo = respNodeIdList.get(respNodeId);
	    	
	    	QueryMesgToSubspaceRegion<NodeIDType> queryMesgToSubspaceRegion = 
					new QueryMesgToSubspaceRegion<NodeIDType>
	    (getMyID(), currReq.getRequestId(), query, grpGUID, maxMatchingSubspaceId, userIP, userPort, overlapInfo.hashCode);
	    	
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
					+ getMyID() +" to node "+respNodeId);
	    }
	    
	    // trigger information like userIP, userPort 
	    // are stored for each attribute in the query one at a time
	    // We use value of one attribute and use the default value of other attributes 
	    // and do this for each attribute in turn.
	    //FIXME: check trigger with replication
	    if( ContextServiceConfig.TRIGGER_ENABLED )
	    {
	    	processTriggerOnQueryMsgFromUser(currReq);
	    }
	    this.numberOfQueryFromUserDepart++;
	}
	
	private void processTriggerOnQueryMsgFromUser(QueryInfo<NodeIDType> currReq)
	{
		
		HashMap<Integer, Vector<ProcessingQueryComponent>> overlappingSubspaces =
    			new HashMap<Integer, Vector<ProcessingQueryComponent>>();
		getAllUniqueOverlappingSubspaces( currReq.getProcessingQC(), overlappingSubspaces );
    	
    	Iterator<Integer> overlapSubspaceIter = overlappingSubspaces.keySet().iterator();
    	HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subapceInfoMap = 
    			this.subspaceConfigurator.getSubspaceInfoMap();
    	while( overlapSubspaceIter.hasNext() )
    	{
    		int subspaceId = overlapSubspaceIter.next();
    		Vector<SubspaceInfo<NodeIDType>> replicasVect 
    										= subapceInfoMap.get(subspaceId);
    		
    		// trigger info on a query just goes to any one random replica of a subspace
    		// it doesn't need to be stored on all replicas of a subspace
    		SubspaceInfo<NodeIDType> currSubInfo = replicasVect.get(this.replicaChoosingRand.nextInt(replicasVect.size()));
    		int replicaNum = currSubInfo.getReplicaNum();
    		Vector<ProcessingQueryComponent> matchingComp = overlappingSubspaces.get(subspaceId);
    		
    		for(int i=0; i<matchingComp.size(); i++)
    		{
    			ProcessingQueryComponent matchingQComp = matchingComp.get(i);
    			
    			String currMatchingAttr = matchingQComp.getAttributeName();
    			
				HashMap<String, AttributePartitionInfo> attrsSubspaceInfo = currSubInfo.getAttributesOfSubspace();
	    		
	    		Iterator<String> subspaceAttrIter = attrsSubspaceInfo.keySet().iterator();
				
	    		Vector<ProcessingQueryComponent> triggerStorageComp = new Vector<ProcessingQueryComponent>();
				while( subspaceAttrIter.hasNext() )
				{
					//double value = AttributeTypes.NOT_SET;
					
					String attrName = subspaceAttrIter.next();
					ProcessingQueryComponent qcomponent = null;
					if( currMatchingAttr.equals(attrName) )
					{
						qcomponent = new ProcessingQueryComponent( attrName, matchingQComp.getLowerBound(), 
								matchingQComp.getUpperBound() );
					}
					else
					{
						AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
						qcomponent = new ProcessingQueryComponent( attrName, attrMetaInfo.getDefaultValue(), 
								attrMetaInfo.getDefaultValue());
					}
					
					triggerStorageComp.add(qcomponent);
				}
				
				HashMap<Integer, OverlappingInfoClass> overlappingRegion = 
						this.hyperspaceDB.getOverlappingRegionsInSubspace(subspaceId, replicaNum, 
								triggerStorageComp);
				
				Iterator<Integer> overlapIter = overlappingRegion.keySet().iterator();
				
				while( overlapIter.hasNext() )
			    {
			    	Integer respNodeId = overlapIter.next();
			    	OverlappingInfoClass overlapInfo = overlappingRegion.get(respNodeId);
			    	
			    	QueryTriggerMessage<NodeIDType> queryTriggerMessage = 
							new QueryTriggerMessage<NodeIDType>
			    				(getMyID(), currReq.getRequestId(), currReq.getQuery(), 
			    						currReq.getGroupGUID(), subspaceId, 
			    						currReq.getUserIP(), currReq.getUserPort(), overlapInfo.hashCode);
			    	
					try
					{
						this.messenger.sendToID( (NodeIDType)respNodeId, queryTriggerMessage.toJSONObject() );
					} catch (IOException e)
					{
						e.printStackTrace();
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
					ContextServiceLogger.getLogger().info("Sending QueryMesgToSubspaceRegion mesg from " 
							+ getMyID() +" to node "+respNodeId);
			    }
    		}
    	}
	}
	
	private void processQueryMesgToSubspaceRegion(QueryMesgToSubspaceRegion<NodeIDType> queryMesgToSubspaceRegion)
	{
		this.numberOfQuerySubspaceRegion++;
		//long requestId 		= queryMesgToSubspaceRegion.getRequestId();
		String query 			= queryMesgToSubspaceRegion.getQuery();
		String groupGUID 		= queryMesgToSubspaceRegion.getGroupGUID();
		int subspaceId 			= queryMesgToSubspaceRegion.getSubspaceNum();
		//String userIP       	= queryMesgToSubspaceRegion.getUserIP();
		//int userPort        	= queryMesgToSubspaceRegion.getUserPort();
		//int hashCode        	= queryMesgToSubspaceRegion.getHashCode();
		JSONArray resultGUIDs = new JSONArray();
		int resultSize = this.hyperspaceDB.processSearchQueryInSubspaceRegion(subspaceId, query, resultGUIDs);
		
		QueryMesgToSubspaceRegionReply<NodeIDType> queryMesgToSubspaceRegionReply = 
				new QueryMesgToSubspaceRegionReply<NodeIDType>( getMyID(), queryMesgToSubspaceRegion.getRequestId(), 
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
				+ getMyID() +" to node "+queryMesgToSubspaceRegion.getSender());
		this.numberOfQuerySubspaceRegionReply++;
	}
	
	private void processQueryMesgToSubspaceRegionReply(QueryMesgToSubspaceRegionReply<NodeIDType> 
												queryMesgToSubspaceRegionReply)
	{
		NodeIDType senderID = queryMesgToSubspaceRegionReply.getSender();
		long requestId = queryMesgToSubspaceRegionReply.getRequestId();
		
		QueryInfo<NodeIDType> queryInfo = pendingQueryRequests.get(requestId);
		
		boolean allRepRecvd = 
				queryInfo.setRegionalReply((Integer)senderID, queryMesgToSubspaceRegionReply);
		
		//ContextServiceLogger.getLogger().fine("processQueryMesgToSubspaceRegionReply redId "+requestId);
		
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
			
			
			QueryMsgFromUserReply<NodeIDType> queryMsgFromUserReply = new QueryMsgFromUserReply<NodeIDType>(this.getMyID(),
					queryInfo.getQuery(), queryInfo.getGroupGUID(), concatResult, queryInfo.getUserReqID(), totalNumReplies);
			
			try
			{
				this.messenger.sendToAddress(new InetSocketAddress(queryInfo.getUserIP(), queryInfo.getUserPort()), 
						queryMsgFromUserReply.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			ContextServiceLogger.getLogger().info("Sending queryMsgFromUserReply mesg from " 
					+ getMyID() +" to node "+new InetSocketAddress(queryInfo.getUserIP(), queryInfo.getUserPort()));
			
			
			pendingQueryRequests.remove(requestId);
		}
	}
	
	private void processQueryTriggerMessage(QueryTriggerMessage<NodeIDType> queryTriggerMessage)
	{
		String query 		= queryTriggerMessage.getQuery();
		String groupGUID 	= queryTriggerMessage.getGroupGUID();
		int subspaceId 		= queryTriggerMessage.getSubspaceNum();
		String userIP       = queryTriggerMessage.getUserIP();
		int userPort        = queryTriggerMessage.getUserPort();
		int hashCode        = queryTriggerMessage.getHashCode();
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			this.hyperspaceDB.insertIntoSubspaceTriggerInfo(subspaceId, hashCode, query, groupGUID, userIP, userPort);
		}
	}
	
	private void processUpdateTriggerMessage(UpdateTriggerMessage<NodeIDType> updateTriggerMessage)
	{
		long requestID  = updateTriggerMessage.getRequestId();
		int subspaceId = updateTriggerMessage.getSubspaceNum();
		JSONObject oldValJSON = updateTriggerMessage.getOldUpdateValPair();
		JSONObject newUpdateVal = updateTriggerMessage.getNewUpdateValPair();
		
		//int oldNewVal = updateTriggerMessage.getOldNewVal();
		int hashCode  = updateTriggerMessage.getHashCode();
		
		JSONArray allGroups = this.hyperspaceDB.getTriggerInfo(subspaceId, hashCode);
		
		JSONArray toBeRemoved = new JSONArray();
		JSONArray toBeAdded = new JSONArray();
		
		// now check each group
		for( int i=0;i<allGroups.length();i++ )
		{
			JSONObject currGroup;
			try 
			{
				currGroup = allGroups.getJSONObject(i);
				String groupQuery = currGroup.getString("userQuery");
				// just creating a dummy queryinfo for parsing query
				QueryInfo<NodeIDType> qinfo = new QueryInfo<NodeIDType>(groupQuery);
				HashMap<String, ProcessingQueryComponent> pqcMap = qinfo.getProcessingQC();
				// first check if satisfied by old values
				Iterator<String> attrIter = pqcMap.keySet().iterator();
				boolean oldSatisfied = true;
				while( attrIter.hasNext() )
				{
					String attrName = attrIter.next();
					
					ProcessingQueryComponent qcomponent = pqcMap.get(attrName);
					
					boolean retValue = AttributeTypes.checkForComponent(qcomponent, oldValJSON);
					
					if(!retValue)
					{
						oldSatisfied = false;
						break;
					}	
				}
				
				boolean newSatisfied = true;
				
				attrIter = pqcMap.keySet().iterator();
				while( attrIter.hasNext() )
				{
					String attrName = attrIter.next();
					ProcessingQueryComponent qcomponent = pqcMap.get(attrName);
					
					String currrValue;
					if( newUpdateVal.has( qcomponent.getAttributeName() ) )
					{
						currrValue = newUpdateVal.getString( qcomponent.getAttributeName() );
					}
					else
					{
						currrValue = oldValJSON.getString( qcomponent.getAttributeName() );
					}
					JSONObject valueJSON = new JSONObject();
					valueJSON.put(qcomponent.getAttributeName(), currrValue);
					
					boolean retValue = AttributeTypes.checkForComponent(qcomponent, valueJSON);
					
					if(!retValue)
					{
						newSatisfied = false;
						break;
					}
				}
				
				// trigger needs to be snet when oldval is satisfied but not new value
				// or old not satisfied but new value is satisfied.
				
				if(oldSatisfied && !newSatisfied)
				{
					toBeRemoved.put(currGroup);
				}
				if(!oldSatisfied && newSatisfied)
				{
					toBeAdded.put(currGroup);
				}
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		ContextServiceLogger.getLogger().fine("toBeRemoved size "+toBeRemoved.length()
			+" toBeAdded size "+toBeAdded.length());
		
		UpdateTriggerReply<NodeIDType> updTriggerRep = 
				new UpdateTriggerReply<NodeIDType>( this.getMyID(), requestID, subspaceId, 
						toBeRemoved, toBeAdded);
		
		try
		{
			this.messenger.sendToID( updateTriggerMessage.getSender(), updTriggerRep.toJSONObject() );
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	private void processValueUpdateFromGNS( ValueUpdateFromGNS<NodeIDType> valueUpdateFromGNS )
	{
		String GUID 			  		= valueUpdateFromGNS.getGUID();
		NodeIDType respNodeId 	  		= this.getResponsibleNodeId(GUID);
		// long userRequesID 		  	= valueUpdateFromGNS.getUserRequestID();
		
		// just forward the request to the node that has 
		// guid stored in primary subspace.
		if( this.getMyID() != respNodeId )
		{
			ContextServiceLogger.getLogger().fine("not primary node case souceIp "+valueUpdateFromGNS.getSourceIP()
			+" sourcePort "+valueUpdateFromGNS.getSourcePort());
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
			ContextServiceLogger.getLogger().fine("primary node case souceIp "+valueUpdateFromGNS.getSourceIP()
			+" sourcePort "+valueUpdateFromGNS.getSourcePort());
			
			UpdateInfo<NodeIDType> updReq  	= null;
			long requestID 					= -1;
			// if no outstanding request then it is set to true
			boolean sendOutRequest 			= false;
			
			synchronized( this.pendingUpdateLock )
			{
				updReq = new UpdateInfo<NodeIDType>(valueUpdateFromGNS, updateIdCounter++);
				pendingUpdateRequests.put(updReq.getRequestId(), updReq);
				requestID = updReq.getRequestId();
				
				GUIDUpdateInfo<NodeIDType> guidUpdateInfo = this.guidUpdateInfoMap.get(GUID);
				
				if(guidUpdateInfo == null)
				{
					guidUpdateInfo = new GUIDUpdateInfo<NodeIDType>(GUID);
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
	 * this function processes a request serially.
	 * when one outstanding request completes.
	 */
	private void processUpdateSerially(UpdateInfo<NodeIDType> updateReq)
	{
		assert(updateReq != null);
		try
		{
			ContextServiceLogger.getLogger().fine("processUpdateSerially called "+updateReq.getRequestId() +" JSON"+updateReq.getValueUpdateFromGNS().toJSONObject().toString());
		}
		catch(JSONException jso)
		{
			jso.printStackTrace();
		}
		
		String GUID = updateReq.getValueUpdateFromGNS().getGUID();
		JSONObject attrValuePairs = updateReq.getValueUpdateFromGNS().getAttrValuePairs();
		long requestID = updateReq.getRequestId();
		
		// get the old value and process the update in primary subspace and other subspaces.
		String tableName = "primarySubspaceDataStorage";
		
		try
		{
			JSONObject oldValueJSON 	= this.hyperspaceDB.getGUIDRecordFromPrimarySubspace(GUID);
			int updateOrInsert 			= -1;
			
			if(oldValueJSON.length() == 0)
			{
				updateOrInsert = HyperspaceMySQLDB.INSERT_REC;
			}
			else
			{
				updateOrInsert = HyperspaceMySQLDB.UPDATE_REC;
			}
			
			try
			{
				// attributes which are not set should be set to default value
				// for subspace hashing
				if( oldValueJSON.length() != AttributeTypes.attributeMap.size() )
				{
					Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
					while(attrIter.hasNext())
					{
						String attrName = attrIter.next();
						AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
						if( !oldValueJSON.has(attrName) )
						{
							try
							{
								
								oldValueJSON.put(attrName, attrMetaInfo.getDefaultValue());
							} catch (JSONException e)
							{
								e.printStackTrace();
							}
						}
					}
				}
			} catch(Error | Exception ex)
			{
				ex.printStackTrace();
			}
			
			this.hyperspaceDB.storeGUIDInSubspace(tableName, GUID, attrValuePairs, updateOrInsert);
			//JSONObject oldValueJSON 	= this.hyperspaceDB.storeGUIDInSubspace(tableName, GUID, attrValuePairs);
			// process update at other subspaces.
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap
					= this.subspaceConfigurator.getSubspaceInfoMap();
			
			
			Iterator<Integer> keyIter   = subspaceInfoMap.keySet().iterator();
			//int maxMatchingAttrs 		= 0;
			//int maxMatchingSubspaceNum 	= -1;
			// subspaceNum to nodeId in that subspace mapping
			// HashMap<Integer, Integer> oldValueMapping = new HashMap<Integer, Integer>();
			// HashMap<Integer, Integer> newValueMapping = new HashMap<Integer, Integer>();
			NodeIDType oldRespNodeId = null, newRespNodeId = null;
			
			while( keyIter.hasNext() )
			{
				int subspaceId = keyIter.next();
				Vector<SubspaceInfo<NodeIDType>> replicasVect 
										= subspaceInfoMap.get(subspaceId);
				
				for( int i=0;i<replicasVect.size();i++ )
				{
					SubspaceInfo<NodeIDType> currSubInfo 
								= replicasVect.get(i);
					int replicaNum = currSubInfo.getReplicaNum();
					
					HashMap<String, AttributePartitionInfo> attrsSubspaceInfo = currSubInfo.getAttributesOfSubspace();
					
					//int currMaxMatch = 0;
					Vector<ProcessingQueryComponent> oldQueryComponents = new Vector<ProcessingQueryComponent>();
					
					Iterator<String> subspaceAttrIter = attrsSubspaceInfo.keySet().iterator();
					
					while( subspaceAttrIter.hasNext() )
					{
						String attrName = subspaceAttrIter.next();
						//( String attributeName, String leftOperator, double leftValue, 
						//		String rightOperator, double rightValue )
						ProcessingQueryComponent qcomponent = new ProcessingQueryComponent( attrName, oldValueJSON.getString(attrName), 
								oldValueJSON.getString(attrName) );
						
						oldQueryComponents.add(qcomponent);
					}
					
					HashMap<Integer, OverlappingInfoClass> overlappingRegion = 
								this.hyperspaceDB.getOverlappingRegionsInSubspace(subspaceId, replicaNum, oldQueryComponents);
					
					if( overlappingRegion.size() != 1 )
					{	
						assert(false);
					}
					else
					{
						//oldValueMapping.put(subspaceNum, overlappingRegion.keySet().iterator().next());
						oldRespNodeId = (NodeIDType)overlappingRegion.keySet().iterator().next();
					}
					
					// for new value
					Vector<ProcessingQueryComponent> newQueryComponents = new Vector<ProcessingQueryComponent>();
					Iterator<String> subspaceAttrIter1 = attrsSubspaceInfo.keySet().iterator();
					while( subspaceAttrIter1.hasNext() )
					{
						String attrName = subspaceAttrIter1.next();
						
						String value;
						if( attrValuePairs.has(attrName) )
						{
							value = attrValuePairs.getString(attrName);
						}
						else
						{
							value = oldValueJSON.getString(attrName);
						}
						ProcessingQueryComponent qcomponent = new ProcessingQueryComponent(attrName, value, value );
						newQueryComponents.add(qcomponent);
					}
					
					HashMap<Integer, OverlappingInfoClass> newOverlappingRegion = 
								this.hyperspaceDB.getOverlappingRegionsInSubspace(subspaceId, replicaNum, newQueryComponents);
					
					if( newOverlappingRegion.size() != 1 )
					{
						assert(false);
					}
					else
					{
						newRespNodeId = (NodeIDType)newOverlappingRegion.keySet().iterator().next();
					}
					
					ContextServiceLogger.getLogger().fine
						("oldNodeId "+oldRespNodeId+" newRespNodeId "+newRespNodeId);
					
					// send messages to the subspace region nodes
					if( oldRespNodeId == newRespNodeId )
					{
						// add entry for reply
						// 1 reply as both old and new goes to same node
						updateReq.initializeSubspaceEntry(subspaceId, replicaNum);
						
						ValueUpdateToSubspaceRegionMessage<NodeIDType>  valueUpdateToSubspaceRegionMessage 
							= new ValueUpdateToSubspaceRegionMessage<NodeIDType>(this.getMyID(), -1, GUID, attrValuePairs,
								ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY, subspaceId, requestID);
						
						try
						{
							this.messenger.sendToID
									(oldRespNodeId, valueUpdateToSubspaceRegionMessage.toJSONObject());
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
						// add entry for reply
						// 2 reply as both old and new goes to different node
						updateReq.initializeSubspaceEntry(subspaceId, replicaNum);
						
						ValueUpdateToSubspaceRegionMessage<NodeIDType>  oldValueUpdateToSubspaceRegionMessage 
							= new ValueUpdateToSubspaceRegionMessage<NodeIDType>(this.getMyID(), -1, GUID, attrValuePairs,
								ValueUpdateToSubspaceRegionMessage.REMOVE_ENTRY, subspaceId, requestID);
						
						try
						{
							this.messenger.sendToID(oldRespNodeId, oldValueUpdateToSubspaceRegionMessage.toJSONObject());
						} catch (IOException e)
						{
							e.printStackTrace();
						} catch (JSONException e)
						{
							e.printStackTrace();
						}
						
						ValueUpdateToSubspaceRegionMessage<NodeIDType>  newValueUpdateToSubspaceRegionMessage 
						 = new ValueUpdateToSubspaceRegionMessage<NodeIDType>(this.getMyID(), -1, GUID, attrValuePairs,
								ValueUpdateToSubspaceRegionMessage.ADD_ENTRY, subspaceId, requestID);
						
						try
						{
							this.messenger.sendToID(newRespNodeId, newValueUpdateToSubspaceRegionMessage.toJSONObject());
						} catch (IOException e)
						{
							e.printStackTrace();
						} catch (JSONException e)
						{
							e.printStackTrace();
						}
					}
					
					//getting group GUIDs that are affected
					//FIXME: check how triggers can be affected by replica of subspaces
					if( ContextServiceConfig.TRIGGER_ENABLED )
					{
						triggerProcessingOnUpdate( attrValuePairs, attrsSubspaceInfo, 
								subspaceId, replicaNum, oldValueJSON, requestID );
					}
				}
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}
	
	@SuppressWarnings("unchecked")
	private void triggerProcessingOnUpdate( JSONObject attrValuePairs, HashMap<String, AttributePartitionInfo> attrsSubspaceInfo, 
			int subspaceId, int replicaNum, JSONObject  oldValueJSON, long requestID ) throws JSONException
	{
		// update can be over multiple attributes
		Iterator<String> attrIter = attrValuePairs.keys();
		
		while( attrIter.hasNext() )
		{
			String currAttrName = attrIter.next();
			String currValue = attrValuePairs.getString(currAttrName);
			String oldValue  = oldValueJSON.getString(currAttrName);
			
			// current attribute is contained 
			// in the attribute subspace
			if( attrsSubspaceInfo.containsKey(currAttrName) )
			{
				Iterator<String> subspaceAttrIter = attrsSubspaceInfo.keySet().iterator();
				//find old overlapping groups
				Vector<ProcessingQueryComponent> oldTriggerComponents = new Vector<ProcessingQueryComponent>();
				
				Vector<ProcessingQueryComponent> newTriggerComponents = new Vector<ProcessingQueryComponent>();
				
				
				while( subspaceAttrIter.hasNext() )
				{
					String attrName = subspaceAttrIter.next();
					//( String attributeName, String leftOperator, double leftValue, 
					//		String rightOperator, double rightValue )
					
					ProcessingQueryComponent oldQcomponent = null;
					ProcessingQueryComponent newQcomponent = null;
					
					if( currAttrName.equals(attrName) )
					{
						oldQcomponent = new ProcessingQueryComponent( attrName, oldValue, oldValue );
						newQcomponent = new ProcessingQueryComponent( attrName, currValue, currValue );
					}
					else
					{
						AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
						
						oldQcomponent = new ProcessingQueryComponent( attrName, attrMetaInfo.getDefaultValue(), 
								attrMetaInfo.getDefaultValue() );
						
						newQcomponent = new ProcessingQueryComponent( attrName, attrMetaInfo.getDefaultValue(), 
								attrMetaInfo.getDefaultValue() );
					}
					
					oldTriggerComponents.add(oldQcomponent);
					newTriggerComponents.add(newQcomponent);
				}
				
				HashMap<Integer, OverlappingInfoClass> oldOverlappingRegion = 
							this.hyperspaceDB.getOverlappingRegionsInSubspace(subspaceId, replicaNum, oldTriggerComponents);
				
				if( oldOverlappingRegion.size() != 1 )
				{
					// it should fall in exactly one region/node
					assert(false);
				}
				else
				{
					Integer respNodeID = oldOverlappingRegion.keySet().iterator().next();
					OverlappingInfoClass overlapInfoObj = oldOverlappingRegion.get(respNodeID);
					// using JSONObject so that later on
					// values can be generalized to other datatypes
					// compared to double now.
					
					UpdateTriggerMessage<NodeIDType>  updateTriggerMessage 
					 = new UpdateTriggerMessage<NodeIDType>( this.getMyID(), requestID, subspaceId, 
							 oldValueJSON, attrValuePairs, UpdateTriggerMessage.OLD_VALUE, overlapInfoObj.hashCode);
					
					try
					{
						this.messenger.sendToID((NodeIDType) respNodeID, updateTriggerMessage.toJSONObject());
					} catch (IOException e)
					{
						e.printStackTrace();
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
				}
				// find new overlapping groups
				
				HashMap<Integer, OverlappingInfoClass> newOverlappingRegion = 
						this.hyperspaceDB.getOverlappingRegionsInSubspace(subspaceId, replicaNum, newTriggerComponents);
				
				
				if( newOverlappingRegion.size() != 1 )
				{
					assert(false);
				}
				else
				{
					Integer respNodeID = newOverlappingRegion.keySet().iterator().next();
					OverlappingInfoClass overlapInfoObj = newOverlappingRegion.get(respNodeID);
					// using JSONObject so that later on
					// values can be generalized to other datatypes
					// compared to double now.
					
					UpdateTriggerMessage<NodeIDType>  updateTriggerMessage 
					 = new UpdateTriggerMessage<NodeIDType>( this.getMyID(), requestID, subspaceId, 
							 oldValueJSON, attrValuePairs, UpdateTriggerMessage.NEW_VALUE, overlapInfoObj.hashCode);
					
					try
					{
						this.messenger.sendToID((NodeIDType) respNodeID, updateTriggerMessage.toJSONObject());
					} catch (IOException e)
					{
						e.printStackTrace();
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
				}
				
			}
		}
	}
	
	
	private void processValueUpdateToSubspaceRegionMessage(
			ValueUpdateToSubspaceRegionMessage<NodeIDType> valueUpdateToSubspaceRegionMessage)
	{
		int subspaceId = valueUpdateToSubspaceRegionMessage.getSubspaceNum();
		String GUID = valueUpdateToSubspaceRegionMessage.getGUID();
		JSONObject attrValuePairs = valueUpdateToSubspaceRegionMessage.getAttrValuePairs();
		int operType = valueUpdateToSubspaceRegionMessage.getOperType();
		int replicaNum = getTheReplicaNumForASubspace(subspaceId);
		
		String tableName 	= "subspaceId"+subspaceId+"DataStorage";
		try 
		{
			int numRep = 1;
			switch(operType)
			{
				case ValueUpdateToSubspaceRegionMessage.ADD_ENTRY:
				{
					numRep = 2;
					this.hyperspaceDB.storeGUIDInSubspace(tableName, GUID, attrValuePairs, HyperspaceMySQLDB.INSERT_REC);
					break;
				}
				case ValueUpdateToSubspaceRegionMessage.REMOVE_ENTRY:
				{
					numRep = 2;
					this.hyperspaceDB.deleteGUIDFromSubspaceRegion(tableName, GUID);
					break;
				}
				case ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY:
				{
					numRep = 1;
					this.hyperspaceDB.storeGUIDInSubspace(tableName, GUID, attrValuePairs, HyperspaceMySQLDB.UPDATE_REC);
					break;
				}
			}
			

			//ContextServiceLogger.getLogger().fine("Sending valueUpdateToSubspaceRegionReplyMessage to "
			//				+valueUpdateToSubspaceRegionMessage.getSender()+" from "+this.getMyID());
			
			ValueUpdateToSubspaceRegionReplyMessage<NodeIDType>  valueUpdateToSubspaceRegionReplyMessage 
				= new ValueUpdateToSubspaceRegionReplyMessage<NodeIDType>(this.getMyID(), 
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
		HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap
				= this.subspaceConfigurator.getSubspaceInfoMap();
		Vector<SubspaceInfo<NodeIDType>> replicasVect 
				= subspaceInfoMap.get(subpsaceId);
		
		int replicaNum = -1;
		for( int i=0;i<replicasVect.size();i++ )
		{
			SubspaceInfo<NodeIDType> subInfo = replicasVect.get(i);
			if( this.hyperspaceDB.checkIfSubspaceHasMyID(subInfo.getNodesOfSubspace()))
			{
				replicaNum = subInfo.getReplicaNum();
				break;
			}
		}
		return replicaNum;
	}
	
	private void processGetMessage(GetMessage<NodeIDType> getMessage)
	{
		String GUID 			  = getMessage.getGUIDsToGet();
		NodeIDType respNodeId 	  = this.getResponsibleNodeId(GUID);
		
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
			JSONObject valueJSON= this.hyperspaceDB.getGUIDRecordFromPrimarySubspace(GUID);
			
			
			GetReplyMessage<NodeIDType> getReplyMessage = new GetReplyMessage<NodeIDType>(this.getMyID(),
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
		(ValueUpdateToSubspaceRegionReplyMessage<NodeIDType> valueUpdateToSubspaceRegionReplyMessage)
	{
		long requestID = valueUpdateToSubspaceRegionReplyMessage.getRequestID();
		int subspaceId = valueUpdateToSubspaceRegionReplyMessage.getSubspaceNum();
		int numReply = valueUpdateToSubspaceRegionReplyMessage.getNumReply();
		int replicaNum = valueUpdateToSubspaceRegionReplyMessage.getReplicaNum();
		
		UpdateInfo<NodeIDType> updInfo = pendingUpdateRequests.get(requestID);
		boolean completion = updInfo.setUpdateReply(subspaceId, replicaNum, numReply);
		
		if( completion )
		{
			ValueUpdateFromGNSReply<NodeIDType> valueUpdateFromGNSReply = new ValueUpdateFromGNSReply<NodeIDType>
			(this.getMyID(), updInfo.getValueUpdateFromGNS().getVersionNum(), updInfo.getValueUpdateFromGNS().getUserRequestID());
			
			ContextServiceLogger.getLogger().fine("reply IP Port "+updInfo.getValueUpdateFromGNS().getSourceIP()
					+":"+updInfo.getValueUpdateFromGNS().getSourcePort()+ " ValueUpdateFromGNSReply for requestId "+requestID
					+" "+valueUpdateFromGNSReply);
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
			
			UpdateInfo<NodeIDType> removedUpdate = null;
			if( ContextServiceConfig.TRIGGER_ENABLED )
			{
				boolean triggerCompl = updInfo.checkAllTriggerRepRecvd();
				
				if( triggerCompl )
					removedUpdate = pendingUpdateRequests.remove(requestID);
			}
			else
			{
				removedUpdate = pendingUpdateRequests.remove(requestID);
			}
			
			// starts the queues serialized updates for that guid
			if(removedUpdate != null)
			{
				startANewUpdate(removedUpdate, requestID);
			}
		}
	}
	
	private void startANewUpdate(UpdateInfo<NodeIDType> removedUpdate, long requestID)
	{
		boolean startANewUpdate = false;
		Long nextRequestID = null;
		synchronized( this.pendingUpdateLock )
		{
			// remove from guidUpdateInfo
			GUIDUpdateInfo<NodeIDType> guidUpdateInfo = 
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
	
	private void processUpdateTriggerReply(
			UpdateTriggerReply<NodeIDType> updateTriggerReply) 
	{
		long requestID              	= updateTriggerReply.getRequestId();
		JSONArray toBeAddedGroups  		= updateTriggerReply.getToBeAddedGroups();
		JSONArray toBeRemovedGroups 	= updateTriggerReply.getToBeRemovedGroups();
		
		UpdateInfo<NodeIDType> updInfo  = pendingUpdateRequests.get(requestID);
		boolean triggerCompl = updInfo.setUpdateTriggerReply(toBeRemovedGroups, toBeAddedGroups);
		//boolean triggerCompl = updInfo.checkAllTriggerRepRecvd();
		
		if(triggerCompl)
		{
			HashMap<String, JSONObject> toBeRemovedGroupsMap = updInfo.getToBeRemovedGroups();
			HashMap<String, JSONObject> toBeAddedGroupsMap   = updInfo.getToBeAddedGroups();
			
			Iterator<String> iter = toBeRemovedGroupsMap.keySet().iterator();
			
			while( iter.hasNext() )
			{
				String groupGUID = iter.next();
				JSONObject groupInfo = toBeRemovedGroupsMap.get(groupGUID);
				
				try 
				{
					String queryString = groupInfo.getString(HyperspaceMySQLDB.userQuery);
					RefreshTrigger<NodeIDType> refTrig = new RefreshTrigger<NodeIDType>
					(this.getMyID(), queryString, groupGUID, updInfo.getValueUpdateFromGNS().getVersionNum(),
							updInfo.getValueUpdateFromGNS().getGUID(), RefreshTrigger.REMOVE);
					
					String userIP = groupInfo.getString(HyperspaceMySQLDB.userIP);
					int userPort  = groupInfo.getInt(HyperspaceMySQLDB.userPort);
					
					ContextServiceLogger.getLogger().fine("processUpdateTriggerReply removed grps queryString "
							+queryString+" userIP "+userIP+" userPort "+userPort);
					
					try
					{
						this.messenger.sendToAddress( new InetSocketAddress(userIP, userPort), 
								refTrig.toJSONObject() );
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
			
			iter = toBeAddedGroupsMap.keySet().iterator();
			
			while( iter.hasNext() )
			{
				String groupGUID = iter.next();
				JSONObject groupInfo = toBeAddedGroupsMap.get(groupGUID);
				
				try
				{
					String queryString = groupInfo.getString(HyperspaceMySQLDB.userQuery);
					
					RefreshTrigger<NodeIDType> refTrig = new RefreshTrigger<NodeIDType>
					(this.getMyID(), queryString, groupGUID, updInfo.getValueUpdateFromGNS().getVersionNum(),
							updInfo.getValueUpdateFromGNS().getGUID(), RefreshTrigger.ADD);
					
					String userIP = groupInfo.getString(HyperspaceMySQLDB.userIP);
					int userPort = groupInfo.getInt(HyperspaceMySQLDB.userPort);
					
					
					ContextServiceLogger.getLogger().fine("processUpdateTriggerReply added grps queryString "
							+queryString+" userIP "+userIP+" userPort "+userPort);
					
					try
					{
						this.messenger.sendToAddress( new InetSocketAddress(userIP, userPort), 
								refTrig.toJSONObject() );
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
			
			// removing here, because updInfo only gets removed 
			// when both trigger and update replies are recvd.
			boolean updateCompl = updInfo.checkAllUpdateReplyRecvd();
			UpdateInfo<NodeIDType> removedUpdate = null;
			
			if( updateCompl )
				removedUpdate = pendingUpdateRequests.remove(requestID);
			
			// starts the queues serialized updates for that guid
			// null is checked becuase it can also be remove on
			// update completion. So only one can start the new update
			if(removedUpdate != null)
			{
					startANewUpdate(removedUpdate, requestID);
			}
		}
	}
	
	private void processClientConfigRequest(ClientConfigRequest<NodeIDType> clientConfigRequest)
	{
		JSONArray nodeConfigArray 		= new JSONArray();
		JSONArray attributeArray  		= new JSONArray();
		
		Iterator<NodeIDType> nodeIDIter = this.allNodeIDs.iterator();
		
		while( nodeIDIter.hasNext() )
		{
			NodeIDType nodeId = nodeIDIter.next();
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
		
		InetSocketAddress sourceSocketAddr = new InetSocketAddress(clientConfigRequest.getSourceIP(),
				clientConfigRequest.getSourcePort());
		ClientConfigReply<NodeIDType> configReply 
					= new ClientConfigReply<NodeIDType>(this.getMyID(), nodeConfigArray,
							attributeArray);
		try 
		{
			this.messenger.sendToAddress(sourceSocketAddr, configReply.toJSONObject());
		} catch (IOException e) 
		{
			e.printStackTrace();
		} catch (JSONException e) 
		{
			e.printStackTrace();
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
		HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap 
			= this.subspaceConfigurator.getSubspaceInfoMap();
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
	
	
	/**
	 * returns subspacenum of all the subspaces a query overlaps with. 
	 * But returns only uniquer subspaces, not all the replicas of the overlapiing subspaces.
	 * @return
	 */
	private void getAllUniqueOverlappingSubspaces( HashMap<String, ProcessingQueryComponent> pqueryComponents, 
			HashMap<Integer, Vector<ProcessingQueryComponent>> overlappingSubspaces )
	{
		assert(pqueryComponents != null);
		assert(overlappingSubspaces != null);
		
		HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap 
			= this.subspaceConfigurator.getSubspaceInfoMap();
		
		Iterator<Integer> keyIter   	= subspaceInfoMap.keySet().iterator();
		
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
				if( attrsSubspaceInfo.containsKey(attrName) )
				{
					currMaxMatch = currMaxMatch + 1;
					currMatchingComponents.add(pqc);
				}
			}
			
			if( currMaxMatch > 0 )
			{
				overlappingSubspaces.put(subspaceId, currMatchingComponents);
			}
		}
	}
	
	private class MaxAttrMatchingStorageClass
	{
		public int subspaceId;
		public Vector<ProcessingQueryComponent> currMatchingComponents;
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
				switch(event.getType())
				{
					case  QUERY_MSG_FROM_USER:
					{
						//long t0 = System.currentTimeMillis();	
						@SuppressWarnings("unchecked")
						QueryMsgFromUser<NodeIDType> queryMsgFromUser 
												= (QueryMsgFromUser<NodeIDType>)event;
						
						processQueryMsgFromUser(queryMsgFromUser);
						
						//DelayProfiler.updateDelay("handleQueryMsgFromUser", t0);
						break;
					}
					case QUERY_MESG_TO_SUBSPACE_REGION:
					{
						//long t0 = System.currentTimeMillis();
						
						@SuppressWarnings("unchecked")
						QueryMesgToSubspaceRegion<NodeIDType> queryMesgToSubspaceRegion = 
								(QueryMesgToSubspaceRegion<NodeIDType>) event;
						
						log.fine("CS"+getMyID()+" received " + event.getType() + ": " + event);
						
						processQueryMesgToSubspaceRegion(queryMesgToSubspaceRegion);
						//processQueryMsgToMetadataNode(queryMsgToMetaNode);
						
						//DelayProfiler.updateDelay("handleQueryMsgToMetadataNode", t0);
						break;
					}
					case QUERY_MESG_TO_SUBSPACE_REGION_REPLY:
					{
						//long t0 = System.currentTimeMillis();
						@SuppressWarnings("unchecked")
						QueryMesgToSubspaceRegionReply<NodeIDType> queryMesgToSubspaceRegionReply = 
								(QueryMesgToSubspaceRegionReply<NodeIDType>)event;
						
						log.fine("CS"+getMyID()+" received " + event.getType() + ": " + queryMesgToSubspaceRegionReply);
						
						processQueryMesgToSubspaceRegionReply(queryMesgToSubspaceRegionReply);
						
						//DelayProfiler.updateDelay("handleQueryMsgToValuenode", t0);
						break;
					}
					case VALUE_UPDATE_MSG_FROM_GNS:
					{
						//long t0 = System.currentTimeMillis();
						@SuppressWarnings("unchecked")
						ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS = (ValueUpdateFromGNS<NodeIDType>)event;
						//log.info("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgFromGNS);
						ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgFromGNS);
						
						processValueUpdateFromGNS(valUpdMsgFromGNS);
						
						//DelayProfiler.updateDelay("handleValueUpdateFromGNS", t0);
						break;
					}
					case VALUEUPDATE_TO_SUBSPACE_REGION_MESSAGE:
					{
						/* Actions:
						 * - send the update message to the responsible value node
						 */
						//long t0 = System.currentTimeMillis();
						
						@SuppressWarnings("unchecked")
						ValueUpdateToSubspaceRegionMessage<NodeIDType> valueUpdateToSubspaceRegionMessage 
									= (ValueUpdateToSubspaceRegionMessage<NodeIDType>)event;
						//log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valueUpdateToSubspaceRegionMessage);
						ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " + valueUpdateToSubspaceRegionMessage);
						
						processValueUpdateToSubspaceRegionMessage(valueUpdateToSubspaceRegionMessage);
						//DelayProfiler.updateDelay("handleValueUpdateMsgToMetadataNode", t0);
						break;
					}
					
					case GET_MESSAGE:
					{
						@SuppressWarnings("unchecked")
						GetMessage<NodeIDType> getMessage 
									= (GetMessage<NodeIDType>)event;
						//log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valueUpdateToSubspaceRegionMessage);
						ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " 
										+ getMessage);
						
						processGetMessage(getMessage);
						break;
					}
					/*case GET_REPLY_MESSAGE:
					{
						@SuppressWarnings("unchecked")
						GetReplyMessage<NodeIDType> getReplyMessage 
									= (GetReplyMessage<NodeIDType>)event;
						//log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valueUpdateToSubspaceRegionMessage);
						ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " 
										+ getReplyMessage);
						processGetReplyMessage(getReplyMessage);
						break;
					}*/
					
					case VALUEUPDATE_TO_SUBSPACE_REGION_REPLY_MESSAGE:
					{
						@SuppressWarnings("unchecked")
						ValueUpdateToSubspaceRegionReplyMessage<NodeIDType> valueUpdateToSubspaceRegionReplyMessage 
									= (ValueUpdateToSubspaceRegionReplyMessage<NodeIDType>)event;
						//log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valueUpdateToSubspaceRegionMessage);
						ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " 
								+ valueUpdateToSubspaceRegionReplyMessage);
						processValueUpdateToSubspaceRegionMessageReply(valueUpdateToSubspaceRegionReplyMessage);
						break;
					}
					
					case QUERY_TRIGGER_MESSAGE:
					{
						@SuppressWarnings("unchecked")
						QueryTriggerMessage<NodeIDType> queryTriggerMessage 
									= (QueryTriggerMessage<NodeIDType>)event;
						//log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valueUpdateToSubspaceRegionMessage);
						ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " 
								+ queryTriggerMessage);
						processQueryTriggerMessage(queryTriggerMessage);
						break;
					}
					
					case UPDATE_TRIGGER_MESSAGE:
					{
						@SuppressWarnings("unchecked")
						UpdateTriggerMessage<NodeIDType> updateTriggerMessage 
									= (UpdateTriggerMessage<NodeIDType>)event;
						//log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valueUpdateToSubspaceRegionMessage);
						ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " 
								+ updateTriggerMessage);
						processUpdateTriggerMessage(updateTriggerMessage);
						break;
					}
					
					case UPDATE_TRIGGER_REPLY_MESSAGE:
					{
						@SuppressWarnings("unchecked")
						UpdateTriggerReply<NodeIDType> updateTriggerReply 
									= (UpdateTriggerReply<NodeIDType>)event;
						//log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valueUpdateToSubspaceRegionMessage);
						ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " 
								+ updateTriggerReply);
						processUpdateTriggerReply(updateTriggerReply);
						
						break;
					}
					
					case CONFIG_REQUEST:
					{
						@SuppressWarnings("unchecked")
						ClientConfigRequest<NodeIDType> configRequest 
									= (ClientConfigRequest<NodeIDType>)event;
						
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
			catch (Exception | Error ex)
			{
				ex.printStackTrace();
			}
		}
	}
	
	@Override
	public void checkQueryCompletion(QueryInfo<NodeIDType> qinfo) 
	{
	}
	
	private class ProfilerStatClass implements Runnable
	{
		private long localNumberOfQueryFromUser							= 0;
		private long localNumberOfQueryFromUserDepart					= 0;
		private long localNumberOfQuerySubspaceRegion					= 0;
		private long localNumberOfQuerySubspaceRegionReply				= 0;
		
		@Override
		public void run()
		{
			while(true)
			{
				try
				{
					Thread.sleep(10000);
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
				
				long diff1 = numberOfQueryFromUser - localNumberOfQueryFromUser;
				long diff2 = numberOfQueryFromUserDepart - localNumberOfQueryFromUserDepart;
				long diff3 = numberOfQuerySubspaceRegion - localNumberOfQuerySubspaceRegion;
				long diff4 = numberOfQuerySubspaceRegionReply - localNumberOfQuerySubspaceRegionReply;
				
				localNumberOfQueryFromUser							= numberOfQueryFromUser;
				localNumberOfQueryFromUserDepart					= numberOfQueryFromUserDepart;
				localNumberOfQuerySubspaceRegion					= numberOfQuerySubspaceRegion;
				localNumberOfQuerySubspaceRegionReply				= numberOfQuerySubspaceRegionReply;
				
				//ContextServiceLogger.getLogger().fine("QueryFromUserRate "+diff1+" QueryFromUserDepart "+diff2+" QuerySubspaceRegion "+diff3+
				//		" QuerySubspaceRegionReply "+diff4+
				//		" DelayProfiler stats "+DelayProfiler.getStats());
				
				//ContextServiceLogger.getLogger().fine( "Pending query requests "+pendingQueryRequests.size() );
				//ContextServiceLogger.getLogger().fine("DelayProfiler stats "+DelayProfiler.getStats());
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
		for(int j = 0; j<2; j++)
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
			ContextServiceLogger.getLogger().fine("hyperspaceDB.insertIntoSubspacePartitionInfo complete");
		}
	}
	
	/*@SuppressWarnings("unchecked")
	private void readSubspaceInfo() throws NumberFormatException, IOException
	{
		FileReader freader 	  = new FileReader(
				ContextServiceConfig.configFileDirectory+"/"+ContextServiceConfig.subspaceInfoFileName);
		BufferedReader reader = new BufferedReader( freader );
		String line 		  = null;
		
		while ( (line = reader.readLine()) != null )
		{
			String [] parsed = line.split(",");
			Integer subspaceNum = Integer.parseInt(parsed[0]);
			Vector<NodeIDType> subspaceNodes = new Vector<NodeIDType>();
			
			for(int i=1;i<parsed.length;i++)
			{
				subspaceNodes.add((NodeIDType)((Integer)Integer.parseInt(parsed[i])));
			}
			
			line = reader.readLine();
			parsed = line.split(",");
			Integer newSubspaceNum = Integer.parseInt(parsed[0]);
			
			HashMap<String, AttributePartitionInfo> subspaceAttrs = new HashMap<String, AttributePartitionInfo>();
			if(subspaceNum == newSubspaceNum)
			{
				for( int i=1;i<parsed.length;i++ )
				{
					String attrName = parsed[i].trim();
					assert(AttributeTypes.attributeMap.get(attrName) != null);
					AttributePartitionInfo attrPartInfo = new AttributePartitionInfo
							( AttributeTypes.attributeMap.get(attrName) );
					subspaceAttrs.put(attrName, attrPartInfo);
				}
			}
			else
			{
				assert(false);
			}	
			
			double numAttr  = subspaceAttrs.size();
			double numNodes = subspaceNodes.size();
			
			int numPartitions = (int)Math.ceil(Math.pow(numNodes, 1.0/numAttr));
			
			Iterator<String> subspaceAttrIter = subspaceAttrs.keySet().iterator();
			while( subspaceAttrIter.hasNext() )
			{
				String attrName = subspaceAttrIter.next();
				AttributePartitionInfo attrPartInfo = subspaceAttrs.get(attrName);
				attrPartInfo.initializePartitionInfo(numPartitions);
			}
			
			SubspaceInfo<NodeIDType> subspaceInfo = new 
					SubspaceInfo<NodeIDType>(subspaceNum, subspaceAttrs, subspaceNodes, numPartitions);
			this.subspaceInfoVector.put(subspaceInfo.getSubspaceNum(), subspaceInfo);
		}
		reader.close();
		freader.close();
	}*/
}