package edu.umass.cs.contextservice.schemes;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
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

import edu.umass.cs.contextservice.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.gns.GNSCalls;
import edu.umass.cs.contextservice.hyperspace.storage.DomainPartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ContextServicePacket.PacketType;
import edu.umass.cs.contextservice.messages.GetMessage;
import edu.umass.cs.contextservice.messages.GetReplyMessage;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegionReply;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgFromUserReply;
import edu.umass.cs.contextservice.messages.QueryMsgToValuenodeReply;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.processing.QueryComponent;
import edu.umass.cs.contextservice.processing.QueryInfo;
import edu.umass.cs.contextservice.processing.QueryParser;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.InterfaceNodeConfig;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.utils.DelayProfiler;

public class HyperspaceHashing<NodeIDType> extends AbstractScheme<NodeIDType>
{
	public static final int THREAD_POOL_SIZE					= 100;
	private final ExecutorService nodeES;
	
	private  HashMap<Integer, SubspaceInfo<NodeIDType>> subspaceInfoVector;
	private  HyperspaceMySQLDB<NodeIDType> hyperspaceDB 		= null;
	
	private long numberOfQueryFromUser							= 0;
	private long numberOfQueryFromUserDepart					= 0;
	private long numberOfQuerySubspaceRegion					= 0;
	private long numberOfQuerySubspaceRegionReply				= 0;
	
	public static final Logger log 								= ContextServiceLogger.getLogger();
	
	public HyperspaceHashing(InterfaceNodeConfig<NodeIDType> nc,
			JSONMessenger<NodeIDType> m)
	{
		super(nc, m);
		subspaceInfoVector = new HashMap<Integer, SubspaceInfo<NodeIDType>>();
		
		try
		{
			readSubspaceInfo();
		} catch (NumberFormatException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		}
		
		//System.out.println("readSubspaceInfo completed");
		try
		{
			hyperspaceDB = new HyperspaceMySQLDB<NodeIDType>(this.getMyID(), subspaceInfoVector);
		} catch (Exception e)
		{
			e.printStackTrace();
		}
		//System.out.println("HyperspaceMySQLDB completed");
		
		generateSubspacePartitions();
		nodeES = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		//System.out.println("generateSubspacePartitions completed");
		//nodeES = Executors.newCachedThreadPool();
		
		new Thread(new ProfilerStatClass()).start();
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryMsgFromUser(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
	{
		//System.out.println("handleQueryMsgFromUser");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateFromGNS(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		//System.out.println("handleValueUpdateFromGNS");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryMesgToSubspaceRegion(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		//System.out.println("handleQueryMesgToSubspaceRegion");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryMesgToSubspaceRegionReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
	{
		//System.out.println("handleQueryMesgToSubspaceRegionReply");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateToSubspaceRegionMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
	{
		//System.out.println("handleValueUpdateToSubspaceRegionMessage");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleGetMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		//System.out.println("handleValueUpdateToSubspaceRegionMessage");
		nodeES.submit(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleGetReplyMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		//System.out.println("handleValueUpdateToSubspaceRegionMessage");
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
	
	@SuppressWarnings("unchecked")
	private void readSubspaceInfo() throws NumberFormatException, IOException
	{
		FileReader freader 	  = new FileReader(ContextServiceConfig.NodeSubspaceInfo);
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
			
			HashMap<String, Boolean> subspaceAttrs = new HashMap<String, Boolean>();
			if(subspaceNum == newSubspaceNum)
			{
				for( int i=1;i<parsed.length;i++ )
				{
					//subspaceAttrs.add(parsed[i]);
					subspaceAttrs.put(parsed[i].trim(), true);
				}
			}
			else
			{
				assert(false);
			}
			
			Vector<DomainPartitionInfo> domainPartitionInfoVect = new Vector<DomainPartitionInfo>();
			double numAttr  = subspaceAttrs.size();
			double numNodes = subspaceNodes.size();
			
			double numPartitions = Math.ceil(Math.pow(numNodes, 1.0/numAttr));
			double numElemementsPerPartition = 
					Math.floor((AttributeTypes.MAX_VALUE - AttributeTypes.DEFAULT_VALUE)/numPartitions);
			
			double currLower = AttributeTypes.DEFAULT_VALUE;
			double currUpper = AttributeTypes.DEFAULT_VALUE;
			
			for(int j=0;j<numPartitions;j++)
			{
				currLower = currUpper;
				currUpper = currLower + numElemementsPerPartition;
				
				if(currUpper > AttributeTypes.MAX_VALUE)
					currUpper = AttributeTypes.MAX_VALUE;
				
				DomainPartitionInfo partitionInfo = new DomainPartitionInfo(j, currLower, currUpper);
				domainPartitionInfoVect.add(partitionInfo);
			}
			
			SubspaceInfo<NodeIDType> subspaceInfo = new 
					SubspaceInfo<NodeIDType>(subspaceNum, subspaceAttrs, subspaceNodes, domainPartitionInfoVect);
			this.subspaceInfoVector.put(subspaceInfo.getSubspaceNum(), subspaceInfo);
		}
		reader.close();
		freader.close();
	}
	
	/**
	 * recursive function to generate all the
	 * subspace regions/partitions.
	 */
	private void generateSubspacePartitions()
	{
		for(int i=0; i<this.subspaceInfoVector.size(); i++)
		{
			SubspaceInfo<NodeIDType> subspaceInfo = subspaceInfoVector.get(i);
			HashMap<String, Boolean> attrsOfSubspace = subspaceInfo.getAttributesOfSubspace();
			Vector<NodeIDType> nodesOfSubspace = subspaceInfo.getNodesOfSubspace();
			Vector<DomainPartitionInfo> domainPartitionInfo = subspaceInfo.getDomainPartitionInfo(); 
			
			double numAttr  = attrsOfSubspace.size();
			//double numNodes = nodesOfSubspace.size();
			
			Integer[] partitionNumArray = new Integer[domainPartitionInfo.size()];
			for(int j = 0; j<domainPartitionInfo.size(); j++)
			{
				partitionNumArray[j] = new Integer(domainPartitionInfo.get(j).getPartitionNum());
				//System.out.println("partitionNumArray[j] "+j+" "+partitionNumArray[j]);
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
				//System.out.println("perm.getVector() "+perm.getVector());
				hyperspaceDB.insertIntoSubspacePartitionInfo(subspaceInfo.getSubspaceNum(), perm.getVector(), respNodeId);
				//System.out.println("hyperspaceDB.insertIntoSubspacePartitionInfo complete");
				nodeIdCounter++;
			}
		}
	}
	
	@SuppressWarnings("unchecked")
	private void processQueryMsgFromUser(QueryMsgFromUser<NodeIDType> queryMsgFromUser)
	{
		this.numberOfQueryFromUser++;
		
		String query = queryMsgFromUser.getQuery();
		long userReqID = queryMsgFromUser.getUserReqNum();
		String userIP = queryMsgFromUser.getSourceIP();
		int userPort = queryMsgFromUser.getSourcePort();
		
		log.fine("QUERY RECVD: QUERY_MSG recvd query recvd "+query);
		
		// create the empty group in GNS
		String grpGUID = GNSCalls.createQueryGroup(query);
		if( grpGUID.length() <= 0 )
		{
			System.out.println("Query request failed at the recieving node "+queryMsgFromUser);
			return;
		}
		
		Vector<QueryComponent> qcomponents = QueryParser.parseQuery(query);
		
		Vector<QueryComponent> matchingQueryComponents = new Vector<QueryComponent>();
		int maxMatchingSubspaceNum = getMaxOverlapSubspace(qcomponents, matchingQueryComponents);
		
		System.out.println("userReqID "+userReqID+" maxMatchingSubspaceNum "+maxMatchingSubspaceNum+" matchingQueryComponents "
				+matchingQueryComponents.size()+" query "+query);
		
		// get number of nodes/or regions to send to in that subspace.
	    HashMap<Integer, JSONArray> respNodeIdList = this.hyperspaceDB.getOverlappingRegionsInSubspace
	    		(maxMatchingSubspaceNum, matchingQueryComponents);
	    
	    // query is conflicting, like same attribute has conflicting ranges
	    // in conjunction. 1 <= contextATT0 <= 5 && 10 <= contextATT0 <= 15,
	    // in current query patterns this query can be generated.
	    if(respNodeIdList.size() == 0)
	    {
	    	QueryMsgFromUserReply<NodeIDType> queryMsgFromUserReply = new QueryMsgFromUserReply<NodeIDType>(this.getMyID(),
					query, grpGUID, new JSONArray(), userReqID, 0);
	    	
			try
			{
				this.messenger.sendToAddress(new InetSocketAddress(userIP, userPort), 
						queryMsgFromUserReply.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			return;
	    }
	    
	    
		QueryInfo<NodeIDType> currReq  
			= new QueryInfo<NodeIDType>(query, getMyID(), grpGUID, userReqID, userIP, userPort, qcomponents);
		
		synchronized(this.pendingQueryLock)
		{
			currReq.setQueryRequestID(queryIdCounter++);
		}
		
		pendingQueryRequests.put(currReq.getRequestId(), currReq);
		
		log.fine("processQueryMsgFromUser respNodeIdList size "+respNodeIdList.size()+
	    		" requestId "+currReq.getRequestId() +" maxMatchingSubspaceNum "+maxMatchingSubspaceNum);
		
	    currReq.initializeRegionalReplies(respNodeIdList);
		
	    Iterator<Integer> respNodeIdIter = respNodeIdList.keySet().iterator();
	    
	    while( respNodeIdIter.hasNext() )
	    {
	    	Integer respNodeId = respNodeIdIter.next();
	    	
	    	QueryMesgToSubspaceRegion<NodeIDType> queryMesgToSubspaceRegion = 
					new QueryMesgToSubspaceRegion<NodeIDType>(getMyID(), currReq.getRequestId(), query, grpGUID, maxMatchingSubspaceNum);
			
			try
			{
				this.messenger.sendToID((NodeIDType)respNodeId, queryMesgToSubspaceRegion.toJSONObject());
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
	    this.numberOfQueryFromUserDepart++;
	}
	
	private void processQueryMesgToSubspaceRegion(QueryMesgToSubspaceRegion<NodeIDType> queryMesgToSubspaceRegion)
	{
		this.numberOfQuerySubspaceRegion++;
		long requestId 		= queryMesgToSubspaceRegion.getRequestId();
		String query 		= queryMesgToSubspaceRegion.getQuery();
		String groupGUID 	= queryMesgToSubspaceRegion.getGroupGUID();
		int subspaceNum 	= queryMesgToSubspaceRegion.getSubspaceNum();
		
		JSONArray resultGUIDs = this.hyperspaceDB.processSearchQueryInSubspaceRegion(subspaceNum, query);
		
		QueryMesgToSubspaceRegionReply<NodeIDType> queryMesgToSubspaceRegionReply = 
				new QueryMesgToSubspaceRegionReply<NodeIDType>(getMyID(), queryMesgToSubspaceRegion.getRequestId(), 
						groupGUID, resultGUIDs);
		
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
		
		System.out.println("processQueryMesgToSubspaceRegionReply redId "+requestId);
		
		if(allRepRecvd)
		{
			JSONArray concatResult 	= new JSONArray();
			int totalNumReplies 	= 0;
			
			if(ContextServiceConfig.sendFullReplies)
			{
				HashMap<Integer, JSONArray> repliesHashMap = queryInfo.getRepliesHashMap();
				
				Iterator<Integer> nodeIdIter = repliesHashMap.keySet().iterator();
				
				
				while(nodeIdIter.hasNext())
				{
					JSONArray currArray = repliesHashMap.get(nodeIdIter.next());
					concatResult.put(currArray);
				}
			}
			else
			{
				HashMap<Integer, Integer> repliesSizeHashMap = queryInfo.getRepliesSizeHashMap();
				
				Iterator<Integer> nodeIdIter = repliesSizeHashMap.keySet().iterator();	
				
				while( nodeIdIter.hasNext() )
				{
					int currRepSize = repliesSizeHashMap.get(nodeIdIter.next());
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
	
	@SuppressWarnings("unchecked")
	private void processValueUpdateFromGNS(ValueUpdateFromGNS<NodeIDType> valueUpdateFromGNS)
	{
		String GUID 			  = valueUpdateFromGNS.getGUID();
		NodeIDType respNodeId 	  = this.getResponsibleNodeId(GUID);
		JSONObject attrValuePairs = valueUpdateFromGNS.getAttrValuePairs();
		
		// just forward the request to the node that has 
		// guid stored in primary subspace.
		if( this.getMyID() != respNodeId )
		{
			try
			{
				this.messenger.sendToID(respNodeId, valueUpdateFromGNS.toJSONObject());
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
			// get the old value and process the update in primary subspace and other subspaces.
			String tableName = "primarySubspaceDataStorage";
			try
			{
				JSONObject oldValueJSON 	= this.hyperspaceDB.storeGUIDInSubspace(tableName, GUID, attrValuePairs);
				// process update at other subspaces.
				
				
				Iterator<Integer> keyIter   = this.subspaceInfoVector.keySet().iterator();
				//int maxMatchingAttrs 		= 0;
				//int maxMatchingSubspaceNum 	= -1;
				// subspaceNum to nodeId in that subspace mapping
				//HashMap<Integer, Integer> oldValueMapping = new HashMap<Integer, Integer>();
				//HashMap<Integer, Integer> newValueMapping = new HashMap<Integer, Integer>();
				NodeIDType oldRespNodeId = null, newRespNodeId = null;
				
				while( keyIter.hasNext() )
				{
					int subspaceNum = keyIter.next();
					SubspaceInfo<NodeIDType> currSubInfo = this.subspaceInfoVector.get(subspaceNum);
					HashMap<String, Boolean> attrsSubspaceInfo = currSubInfo.getAttributesOfSubspace();
					
					//int currMaxMatch = 0;
					Vector<QueryComponent> oldQueryComponents = new Vector<QueryComponent>();
					
					Iterator<String> subspaceAttrIter = attrsSubspaceInfo.keySet().iterator();
					
					while( subspaceAttrIter.hasNext() )
					{
						String attrName = subspaceAttrIter.next();
						//( String attributeName, String leftOperator, double leftValue, 
						//		String rightOperator, double rightValue )
						QueryComponent qcomponent = new QueryComponent(attrName, "<=", oldValueJSON.getDouble(attrName), "<=", 
								oldValueJSON.getDouble(attrName) );
						
						oldQueryComponents.add(qcomponent);
					}
					HashMap<Integer, JSONArray> overlappingRegion = 
								this.hyperspaceDB.getOverlappingRegionsInSubspace(subspaceNum, oldQueryComponents);
					
					if(overlappingRegion.size() != 1)
					{
						assert(false);
					}
					else
					{
						//oldValueMapping.put(subspaceNum, overlappingRegion.keySet().iterator().next());
						oldRespNodeId = (NodeIDType)overlappingRegion.keySet().iterator().next();
					}
					
					
					// for new value
					Vector<QueryComponent> newQueryComponents = new Vector<QueryComponent>();
					Iterator<String> subspaceAttrIter1 = attrsSubspaceInfo.keySet().iterator();
					while( subspaceAttrIter1.hasNext() )
					{
						String attrName = subspaceAttrIter1.next();
						//( String attributeName, String leftOperator, double leftValue, 
						//		String rightOperator, double rightValue )
						double value;
						if( attrValuePairs.has(attrName) )
						{
							value = attrValuePairs.getDouble(attrName);
						}
						else
						{
							value = oldValueJSON.getDouble(attrName);
						}
						QueryComponent qcomponent = new QueryComponent(attrName, "<=", value, "<=", 
								value );
						
						newQueryComponents.add(qcomponent);
					}
					HashMap<Integer, JSONArray> newOverlappingRegion = 
								this.hyperspaceDB.getOverlappingRegionsInSubspace(subspaceNum, newQueryComponents);
					
					if( newOverlappingRegion.size() != 1 )
					{
						assert(false);
					}
					else
					{
						//newValueMapping.put(subspaceNum, newOverlappingRegion.keySet().iterator().next());
						newRespNodeId = (NodeIDType)newOverlappingRegion.keySet().iterator().next();
					}
					
					// send messages to the subspace region nodes
					if( oldRespNodeId == newRespNodeId )
					{
						ValueUpdateToSubspaceRegionMessage<NodeIDType>  valueUpdateToSubspaceRegionMessage 
						= new ValueUpdateToSubspaceRegionMessage<NodeIDType>(this.getMyID(), -1, GUID, attrValuePairs,
								ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY, subspaceNum);
						
						try
						{
							this.messenger.sendToID(oldRespNodeId, valueUpdateToSubspaceRegionMessage.toJSONObject());
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
						ValueUpdateToSubspaceRegionMessage<NodeIDType>  oldValueUpdateToSubspaceRegionMessage 
						= new ValueUpdateToSubspaceRegionMessage<NodeIDType>(this.getMyID(), -1, GUID, attrValuePairs,
								ValueUpdateToSubspaceRegionMessage.REMOVE_ENTRY, subspaceNum);
						
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
								ValueUpdateToSubspaceRegionMessage.ADD_ENTRY, subspaceNum);
						
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
				}
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}
	}
	
	private void processValueUpdateToSubspaceRegionMessage(
			ValueUpdateToSubspaceRegionMessage<NodeIDType> valueUpdateToSubspaceRegionMessage)
	{
		int subspaceNum = valueUpdateToSubspaceRegionMessage.getSubspaceNum();
		String GUID = valueUpdateToSubspaceRegionMessage.getGUID();
		JSONObject attrValuePairs = valueUpdateToSubspaceRegionMessage.getAttrValuePairs();
		int operType = valueUpdateToSubspaceRegionMessage.getOperType();
		
		String tableName 	= "subspace"+subspaceNum+"DataStorage";
		try 
		{
			switch(operType)
			{
				case ValueUpdateToSubspaceRegionMessage.ADD_ENTRY:
				{
					this.hyperspaceDB.storeGUIDInSubspace(tableName, GUID, attrValuePairs);
					break;
				}
				case ValueUpdateToSubspaceRegionMessage.REMOVE_ENTRY:
				{
					this.hyperspaceDB.deleteGUIDFromSubspaceRegion(tableName, GUID);
					break;
				}
				case ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY:
				{
					this.hyperspaceDB.storeGUIDInSubspace(tableName, GUID, attrValuePairs);
					break;
				}
			}
		} catch (JSONException e) 
		{
			e.printStackTrace();
		}
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
			String tableName = "primarySubspaceDataStorage";
			JSONObject valueJSON= this.hyperspaceDB.getGUIDRecordFromPrimarySubspace(tableName, GUID);
			
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
	
	/**
	 * returns subspace number of the maximum overlapping
	 * subspace. Used in processing search query.
	 * @return
	 */
	private int getMaxOverlapSubspace( Vector<QueryComponent> queryComponents, 
			Vector<QueryComponent> matchingAttributes )
	{
		Iterator<Integer> keyIter   	= this.subspaceInfoVector.keySet().iterator();
		int maxMatchingAttrs 			= 0;
		//int maxMatchingSubspaceNum 	= -1;
		
		HashMap<Integer, Vector<MaxAttrMatchingStorageClass>> matchingSubspaceHashMap = 
				new HashMap<Integer, Vector<MaxAttrMatchingStorageClass>>();
		
		//Vector<MaxAttrMatchingStorageClass> maxMatchingSubspaceNumVector 
		//											= new Vector<MaxAttrMatchingStorageClass>();
		
		while( keyIter.hasNext() )
		{
			int subspaceNum = keyIter.next();
			SubspaceInfo<NodeIDType> currSubInfo = this.subspaceInfoVector.get(subspaceNum);
			HashMap<String, Boolean> attrsSubspaceInfo = currSubInfo.getAttributesOfSubspace();
			
			int currMaxMatch = 0;
			Vector<QueryComponent> currMatchingComponents = new Vector<QueryComponent>();
			
			for( int i=0; i<queryComponents.size(); i++ )
			{
				QueryComponent qc = queryComponents.get(i);
				if( attrsSubspaceInfo.containsKey(qc.getAttributeName()) )
				{
					currMaxMatch = currMaxMatch + 1;
					currMatchingComponents.add(qc);
				}
			}
			
			if(currMaxMatch >= maxMatchingAttrs)
			{
				maxMatchingAttrs = currMaxMatch;
				MaxAttrMatchingStorageClass maxAttrMatchObj = new MaxAttrMatchingStorageClass();
				maxAttrMatchObj.currMatchingComponents = currMatchingComponents;
				maxAttrMatchObj.maxAttrMatching = currMaxMatch;
				maxAttrMatchObj.subspaceNum = subspaceNum;
				
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
				
				//maxMatchingSubspaceNumVector.add(maxAttrMatchObj);
				//maxMatchingSubspaceNum = subspaceNum;
				//matchingAttributes.clear();
				//matchingAttributes.addAll(currMatchingComponents);
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
			print = print + maxMatchingSubspaceNumVector.get(i).subspaceNum+" ";
		}
		print = print + " chosen "+maxMatchingSubspaceNumVector.get(returnIndex).subspaceNum;
		System.out.println(print);
		
		return maxMatchingSubspaceNumVector.get(returnIndex).subspaceNum;
	}
	
	private class MaxAttrMatchingStorageClass
	{
		public int maxAttrMatching;
		public int subspaceNum;
		public Vector<QueryComponent> currMatchingComponents;
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
				}
				case VALUE_UPDATE_MSG_FROM_GNS:
				{
					//long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS = (ValueUpdateFromGNS<NodeIDType>)event;
					//log.info("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgFromGNS);
					System.out.println("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgFromGNS);
					
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
					System.out.println("CS"+getMyID()+" received " + event.getType() + ": " + valueUpdateToSubspaceRegionMessage);
					
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
					System.out.println("CS"+getMyID()+" received " + event.getType() + ": " 
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
					System.out.println("CS"+getMyID()+" received " + event.getType() + ": " 
									+ getReplyMessage);
					processGetReplyMessage(getReplyMessage);
					break;
				}*/
			}
		}
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleMetadataMsgToValuenode(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		return null;
	}
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryMsgToMetadataNode(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryMsgToValuenode(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
	{
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryMsgToValuenodeReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateMsgToMetadataNode(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateMsgToValuenode(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateMsgToValuenodeReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleBulkGet(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleBulkGetReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleConsistentStoragePut(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleConsistentStoragePutReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public void checkQueryCompletion(QueryInfo<NodeIDType> qinfo) 
	{
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] initializeScheme() 
	{
		return null;
	}

	@Override
	protected void processReplyInternally(
			QueryMsgToValuenodeReply<NodeIDType> queryMsgToValnodeRep,
			QueryInfo<NodeIDType> queryInfo) 
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
					Thread.sleep(1000);
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
				
				System.out.println("QueryFromUserRate "+diff1+" QueryFromUserDepart "+diff2+" QuerySubspaceRegion "+diff3+
						" QuerySubspaceRegionReply "+diff4+
						" DelayProfiler stats "+DelayProfiler.getStats());
				//System.out.println( "Pending query requests "+pendingQueryRequests.size() );
				//System.out.println("DelayProfiler stats "+DelayProfiler.getStats());
			}
		}
	}
	
	public static void main(String[] args)
	{
		double numPartitions = Math.ceil(Math.pow(16, 1.0/4));
		System.out.println("numPartitions "+numPartitions);
		
		double numAttr  = 5;
		//double numNodes = nodesOfSubspace.size();
		
		Integer[] partitionNumArray = new Integer[2];
		for(int j = 0; j<2; j++)
		{
			partitionNumArray[j] = j;
			System.out.println("partitionNumArray[j] "+j+" "+partitionNumArray[j]);
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
			System.out.println("perm.getVector() "+perm.getVector());
			System.out.println("hyperspaceDB.insertIntoSubspacePartitionInfo complete");
		}
	}
}