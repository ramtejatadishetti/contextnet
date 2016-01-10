package edu.umass.cs.contextservice.schemes.old;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.hash.Hashing;

import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.SQLContextServiceDB;
import edu.umass.cs.contextservice.database.records.AttributeMetaObjectRecord;
import edu.umass.cs.contextservice.database.records.AttributeMetadataInfoRecord;
import edu.umass.cs.contextservice.database.records.GroupGUIDRecord;
import edu.umass.cs.contextservice.database.records.MetadataTableInfo;
import edu.umass.cs.contextservice.database.records.NodeGUIDInfoRecord;
import edu.umass.cs.contextservice.database.records.ValueInfoObjectRecord;
import edu.umass.cs.contextservice.database.records.ValueTableInfo;
import edu.umass.cs.contextservice.gns.GNSCalls;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.BulkGet;
import edu.umass.cs.contextservice.messages.BulkGetReply;
import edu.umass.cs.contextservice.messages.ConsistentStoragePut;
import edu.umass.cs.contextservice.messages.ConsistentStoragePutReply;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.EchoMessage;
import edu.umass.cs.contextservice.messages.EchoReplyMessage;
import edu.umass.cs.contextservice.messages.MetadataMsgToValuenode;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgToMetadataNode;
import edu.umass.cs.contextservice.messages.QueryMsgToValuenode;
import edu.umass.cs.contextservice.messages.QueryMsgToValuenodeReply;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateMsgToMetadataNode;
import edu.umass.cs.contextservice.messages.ValueUpdateMsgToValuenode;
//import edu.umass.cs.contextservice.messages.ValueUpdateMsgToValuenodeReply;
import edu.umass.cs.contextservice.messages.ContextServicePacket.PacketType;
import edu.umass.cs.contextservice.queryparsing.QueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.queryparsing.QueryParser;
import edu.umass.cs.contextservice.queryparsing.RecordReadStorage;
import edu.umass.cs.contextservice.schemes.AbstractScheme;
//import edu.umass.cs.contextservice.processing.UpdateInfo;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.InterfaceNodeConfig;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.utils.DelayProfiler;

public class MercuryMySQLInstrumented<NodeIDType> extends AbstractScheme<NodeIDType>
{
	public static final Logger log = ContextServiceLogger.getLogger();
	
	public static final int THREAD_POOL_SIZE													= 500;
	// we don't want to do any computation in handleEvent method threads.
	private final ExecutorService nodeES;
	
	private ConcurrentHashMap<Long, RecordReadStorage<NodeIDType>> pendingRecordReadReqs;
	
	private long readRecordStorageNum															= 0;
	private final Object readRecordMonitor														= new Object();
	
	
	private SQLContextServiceDB<NodeIDType> sqlDBObject 										= null;
	
	//FIXME: sourceID is not properly set, it is currently set to sourceID of each node,
	// it needs to be set to the origin sourceID.
	// Any id-based communication requires NodeConfig and Messenger
	public MercuryMySQLInstrumented(InterfaceNodeConfig<NodeIDType> nc, JSONMessenger<NodeIDType> m)
	{
		super(nc, m);
		
		pendingRecordReadReqs = new ConcurrentHashMap<Long, RecordReadStorage<NodeIDType>>();
		
		nodeES = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		new Thread(new ProfilerStatClass()).start();
		
		try
		{
			sqlDBObject = new SQLContextServiceDB<NodeIDType>(this.getMyID());
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleMetadataMsgToValuenode(
		ProtocolEvent<ContextServicePacket.PacketType, String> event,
		ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		long t0 = System.currentTimeMillis();
		
		@SuppressWarnings("unchecked")
		MetadataMsgToValuenode<NodeIDType> metaMsgToValnode = (MetadataMsgToValuenode<NodeIDType>) event;
		// just need to store the val node info in the local storage
		
		String attrName = metaMsgToValnode.getAttrName();
		double rangeStart = metaMsgToValnode.getRangeStart();
		double rangeEnd = metaMsgToValnode.getRangeEnd();
		
		ContextServiceLogger.getLogger().info("METADATA_MSG recvd at node " + 
				this.getMyID()+" attriName "+attrName + 
				" rangeStart "+rangeStart+" rangeEnd "+rangeEnd);
		
		//AttributeValueInformation<NodeIDType> attrValueInfo = 
		//		new AttributeValueInformation<NodeIDType>(attrName, rangeStart, rangeEnd);
		//this.addValueList(attrValueInfo);
		
		if(!ContextServiceConfig.USESQL)
		{
			ValueInfoObjectRecord<Double> valInfoObjRec = new ValueInfoObjectRecord<Double>
												(rangeStart, rangeEnd, new JSONArray());
			
			this.contextserviceDB.putValueObjectRecord(valInfoObjRec, attrName);
		}
		else
		{	
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleMetadataMsgToValuenode", t0);
		
		return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleQueryMsgFromUser(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleQueryMsgToMetadataNode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleQueryMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleQueryMsgToValuenodeReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleValueUpdateFromGNS(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleValueUpdateMsgToMetadataNode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleValueUpdateMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleValueUpdateMsgToValuenodeReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleBulkGet(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleBulkGetReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleConsistentStoragePut(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleConsistentStoragePutReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) 
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	/**
	 * Takes the attribute name as input and returns the node id 
	 * that is responsible for metadata of that attribute.
	 * @param AttrName
	 * @return
	 */
	public NodeIDType getResponsibleNodeId(String AttrName)
	{
		long t0 = System.currentTimeMillis();
		
		int numNodes = this.allNodeIDs.size();
		
		//String attributeHash = Utils.getSHA1(attributeName);
		int mapIndex = Hashing.consistentHash(AttrName.hashCode(), numNodes);
		@SuppressWarnings("unchecked")
		NodeIDType[] allNodeIDArr = (NodeIDType[]) this.allNodeIDs.toArray();
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("getResponsibleNodeId", t0);
		
		return allNodeIDArr[mapIndex];
	}
	
	@SuppressWarnings("unchecked")
	public GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] initializeScheme()
	{
		return null;
		/*long t0 = System.currentTimeMillis();
		
		log.fine("\n\n\n" +
				"In initializeMetadataObjects NodeId "+getMyID()+"\n\n\n");
		
		LinkedList<GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>> messageList = 
				new  LinkedList<GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>>();
		
		Vector<String> attributes = AttributeTypes.getAllAttributes();
		for(int i=0;i<attributes.size(); i++)
		{
			String currAttName = attributes.get(i);
			log.fine("initializeMetadataObjects currAttName "+currAttName);
			//String attributeHash = Utils.getSHA1(attributeName);
			NodeIDType respNodeId = getResponsibleNodeId(currAttName);
			log.fine("InitializeMetadataObjects currAttName "+currAttName
					+" respNodeID "+respNodeId);
			// This node is responsible(meta data)for this Att.
			if(respNodeId == getMyID() )
			{
				ContextServiceLogger.getLogger().info("Node Id "+getMyID() +
						" meta data node for attribute "+currAttName);
				// FIXME: set proper min max value, probably read attribute names and its min max value from file.
				//AttributeMetadataInformation<NodeIDType> attrMeta = 
				//		new AttributeMetadataInformation<NodeIDType>(currAttName, AttributeTypes.MIN_VALUE, 
				//				AttributeTypes.MAX_VALUE, csNode);
				
				if(!ContextServiceConfig.USESQL)
				{
					AttributeMetadataInfoRecord<NodeIDType, Double> attrMetaRec =
						new AttributeMetadataInfoRecord<NodeIDType, Double>
					(currAttName, AttributeTypes.MIN_VALUE, AttributeTypes.MAX_VALUE);
				
					getContextServiceDB().putAttributeMetaInfoRecord(attrMetaRec);
				}
				
				//csNode.addMetadataInfoRec(attrMetaRec);
				//;addMetadataList(attrMeta);
				//GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] messageTasks = 
				//		attrMeta.assignValueRanges(csNode.getMyID());
				
				GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] messageTasks 
						= assignValueRanges(getMyID(), currAttName, AttributeTypes.MIN_VALUE, AttributeTypes.MAX_VALUE);
				
				// add all the messaging tasks at different value nodes
				for(int j=0;j<messageTasks.length;j++)
				{
					messageList.add(messageTasks[j]);
				}
			}
		}
		GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] returnArr 
					= new GenericMessagingTask[messageList.size()];
		
		for(int i=0;i<messageList.size();i++)
		{
			returnArr[i] = messageList.get(i);
		}
		
		log.fine("\n\n csNode.getMyID() "+getMyID()+
				" returnArr size "+returnArr.length +" messageList.size() "+messageList.size()+"\n\n");
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("initializeScheme", t0);
		
		return returnArr;*/
	}
	
	/****************************** End of protocol task handler methods *********************/
	/*********************** Private methods below **************************/
	/**
	 * Query req received here means that
	 * no group exists in the GNS
	 * @param queryMsgFromUser
	 * @return
	 */
	private void processQueryMsgFromUser(QueryMsgFromUser<NodeIDType> queryMsgFromUser)
	{
		long t0 = System.currentTimeMillis();
		
		String query = queryMsgFromUser.getQuery();
		long userReqID = queryMsgFromUser.getUserReqNum();
		String userIP = queryMsgFromUser.getSourceIP();
		int userPort = queryMsgFromUser.getSourcePort();
		
		//ContextServiceLogger.getLogger().fine("QUERY RECVD: QUERY_MSG recvd query recvd "+query);
		
		// create the empty group in GNS
		String grpGUID = GNSCalls.createQueryGroup(query);
		if( grpGUID.length() <= 0 )
		{
			//ContextServiceLogger.getLogger().fine("Query request failed at the recieving node "+queryMsgFromUser);
			return;
		}
		
		// adding user to the notification set
		//GNSCalls.updateNotificationSetOfAGroup(new InetSocketAddress(userIP, userPort), query);
		
		Vector<QueryComponent> qcomponents = QueryParser.parseQuery(query);
		QueryInfo<NodeIDType> currReq  
			= new QueryInfo<NodeIDType>(query, getMyID(), grpGUID, userReqID, userIP, userPort, qcomponents);
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgFromUser:Parsing", t0);
		
		long t1 = System.currentTimeMillis();
		
		/*synchronized(this.pendingQueryLock)
		{
			currReq.setQueryRequestID(queryIdCounter++);
		}
		pendingQueryRequests.put(currReq.getRequestId(), currReq);*/
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgFromUser:QueryID", t1);
		
		long t2 = System.currentTimeMillis();
		QueryComponent mostRest = getMostRestrictiveComponent(qcomponents);
		//for (int i=0;i<qcomponents.size();i++)
		{
			//QueryComponent qc = qcomponents.elementAt(i);
			
			String atrName = mostRest.getAttributeName();
			NodeIDType respNodeId = getResponsibleNodeId(atrName);
			
			QueryMsgToMetadataNode<NodeIDType> queryMsgToMetaNode = 
					new QueryMsgToMetadataNode<NodeIDType>(getMyID(), mostRest, currReq.getRequestId(), 
							this.getMyID(), query, grpGUID);
			
			try
			{
				this.messenger.sendToID(respNodeId, queryMsgToMetaNode.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			
			ContextServiceLogger.getLogger().info("Sending predicate mesg from " 
					+ getMyID() +" to node "+respNodeId + 
					" predicate "+mostRest.toString());
		}
		
		// send just the query to update metadata at other query nodes
		if(ContextServiceConfig.GROUP_INFO_COMPONENT)
		{
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgFromUser:Loop", t2);
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgFromUser", t0);
		
		if( ContextServiceConfig.DELAY_MEASURE_PRINTS )
		{
			ContextServiceLogger.getLogger().fine("DelayMeasure: processQueryMsgFromUser "+(System.currentTimeMillis() - t0));
		}
	}
	
	private QueryComponent getMostRestrictiveComponent(Vector<QueryComponent> qcomponents)
	{
		QueryComponent retComponent = null;	
		for( int i=0;i<qcomponents.size();i++ )
		{
			if( retComponent == null )
			{
				retComponent = qcomponents.get(i);
			}
			else
			{
				QueryComponent currComp = qcomponents.get(i);
				double currDiff = currComp.getRightValue() - currComp.getLeftValue();
				double retDiff = retComponent.getRightValue() - retComponent.getLeftValue();
				
				if( currDiff < retDiff )
				{
					retComponent = currComp;
				}
			}
		}
		return retComponent;
	}
	
	/**
	 * Processes QueryMsgToMetadataNode node and 
	 * sends back reply in GenericMessaging tasks
	 * QueryMsgToValuenode
	 * @throws JSONException
	 * @throws IOException
	 */
	private void processQueryMsgToMetadataNode(QueryMsgToMetadataNode<NodeIDType> queryMsgToMetaNode)
	{
		long t0 = System.currentTimeMillis();
		
		log.fine("processQueryMsgToMetadataNode: " +
				"predicate recvd string form "+queryMsgToMetaNode.getQueryComponent());
		
		QueryComponent qc= queryMsgToMetaNode.getQueryComponent();
		String attrName = qc.getAttributeName();
		
		ContextServiceLogger.getLogger().info("Predicate mesg recvd at" 
				+ this.getMyID() +" from node "+queryMsgToMetaNode.getSourceId() +
				" predicate "+qc.toString());
		
		{
			long t1 = System.currentTimeMillis();
			List<MetadataTableInfo<Integer>> attrMetaObjRecList 
							= this.sqlDBObject.getAttributeMetaObjectRecord(attrName, qc.getLeftValue(), qc.getRightValue());
			
			if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgToMetadataNode:DatabaseGet", t1);
			
			long t2 = System.currentTimeMillis();
			for( int i=0; i<attrMetaObjRecList.size(); i++ )
			{
				@SuppressWarnings("unchecked")
				MetadataTableInfo<NodeIDType> currObj = (MetadataTableInfo<NodeIDType>) attrMetaObjRecList.get(i);
				
				//this.contextserviceDB.putGroupGUIDRecord(groupGUIDRec);
				//GroupGUIDInfo grpGUIDInfo = new GroupGUIDInfo(queryMsgToMetaNode.getGroupGUID(),
				//		queryMsgToMetaNode.getQuery());
				
				//currObj.addGroupGUIDInfo(grpGUIDInfo);
				
				QueryMsgToValuenode<NodeIDType> queryMsgToValnode 
					= new QueryMsgToValuenode<NodeIDType>( queryMsgToMetaNode.getSourceId(), qc,
						queryMsgToMetaNode.getRequestId(), queryMsgToMetaNode.getSourceId(),
						queryMsgToMetaNode.getQuery(), queryMsgToMetaNode.getGroupGUID(), attrMetaObjRecList.size() );
				
				ContextServiceLogger.getLogger().info("Sending ValueNodeMessage from" 
						+ this.getMyID() +" to node "+currObj.getNodeID() + 
						" predicate "+qc.toString());
				
				long t3 = System.currentTimeMillis();
				
				try 
				{
					messenger.sendToID(currObj.getNodeID(), queryMsgToValnode.toJSONObject());
				} catch (IOException e)
				{
					e.printStackTrace();
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
				if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgToMetadataNode:NIO", t3);
			}
			if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgToMetadataNode:Loop", t2);
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgToMetadataNode", t0);
		
		if( ContextServiceConfig.DELAY_MEASURE_PRINTS )
		{
			ContextServiceLogger.getLogger().fine("DelayMeasure: processQueryMsgToMetadataNode "+(System.currentTimeMillis() - t0));
		}
	}
	
	/**
	 * Processes the QueryMsgToValuenode and replies with 
	 * QueryMsgToValuenodeReply, which contains the GUIDs
	 */
	private void processQueryMsgToValuenode(QueryMsgToValuenode<NodeIDType> queryMsgToValnode)
	{
		long t0 = System.currentTimeMillis();
		
		QueryComponent predicate = queryMsgToValnode.getQueryComponent();
		Vector<QueryComponent> qcomponents = QueryParser.parseQuery(queryMsgToValnode.getQuery());
		
		JSONArray resultGUIDs = new JSONArray();
		
		if( !ContextServiceConfig.USESQL )
		{
		    List<ValueInfoObjectRecord<Double>> valInfoObjRecList = 
					this.contextserviceDB.getValueInfoObjectRecord
						(predicate.getAttributeName(), predicate.getLeftValue(), predicate.getRightValue());
		    
		    ContextServiceLogger.getLogger().info("QueryMsgToValuenode recvd at " 
					+ this.getMyID() +" from node "+queryMsgToValnode.getSourceId() +
					" predicate "+predicate.toString()+"valInfoObjRecList "+valInfoObjRecList.size());
		    
		    if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgToValuenode:DatabaseGet ", t0);
			
		    long t3 = System.currentTimeMillis();
			for(int i=0;i<valInfoObjRecList.size();i++)
			{
				ValueInfoObjectRecord<Double> valueObjRec = valInfoObjRecList.get(i);
				
				ContextServiceLogger.getLogger().fine("valueObjRec "+valueObjRec);
				
				
				JSONArray nodeGUIDList = valueObjRec.getNodeGUIDList();
				
				for(int j=0;j<nodeGUIDList.length();j++)
				{
					try
					{
						ContextServiceLogger.getLogger().finer("nodeGUIDList "+nodeGUIDList);
						JSONObject nodeGUIDJSON = nodeGUIDList.getJSONObject(j);
						NodeGUIDInfoRecord<Double> nodeGUIDRec = 
								new NodeGUIDInfoRecord<Double>(nodeGUIDJSON);
						
						if( Utils.checkQCForOverlapWithValue(nodeGUIDRec.getAttrValue(), predicate) )
						{
							resultGUIDs.put(nodeGUIDRec.getNodeGUID());
						}
					}
					catch(JSONException jso)
					{
						jso.printStackTrace();
					}
				}
			}
			if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgToValuenode:Loop ", t3);
		}
		else
		{
			resultGUIDs = this.sqlDBObject.getValueInfoObjectRecord
					(predicate.getAttributeName(), predicate.getLeftValue(), predicate.getRightValue());
			ContextServiceLogger.getLogger().fine("processQueryMsgToValuenode:DatabaseGet resultGUIDs "+resultGUIDs.length());
			if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgToValuenode:DatabaseGet ", t0);
			
			if( (qcomponents.size() > 1) && (resultGUIDs.length() > 0) )
			{
				HashMap<NodeIDType, JSONArray> bulkGetMap = new HashMap<NodeIDType, JSONArray>();
				
				for(int i=0;i<resultGUIDs.length();i++)
				{
					try
					{
						String GUID = resultGUIDs.getString(i);
						NodeIDType destNodeId = 
								this.getResponsibleNodeId(GUID);
						
						JSONArray guidJSON = bulkGetMap.get(destNodeId);
						
						if(guidJSON == null)
						{
							guidJSON = new JSONArray();
							guidJSON.put(GUID);
							bulkGetMap.put(destNodeId, guidJSON);
						}
						else
						{
							guidJSON.put(GUID);
						}
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
				}
				long currRequestNum = 0;
				
				/*synchronized(this.readRecordMonitor)
				{
					currRequestNum = this.readRecordStorageNum++;
				}
				RecordReadStorage<NodeIDType> currRequestRec 
							= new RecordReadStorage<NodeIDType>(currRequestNum, bulkGetMap.keySet().size(), 
									queryMsgToValnode);
				
				this.pendingRecordReadReqs.put(currRequestNum, currRequestRec);*/
				
				Iterator<NodeIDType> keyIter = bulkGetMap.keySet().iterator();
				
				while( keyIter.hasNext() )
				{
					NodeIDType destNodeId = keyIter.next();
					JSONArray guidToCheck = bulkGetMap.get(destNodeId);
					
					BulkGet<NodeIDType> bulkGetReq = 
							new BulkGet<NodeIDType>( this.getMyID(), currRequestNum, guidToCheck, queryMsgToValnode.getQuery() );
					
					try {
						this.messenger.sendToID(destNodeId, bulkGetReq.toJSONObject());
					} catch (IOException e) 
					{
						e.printStackTrace();
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}
				}
			}
			else
			{
				/*long requestID = queryMsgToValnode.getRequestId();
				int componentID = predicate.getComponentID();
				
				long t4 = System.currentTimeMillis();
				QueryMsgToValuenodeReply<NodeIDType> queryMsgToValReply 
					= new QueryMsgToValuenodeReply<NodeIDType>(getMyID(), resultGUIDs, requestID, 
							componentID, getMyID(), queryMsgToValnode.getNumValNodesContacted());
							
				try
				{
					this.messenger.sendToID(queryMsgToValnode.getSourceId(), 
							queryMsgToValReply.toJSONObject());
				} catch (IOException e)
				{
					e.printStackTrace();
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
				
				if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgToValuenode:Sending ", t4);
						//relaying the query to the value nodes of the attribute
				
				ContextServiceLogger.getLogger().info("Sending QueryMsgToValuenodeReply from " 
								+ this.getMyID() +" to node "+queryMsgToValnode.getSourceId()+
								" reply "+queryMsgToValReply.toString());*/
			}
		}
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processQueryMsgToValuenode", t0);
		ContextServiceLogger.getLogger().fine("DelayMeasure: processQueryMsgToValuenode "+(System.currentTimeMillis() - t0));
		
		if( ContextServiceConfig.DELAY_MEASURE_PRINTS )
		{
			ContextServiceLogger.getLogger().fine("DelayMeasure: processQueryMsgToValuenode "+(System.currentTimeMillis() - t0));
		}
	}
	
	private void processValueUpdateMsgToMetadataNode(ValueUpdateMsgToMetadataNode<NodeIDType> valUpdateMsgToMetaNode)
	{
		long t0 = System.currentTimeMillis();
		
		long versionNum = valUpdateMsgToMetaNode.getVersionNum();
		String attrName = valUpdateMsgToMetaNode.getAttrName();
		String GUID = valUpdateMsgToMetaNode.getGUID();
		double oldValue = Double.MIN_VALUE;
		double newValue = valUpdateMsgToMetaNode.getNewValue();
		//NodeIDType sourceID = valUpdateMsgToMetaNode.getSourceID();
		long requestID = valUpdateMsgToMetaNode.getRequestID();
		
		ContextServiceLogger.getLogger().info("ValueUpdateToMetadataMesg recvd at " 
				+ this.getMyID() +" for GUID "+GUID+
				" "+attrName + " "+oldValue+" "+newValue);
		
		@SuppressWarnings("unchecked")
		MetadataTableInfo<NodeIDType> newMetaObjRec = 
				(MetadataTableInfo<NodeIDType>) 
				this.sqlDBObject.getAttributeMetaObjectRecord(attrName, newValue, newValue).get(0);
		
		//ContextServiceLogger.getLogger().fine("newMetaObjRec  "+newMetaObjRec.toString() ); 
		// for the new value
		NodeIDType newValueNodeId = newMetaObjRec.getNodeID();
		
		// for the old value
		NodeIDType oldValueNodeId = newValueNodeId;
		if(oldValue != AttributeTypes.NOT_SET)
		{
			//ContextServiceLogger.getLogger().fine("Going oldValue != AttributeTypes.NOT_SET "+oldValue);
			@SuppressWarnings("unchecked")
			MetadataTableInfo<NodeIDType> oldMetaObjRec = 
				(MetadataTableInfo<NodeIDType>) 
				this.sqlDBObject.getAttributeMetaObjectRecord(attrName, oldValue, oldValue).get(0);
				
			oldValueNodeId = oldMetaObjRec.getNodeID();
		}
		
		if( oldValueNodeId.equals(newValueNodeId) )
		{
			//ContextServiceLogger.getLogger().fine("GOes in old = new");
			ValueUpdateMsgToValuenode<NodeIDType> valueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<NodeIDType>
			(this.getMyID(), versionNum, GUID, attrName, oldValue, newValue, 
					ValueUpdateMsgToValuenode.REMOVE_ADD_BOTH, requestID);
			
			try
			{
				this.messenger.sendToID(newValueNodeId, valueUpdateMsgToValnode.toJSONObject());
			} catch (IOException e) 
			{
				e.printStackTrace();
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
			
			ContextServiceLogger.getLogger().info("Sending ValueUpdateMsgToValuenode from" 
					+ this.getMyID() + " to node "+oldValueNodeId +
					" mesg "+valueUpdateMsgToValnode);
		} else
		{
			//ContextServiceLogger.getLogger().fine("GOes in old != new");
			ValueUpdateMsgToValuenode<NodeIDType> oldValueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<NodeIDType>
			(this.getMyID(), versionNum, GUID, attrName, oldValue, newValue, 
					ValueUpdateMsgToValuenode.REMOVE_ENTRY, requestID);
			
			try 
			{
				this.messenger.sendToID(oldValueNodeId, oldValueUpdateMsgToValnode.toJSONObject());
			} catch (IOException e) 
			{
				e.printStackTrace();
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
			
			ValueUpdateMsgToValuenode<NodeIDType> newValueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<NodeIDType>
			(this.getMyID(), versionNum, GUID, attrName, oldValue, newValue, 
					ValueUpdateMsgToValuenode.ADD_ENTRY, requestID);
			
			try
			{
				this.messenger.sendToID(newValueNodeId, newValueUpdateMsgToValnode.toJSONObject());
			} catch (IOException e)
			{
				e.printStackTrace();
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
			
			ContextServiceLogger.getLogger().info("Sending ValueUpdateMsgToValuenode from" 
					+ this.getMyID() + " to node "+oldValueNodeId+" "+ newValueNodeId+
					" mesg "+oldValueUpdateMsgToValnode);
		}
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processValueUpdateMsgToMetadataNode", t0);
		
		if( ContextServiceConfig.DELAY_MEASURE_PRINTS )
		{
			ContextServiceLogger.getLogger().fine("DelayMeasure: processValueUpdateMsgToMetadataNode "+(System.currentTimeMillis() - t0));
		}
	}
	
	/**
	 * adds the reply of the queryComponent
	 * @throws JSONException
	 */
	private void processValueUpdateMsgToValuenode(ValueUpdateMsgToValuenode<NodeIDType> valUpdateMsgToValnode)
	{
		long t0 = System.currentTimeMillis();
		
		ContextServiceLogger.getLogger().info("\n\n Recvd ValueUpdateMsgToValuenode at " 
				+ this.getMyID() +" reply "+valUpdateMsgToValnode);
		
		//LinkedList<GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenodeReply<NodeIDType>>> msgList
		//	= new LinkedList<GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenodeReply<NodeIDType>>>();
		
		String attrName = valUpdateMsgToValnode.getAttrName();
		String GUID = valUpdateMsgToValnode.getGUID();
		double oldValue = valUpdateMsgToValnode.getOldValue();
		double newValue = valUpdateMsgToValnode.getNewValue();
		long versionNum = valUpdateMsgToValnode.getVersionNum();
		//NodeIDType sourceID = valUpdateMsgToValnode.getSourceID();
		//long requestID = valUpdateMsgToValnode.getRequestID();
		// first check whole value ranges to see if this GUID exists and check the version number
		// of update
		
		//FIXME: need to think about consistency, update only for newer version numbers.
		
		boolean doOperation = true;
		
		if(doOperation)
		{
			switch(valUpdateMsgToValnode.getOperType())
			{
				case ValueUpdateMsgToValuenode.ADD_ENTRY:
				{
					if( !ContextServiceConfig.USESQL )
					{
						List<ValueInfoObjectRecord<Double>> valueInfoObjRecList = 
								this.contextserviceDB.getValueInfoObjectRecord(attrName, newValue, newValue);
						
						if(valueInfoObjRecList.size() != 1)
						{
							assert false;
						}
						else
						{
							try
							{
								ValueInfoObjectRecord<Double> valInfoObjRec = valueInfoObjRecList.get(0);
								
								NodeGUIDInfoRecord<Double> nodeGUIDInfRec = new NodeGUIDInfoRecord<Double>
										(GUID, newValue, versionNum, new JSONObject());
								
								//valInfoObjRec.getNodeGUIDList().put(nodeGUIDInfRec);
								this.contextserviceDB.updateValueInfoObjectRecord
											(valInfoObjRec, attrName, nodeGUIDInfRec.toJSONObject(), 
											ValueInfoObjectRecord.Operations.APPEND, ValueInfoObjectRecord.Keys.NODE_GUID_LIST);
							} catch(JSONException jso)
							{
								jso.printStackTrace();
							}
						}
					}
					else
					{
						this.sqlDBObject.putValueObjectRecord(attrName, newValue, GUID, versionNum);
					}
					
					
					//NodeGUIDInfo nodeGUIDObj = new NodeGUIDInfo(GUID, newValue, versionNum);
					//valueObj.addNodeGUID(nodeGUIDObj);
					// send update complete reply back,
					// the source should recv two replies, this one from Add, and one from remove.
					int numRep;
					if(oldValue == AttributeTypes.NOT_SET)
					{
						// first write so just 1 mesg from add
						//sendUpdateReplyBackToUser(sourceIP, sourcePort, versionNum, 1);
						numRep = 1;
					}
					else
					{
						//sendUpdateReplyBackToUser(sourceIP, sourcePort, versionNum, 2);
						numRep = 2;
					}
					
					/*ValueUpdateMsgToValuenodeReply<NodeIDType> newValueUpdateMsgReply
						= new ValueUpdateMsgToValuenodeReply<NodeIDType>
						(this.getMyID(), versionNum, numRep, requestID);
					try
					{
						this.messenger.sendToID(sourceID, newValueUpdateMsgReply.toJSONObject());
					} catch (IOException e) 
					{
						e.printStackTrace();
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}*/
					
					break;
				}
				case ValueUpdateMsgToValuenode.REMOVE_ENTRY:
				{	
					if( !ContextServiceConfig.USESQL )
					{
						List<ValueInfoObjectRecord<Double>> valueInfoObjRecList = 
								this.contextserviceDB.getValueInfoObjectRecord(attrName, newValue, newValue);
						
						if(valueInfoObjRecList.size() != 1)
						{
							assert false;
						}
						else
						{
							try
							{
								ValueInfoObjectRecord<Double> valInfoObjRec = valueInfoObjRecList.get(0);
								
								NodeGUIDInfoRecord<Double> nodeGUIDInfRec = new NodeGUIDInfoRecord<Double>
										(GUID, newValue, versionNum, new JSONObject());
								
								//valInfoObjRec.getNodeGUIDList().put(nodeGUIDInfRec);
								this.contextserviceDB.updateValueInfoObjectRecord
											(valInfoObjRec, attrName, nodeGUIDInfRec.toJSONObject(), 
											ValueInfoObjectRecord.Operations.REMOVE, ValueInfoObjectRecord.Keys.NODE_GUID_LIST);
							} catch(JSONException jso)
							{
								jso.printStackTrace();
							}
						}
					}
					else
					{
						this.sqlDBObject.updateValueInfoObjectRecord(attrName, ValueTableInfo.Operations.REMOVE, 
								GUID, newValue, versionNum);
					}
					
					// send update complete reply back
					//sendUpdateReplyBackToUser(sourceIP, sourcePort, versionNum, 2);
					/*ValueUpdateMsgToValuenodeReply<NodeIDType> newValueUpdateMsgReply
					= new ValueUpdateMsgToValuenodeReply<NodeIDType>
					(this.getMyID(), versionNum, 2, requestID);
				
					try 
					{
						this.messenger.sendToID(sourceID, newValueUpdateMsgReply.toJSONObject());
					} catch (IOException e) 
					{
						e.printStackTrace();
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}*/
					
					break;
				}
				case ValueUpdateMsgToValuenode.REMOVE_ADD_BOTH:
				{
					//ContextServiceLogger.getLogger().fine("REMOVE_ADD_BOTH ");
					//FIXME: may need atomicity here
					// just a value update, but goes to the same node
					//remove
					/*valueObj.removeNodeGUID(GUID);
					
					// and add
					NodeGUIDInfo nodeGUIDObj = new NodeGUIDInfo(GUID, newValue, versionNum);
					valueObj.addNodeGUID(nodeGUIDObj);*/
					if( !ContextServiceConfig.USESQL)
					{
						List<ValueInfoObjectRecord<Double>> valueInfoObjRecList = 
								this.contextserviceDB.getValueInfoObjectRecord(attrName, newValue, newValue);
						
						if(valueInfoObjRecList.size() != 1)
						{
							assert false;
						}
						else
						{
							try
							{
								ValueInfoObjectRecord<Double> valInfoObjRec = valueInfoObjRecList.get(0);
								
								NodeGUIDInfoRecord<Double> nodeGUIDInfRec = new NodeGUIDInfoRecord<Double>
										(GUID, newValue, versionNum, new JSONObject());
									
								this.contextserviceDB.updateValueInfoObjectRecord
								(valInfoObjRec, attrName, nodeGUIDInfRec.toJSONObject(), 
								ValueInfoObjectRecord.Operations.REMOVE, ValueInfoObjectRecord.Keys.NODE_GUID_LIST);
								
								//valInfoObjRec.getNodeGUIDList().put(nodeGUIDInfRec);
								this.contextserviceDB.updateValueInfoObjectRecord
											(valInfoObjRec, attrName, nodeGUIDInfRec.toJSONObject(), 
											ValueInfoObjectRecord.Operations.APPEND, ValueInfoObjectRecord.Keys.NODE_GUID_LIST);
							} catch(JSONException jso)
							{
								jso.printStackTrace();
							}
						}
					}
					else
					{
						
						// no record, so insert
						if(oldValue == Double.MIN_VALUE)
						{
							//ContextServiceLogger.getLogger().fine("REMOVE_ADD_BOTH: SQL PUT");
							this.sqlDBObject.putValueObjectRecord(attrName, newValue, GUID, versionNum);
						}
						else // record present, so update
						{
							//ContextServiceLogger.getLogger().fine("REMOVE_ADD_BOTH: SQL UPDATE");
							this.sqlDBObject.updateValueInfoObjectRecord(attrName, ValueTableInfo.Operations.UPDATE, 
								GUID, newValue, versionNum);
						}
					}
					
					// send update complete reply back
					//sendUpdateReplyBackToUser(sourceIP, sourcePort, versionNum, 1);
					/*ValueUpdateMsgToValuenodeReply<NodeIDType> newValueUpdateMsgReply
					= new ValueUpdateMsgToValuenodeReply<NodeIDType>
					(this.getMyID(), versionNum, 1, requestID);
				
					try
					{
						this.messenger.sendToID(sourceID, newValueUpdateMsgReply.toJSONObject());
					} catch (IOException e)
					{
						e.printStackTrace();
					} catch (JSONException e)
					{
						e.printStackTrace();
					}*/
					break;
				}
			}
		}
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processValueUpdateMsgToValuenode", t0);
		
		if( ContextServiceConfig.DELAY_MEASURE_PRINTS )
		{
			ContextServiceLogger.getLogger().fine("DelayMeasure: processValueUpdateMsgToValuenode "+(System.currentTimeMillis() - t0));
		}
	}
	
	//private void 
	//	processValueUpdateMsgToValuenodeReply(ValueUpdateMsgToValuenodeReply<NodeIDType> valUpdateMsgToValnodeRep)
	//{
		//long t0 = System.currentTimeMillis();
		
		/*long requestId =  valUpdateMsgToValnodeRep.getRequestID();
		UpdateInfo<NodeIDType> updateInfo = pendingUpdateRequests.get(requestId);
		if(updateInfo != null)
		{
			updateInfo.incrementNumReplyRecvd();
			if(updateInfo.getNumReplyRecvd() == valUpdateMsgToValnodeRep.getNumReply())
			{
				//String mesg = "Value update complete";
				//Utils.sendUDP(mesg);
				String sourceIP = updateInfo.getValueUpdateFromGNS().getSourceIP();
				int sourcePort = updateInfo.getValueUpdateFromGNS().getSourcePort();
				
				synchronized(this.pendingUpdateLock)
				{
					if( sourceIP.equals("") && sourcePort == -1 )
					{
						// no reply
					} else
					{
						if( this.pendingUpdateRequests.get(updateInfo.getRequestId())  != null )
						{
							sendUpdateReplyBackToUser( sourceIP, 
								sourcePort, updateInfo.getValueUpdateFromGNS().getVersionNum() );
						}
					}
					pendingUpdateRequests.remove(requestId);
				}
			}
		}*/	
	//	if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processValueUpdateMsgToValuenodeReply", t0);
	//}
	
	/**
	 * adds the reply of the queryComponent
	 * @throws JSONException
	 */
	private void processValueUpdateFromGNS(ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS)
	{
		long t0 = System.currentTimeMillis();
		
		ContextServiceLogger.getLogger().fine("\n\n Recvd ValueUpdateFromGNS at " 
				+ this.getMyID() +" reply "+valUpdMsgFromGNS);
		
		long versionNum = valUpdMsgFromGNS.getVersionNum();
		String GUID = valUpdMsgFromGNS.getGUID();
		JSONObject attrValuePairs = valUpdMsgFromGNS.getAttrValuePairs();
		
		//String attrName = valUpdMsgFromGNS.getAttrName();
		//String oldVal = "";
		//String newVal = valUpdMsgFromGNS.getNewVal();
		//JSONObject allAttrs = new JSONObject();
		/*double oldValD, newValD;
		if(oldVal.equals(""))
		{
			oldValD = AttributeTypes.NOT_SET;
		} else
		{
			oldValD = Double.parseDouble(oldVal);
		}
		newValD = Double.parseDouble(newVal);*/
		
		//long currReqID = -1;
		// send update to the consistent storage node	
		ConsistentStoragePut<NodeIDType> bulkPutMesg = 
				new ConsistentStoragePut<NodeIDType>(this.getMyID(), GUID , attrValuePairs, 
						versionNum);
					
		NodeIDType destNodeID = this.getResponsibleNodeId(GUID);
		
		try
		{
			//ContextServiceLogger.getLogger().fine("Sending ConsistentStoragePut");
			this.messenger.sendToID(destNodeID, bulkPutMesg.toJSONObject());
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		/*synchronized(this.pendingUpdateLock)
		{
			UpdateInfo<NodeIDType> currReq 
				= new UpdateInfo<NodeIDType>(valUpdMsgFromGNS, updateIdCounter++);
			currReqID = currReq.getRequestId();
			pendingUpdateRequests.put(currReqID, currReq);
		}*/
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processValueUpdateFromGNS", t0);
		
		if( ContextServiceConfig.DELAY_MEASURE_PRINTS )
		{
			ContextServiceLogger.getLogger().fine("DelayMeasure: processValueUpdateFromGNS "+(System.currentTimeMillis() - t0));
		}
	}
	
	private void processBulkGet(BulkGet<NodeIDType> bulkGet)
	{
		long t0 = System.currentTimeMillis();
		
		JSONArray getMatchingGUIDs = this.sqlDBObject.checkGUIDsForQuery
				(bulkGet.getQuery(), bulkGet.getGUIDsToGet());
		
		/*NodeIDType destID = bulkGet.getInitiator();
		BulkGetReply<NodeIDType> bulkGetRep = new 
				BulkGetReply<NodeIDType>(this.getMyID(), bulkGet.getReqID(), getMatchingGUIDs);
		
		try 
		{
			this.messenger.sendToID(destID, bulkGetRep.toJSONObject());
		} catch (IOException e) 
		{
			e.printStackTrace();
		} catch (JSONException e) 
		{
			e.printStackTrace();
		}*/
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processBulkGet", t0);
		ContextServiceLogger.getLogger().fine("DelayMeasure: processBulkGet "+(System.currentTimeMillis() - t0));
		if( ContextServiceConfig.DELAY_MEASURE_PRINTS )
		{
			ContextServiceLogger.getLogger().fine("DelayMeasure: processBulkGet "+(System.currentTimeMillis() - t0));
		}
	}
	
	private void processBulkGetReply( BulkGetReply<NodeIDType> bulkGetReplyMesg )
	{
		long bulkGetID = bulkGetReplyMesg.getReqID();

		RecordReadStorage<NodeIDType> recReadSto 
							= this.pendingRecordReadReqs.get(bulkGetID);

		ContextServiceLogger.getLogger().info("processBulkGetReply "+bulkGetID);

		if( recReadSto != null )
		{
			ContextServiceLogger.getLogger().info("recReadSto ");
	
			JSONArray resultArray = recReadSto.addBulkGetReply(bulkGetReplyMesg);
			// only one reply will go here.
			if( resultArray != null )
			{
				ContextServiceLogger.getLogger().info("resultArray != null "+resultArray);
    
				QueryMsgToValuenodeReply<NodeIDType> queryMsgToValReply 
					= new QueryMsgToValuenodeReply<NodeIDType>(getMyID(), resultArray, 
							recReadSto.getQueryMsgToValnode().getRequestId(), 
							recReadSto.getQueryMsgToValnode().getQueryComponent().getComponentID(), getMyID(), 
							recReadSto.getQueryMsgToValnode().getNumValNodesContacted());
				
				try 
				{
					this.messenger.sendToID(recReadSto.getQueryMsgToValnode().getSourceId(),
							queryMsgToValReply.toJSONObject());
				} catch (IOException e) 
				{
					e.printStackTrace();
				} catch (JSONException e) 
				{
					e.printStackTrace();
				}

			ContextServiceLogger.getLogger().info("Sending QueryMsgToValuenodeReply from " 
					+ this.getMyID() +" to node "+recReadSto.getQueryMsgToValnode().getSourceId() +
					" reply "+queryMsgToValReply.toString());
			
			this.pendingRecordReadReqs.remove(bulkGetID);
			}
		}
		
	}
	
	private void processConsistentStoragePut(ConsistentStoragePut<NodeIDType> bulkPutMesg)
	{
		String guid = bulkPutMesg.getGUID();
		JSONObject attrValuePairs = bulkPutMesg.getAttrValuePair();
		long versionNum = bulkPutMesg.getVersionNum();
		String GUID = bulkPutMesg.getGUID();
		JSONObject oldValueJSON = new JSONObject();
		
		//ContextServiceLogger.getLogger().fine("Recvd processConsistentStoragePut");
		try 
		{
			oldValueJSON = this.sqlDBObject.storeInFullDataObjTable(guid, attrValuePairs);
		} catch (JSONException e2) 
		{
			e2.printStackTrace();
		}
		
		long currReqID = -1;
		
		@SuppressWarnings("unchecked")
		Iterator<String> jsoObjKeysIter = attrValuePairs.keys();
		
		int numSent = 0;
		while( jsoObjKeysIter.hasNext() )
		{
			String attrName = jsoObjKeysIter.next();
			
			try
			{
				double newValue = attrValuePairs.getDouble(attrName);
				double oldValue = oldValueJSON.getDouble(attrName);

				ValueUpdateMsgToMetadataNode<NodeIDType> valueUpdMsgToMetanode = 
						new ValueUpdateMsgToMetadataNode<NodeIDType>(this.getMyID(), versionNum, GUID, attrName, 
								oldValue ,newValue, currReqID);
					
				NodeIDType respMetadataNodeId = this.getResponsibleNodeId(attrName);
					
				try 
				{
					this.messenger.sendToID(respMetadataNodeId, valueUpdMsgToMetanode.toJSONObject());
				} catch (IOException e) 
				{
					e.printStackTrace();
				} catch (JSONException e) 
				{
					e.printStackTrace();
				}
			} catch (JSONException e1) 
			{
				e1.printStackTrace();
			}
			numSent++;
		}
		//ContextServiceLogger.getLogger().fine("Sending ValueUpdateMsgToMetadataNode numSent "+numSent);
	}
	
	private void processConsistentStoragePutReply(ConsistentStoragePutReply<NodeIDType> consStoReply)
	{
	}
	
	private LinkedList<GroupGUIDRecord> getGroupsAffectedUsingDatabase
	(AttributeMetaObjectRecord<NodeIDType, Double> metaObjRec, JSONObject allAttr, 
			String updateAttrName, double attrVal) throws JSONException
	{
		long t0 = System.currentTimeMillis();
		
		LinkedList<GroupGUIDRecord> satisfyingGroups = new LinkedList<GroupGUIDRecord>();
		JSONArray groupGUIDList = metaObjRec.getGroupGUIDList();
		
		log.fine("metaObjRec "+metaObjRec+"groupGUIDList "+groupGUIDList);
		for(int i=0;i<groupGUIDList.length();i++)
		{
			JSONObject groupGUIDJSON = groupGUIDList.getJSONObject(i);
			
			GroupGUIDRecord groupGUIDRec = new GroupGUIDRecord(groupGUIDJSON);
			
			//this.getContextServiceDB().getGroupGUIDRecord(groupGUID);
			
			boolean groupCheck = Utils.groupMemberCheck( allAttr, updateAttrName, 
					attrVal, groupGUIDRec.getGroupQuery() );
			
			log.fine("checking group "+groupGUIDJSON+" groupCheck "+groupCheck);
			if(groupCheck)
			{
				//GroupGUIDInfo guidInfo = new GroupGUIDInfo(groupGUID, groupGUIDRec.getGroupQuery());
				satisfyingGroups.add(groupGUIDRec);
			}
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("getGroupsAffectedUsingDatabase", t0);
		return satisfyingGroups;
	}
	
	/**
	 * stores the replies for different query components, until all of the query 
	 * component replies arrive and they are returned to the user/application.
	 * @param valueReply
	 * @throws JSONException 
	 */
	private void addQueryReply(QueryMsgToValuenodeReply<NodeIDType> queryMsgToValnodeRep)
			throws JSONException
	{
		long t0 = System.currentTimeMillis();
		
		long requestId =  queryMsgToValnodeRep.getRequestID();
		QueryInfo<NodeIDType> queryInfo = pendingQueryRequests.get(requestId);
		
		if(queryInfo != null)
		{
			processReplyInternally(queryMsgToValnodeRep, queryInfo);
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("addQueryReply", t0);
	}
	
	public void checkQueryCompletion(QueryInfo<NodeIDType> qinfo)
	{
		long t0 = System.currentTimeMillis();
		
		//if( qinfo.componentReplies.size() == qinfo.queryComponents.size() )
		if( qinfo.componentReplies.size() == 1 )  // just 1 component is used here
		{
			int compID = qinfo.componentReplies.keySet().iterator().next();
			for(int i=0;i<qinfo.queryComponents.size();i++)
			{
				QueryComponent qc = qinfo.queryComponents.get(i);
				if(qc.getComponentID() == compID)
				{
					if( qc.getNumCompReplyRecvd() != qc.getTotalCompReply() )
					{
						break;
					}
					else
					{
						ContextServiceLogger.getLogger().info("\n\n All replies recvd for each component "+
								qc.getNumCompReplyRecvd()+" "+ qc.getTotalCompReply());
						
						JSONArray queryAnswer = new JSONArray();
						  
						LinkedList<String> result = qinfo.componentReplies.get(qc.getComponentID());
						  for(int j=0;j<result.size();j++)
						  {
							  queryAnswer.put(result.get(j));
						  }
						
						ContextServiceLogger.getLogger().info("Query Answer "+queryAnswer);
						
						//FIXME: uncomment this, just for debugging
						GNSCalls.addGUIDsToGroup(queryAnswer, qinfo.getQuery(), qinfo.getGroupGUID());
						
						//takes care of not sending two replies, as concurrent queue will return only one non null;
						QueryInfo<NodeIDType> removedQInfo = this.pendingQueryRequests.remove(qinfo.getRequestId());
						if(removedQInfo != null)
						{
							sendReplyBackToUser(qinfo, queryAnswer);
						}
					}
				}
			}
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("checkQueryCompletion", t0);
	}
	
	/*private boolean checkIfAllRepliesRecvd(QueryInfo<NodeIDType> qinfo)
	{
		long t0 = System.currentTimeMillis();
		int compID = qinfo.componentReplies.keySet().iterator().next();
		boolean resultRet = true;
		for(int i=0;i<qinfo.queryComponents.size();i++)
		{
			QueryComponent qc = qinfo.queryComponents.get(i);
			if(qc.getComponentID() == compID)
			{
				if( qc.getNumCompReplyRecvd() != qc.getTotalCompReply() )
				{
					resultRet = false;
					
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("checkIfAllRepliesRecvd", t0);
					
					return resultRet;
				}
			}
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("checkIfAllRepliesRecvd", t0);
		return resultRet;
	}*/
	
	/**
	 * Function stays here, it will be moved to value partitioner package
	 * whenever that package is decided upon.
	 * Uniformly assigns the value ranges to the nodes in
	 * the system for the given attribute.
	 */
	private GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] 
			assignValueRanges(NodeIDType initiator, String attrName, double attrMin, double attrMax)
	{
		long t0 = System.currentTimeMillis();
		
		//int numValueNodes = this.getAllNodeIDs().size();
		int numValueNodes = 3;
		@SuppressWarnings("unchecked")
		GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] mesgArray 
								= new GenericMessagingTask[numValueNodes];
		
		Set<NodeIDType> allNodeIDs = this.getAllNodeIDs();
		
		int numNodes = allNodeIDs.size();
		
		double attributeMin = attrMin;
		double attributeMax = attrMax;
		
		
		//String attributeHash = Utils.getSHA1(attributeName);
		int mapIndex = Hashing.consistentHash(attrName.hashCode(), numNodes);
			
		for(int i=0;i<numValueNodes;i++)
		{
			double rangeSplit = (attributeMax - attributeMin)/numValueNodes;
			double currMinRange = attributeMin + rangeSplit*i;
			double currMaxRange = attributeMin + rangeSplit*(i+1);
			
			if( currMaxRange > attributeMax )
			{
				currMaxRange = attributeMax;
			}
			
			int currIndex = (mapIndex + i + 1) % numNodes;
			
			@SuppressWarnings("unchecked")
			NodeIDType[] allNodeIDArr = (NodeIDType[]) allNodeIDs.toArray();
			
			NodeIDType currNodeID = (NodeIDType)allNodeIDArr[currIndex];
			
			//AttributeMetadataObject<NodeIDType> attObject = new AttributeMetadataObject<NodeIDType>( currMinRange, 
			//		currMaxRange, currNodeID );
			
			//if(ContextServiceConfig.CACHE_ON)
			//{
			//	metadataInformation.add(attObject);
			//}
			//else
			{
				// add this to database, not to memory
				if( !ContextServiceConfig.USESQL )
				{
					AttributeMetaObjectRecord<NodeIDType, Double> attrMetaObjRec = new
						AttributeMetaObjectRecord<NodeIDType, Double>(currMinRange, currMaxRange,
						currNodeID, new JSONArray());
				
					this.getContextServiceDB().putAttributeMetaObjectRecord(attrMetaObjRec, attrName);
				}
				else
				{
					this.sqlDBObject.putAttributeMetaObjectRecord(attrName, 
							currMinRange, currMaxRange, currNodeID);
					
				}
			}
			
			MetadataMsgToValuenode<NodeIDType> metaMsgToValnode = new MetadataMsgToValuenode<NodeIDType>
							( initiator, attrName, currMinRange, currMaxRange);
			
			GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>> mtask = new GenericMessagingTask<NodeIDType, 
					MetadataMsgToValuenode<NodeIDType>>((NodeIDType) currNodeID, metaMsgToValnode);
			
			mesgArray[i] = mtask;
			
			ContextServiceLogger.getLogger().info("csID "+getMyID()+" Metadata Message attribute "+
					attrName+"dest "+currNodeID+" min range "+currMinRange+" max range "+currMaxRange);
			
			//JSONObject metadataJSON = metadata.getJSONMessage();
			//ContextServiceLogger.getLogger().info("Metadata Message attribute "+attributeName+
			//		"dest "+currNodeID+" min range "+currMinRange+" max range "+currMaxRange);
			// sending the message
			//StartContextServiceNode.sendToNIOTransport(currNodeID, metadataJSON);
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("assignValueRanges", t0);
		
		return mesgArray;
	}
	
	protected void processReplyInternally
	(QueryMsgToValuenodeReply<NodeIDType> queryMsgToValnodeRep, QueryInfo<NodeIDType> queryInfo)
	{
		long t0 = System.currentTimeMillis();
		
		int compId = queryMsgToValnodeRep.getComponentID();
		LinkedList<String> GUIDs = queryInfo.componentReplies.get(compId);
		if( GUIDs == null )
		{
			GUIDs = new LinkedList<String>();
			JSONArray recvArr = queryMsgToValnodeRep.getResultGUIDs();
			//ContextServiceLogger.getLogger().fine("JSONArray size "+recvArr.length() +" "+recvArr);
			for(int i=0; i<recvArr.length();i++)
			{
				try
				{
					GUIDs.add(recvArr.getString(i));
				} catch(JSONException jso)
				{
					jso.printStackTrace();;
				}
			}
			queryInfo.componentReplies.put(compId, GUIDs);
		}
		else
		{
			// merge with exiting GUIDs
			JSONArray recvArr = queryMsgToValnodeRep.getResultGUIDs();
			//ContextServiceLogger.getLogger().fine("JSONArray size "+recvArr.length() +" "+recvArr);
			for(int i=0; i<recvArr.length();i++)
			{
				try
				{
					GUIDs.add(recvArr.getString(i));
				} catch(JSONException jso)
				{
					jso.printStackTrace();
				}
			}
			queryInfo.componentReplies.put(compId, GUIDs);
		}
		
		this.updateNumberOfRepliesRecvd(queryMsgToValnodeRep, queryInfo);
		
		checkQueryCompletion(queryInfo);
		
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("processReplyInternally", t0);
		
		//ContextServiceLogger.getLogger().fine("componentReplies.size() "+componentReplies.size() +
		//		" queryComponents.size() "+queryComponents.size());
		// if there is at least one replies recvd for each component
	}
	
	private void updateNumberOfRepliesRecvd
		(QueryMsgToValuenodeReply<NodeIDType> queryMsgToValnodeRep, QueryInfo<NodeIDType> queryInfo)
	{
		long t0 = System.currentTimeMillis();
		
		for(int i=0;i<queryInfo.queryComponents.size();i++)
		{
			QueryComponent qc = queryInfo.queryComponents.get(i);
			if(qc.getComponentID() == queryMsgToValnodeRep.getComponentID())
			{
				qc.updateNumCompReplyRecvd();
				qc.setTotalCompReply(queryMsgToValnodeRep.getNumValNodesContacted());
			}
		}
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("updateNumberOfRepliesRecvd", t0);
	}
	
	// checks the ip addresses, so that it desn't send notifications twice to same ip
	public void sendNotifications(LinkedList<GroupGUIDRecord> oldGroupLists, LinkedList<GroupGUIDRecord> newGroupLists
			, long versionNum)
	{
		long t0 = System.currentTimeMillis();
		// hash map removes the duplicates
		HashMap<String, String> ipAddressMap = new HashMap<String, String>();
		
		if( oldGroupLists != null)
		{
			for(int i=0;i<oldGroupLists.size();i++)
			{
				GroupGUIDRecord curr = oldGroupLists.get(i);
				JSONArray arr = GNSCalls.getNotificationSetOfAGroup(curr.getGroupQuery(), curr.getGroupGUID());
				
				for(int j=0;j<arr.length();j++)
				{
					try
					{
						String ipport = arr.getString(j);
						ipAddressMap.put(ipport, ipport);
									
						//String [] parsed = ipport.split(":");
						//sendRefreshReplyBackToUser(parsed[0], Integer.parseInt(parsed[1]), 
						//		curr.getGroupQuery(), curr.getGroupGUID());
						//sendNotification(ipport);
					} catch (JSONException e)
					{
						e.printStackTrace();
					} catch (NumberFormatException e)
					{
						e.printStackTrace();
					}
				}
			}
		}
		
		if( newGroupLists != null )
		{
			for(int i=0;i<newGroupLists.size();i++)
			{
				GroupGUIDRecord curr = newGroupLists.get(i);
				JSONArray arr = GNSCalls.getNotificationSetOfAGroup(curr.getGroupQuery(), curr.getGroupGUID());
				
				for(int j=0;j<arr.length();j++)
				{
					try
					{
						String ipport = arr.getString(j);
						ipAddressMap.put(ipport, ipport);
						
						
						//String [] parsed = ipport.split(":");
						//sendRefreshReplyBackToUser(parsed[0], Integer.parseInt(parsed[1]), 
						//		curr.getGroupQuery(), curr.getGroupGUID());
						//sendNotification(ipport);
					} catch (JSONException e)
					{
						e.printStackTrace();
					} catch (NumberFormatException e)
					{
						e.printStackTrace();
					}
				}
			}
		}
		
		/*Iterator<String> keys = ipAddressMap.keySet().iterator();
		while( keys.hasNext() )
		{
			String key = keys.next();
			String [] parsed = key.split(":");
			sendRefreshReplyBackToUser(parsed[0], Integer.parseInt(parsed[1]), 
					"groupQuery", "groupGUID", versionNum);
			//sendNotification(key);
		}*/
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("sendNotifications", t0);
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
					long t0 = System.currentTimeMillis();	
					@SuppressWarnings("unchecked")
					QueryMsgFromUser<NodeIDType> queryMsgFromUser 
											= (QueryMsgFromUser<NodeIDType>)event;
					
					processQueryMsgFromUser(queryMsgFromUser);
					
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleQueryMsgFromUser", t0);
					break;
				}
				case QUERY_MSG_TO_METADATANODE:
				{
					long t0 = System.currentTimeMillis();
					
					@SuppressWarnings("unchecked")
					QueryMsgToMetadataNode<NodeIDType> queryMsgToMetaNode = 
							(QueryMsgToMetadataNode<NodeIDType>) event;
					
					log.fine("CS"+getMyID()+" received " + event.getType() + ": " + event);
					
					processQueryMsgToMetadataNode(queryMsgToMetaNode);
					
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleQueryMsgToMetadataNode", t0);
					break;
				}
				case QUERY_MSG_TO_VALUENODE:
				{
					long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					QueryMsgToValuenode<NodeIDType> queryMsgToValnode = 
							(QueryMsgToValuenode<NodeIDType>)event;
					
					log.fine("CS"+getMyID()+" received " + event.getType() + ": " + queryMsgToValnode);
					
					processQueryMsgToValuenode(queryMsgToValnode);
					
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleQueryMsgToValuenode", t0);
				}
				case QUERY_MSG_TO_VALUENODE_REPLY:
				{	
					long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					QueryMsgToValuenodeReply<NodeIDType> queryMsgToValnodeReply = 
							(QueryMsgToValuenodeReply<NodeIDType>)event;
					
					ContextServiceLogger.getLogger().info("Recvd QueryMsgToValuenodeReply at " 
							+ getMyID() +" reply "+queryMsgToValnodeReply.toString());
					
					try
					{
						addQueryReply(queryMsgToValnodeReply);
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleQueryMsgToValuenodeReply", t0);
					break;
				}
				case VALUE_UPDATE_MSG_FROM_GNS:
				{
					long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS = (ValueUpdateFromGNS<NodeIDType>)event;
					log.info("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgFromGNS);
					
					processValueUpdateFromGNS(valUpdMsgFromGNS);
					
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleValueUpdateFromGNS", t0);
					
					break;
				}
				case VALUE_UPDATE_MSG_TO_METADATANODE:
				{
					/* Actions:
					 * - send the update message to the responsible value node
					 */
					long t0 = System.currentTimeMillis();
					
					@SuppressWarnings("unchecked")
					ValueUpdateMsgToMetadataNode<NodeIDType> valUpdateMsgToMetaNode 
								= (ValueUpdateMsgToMetadataNode<NodeIDType>)event;
					log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valUpdateMsgToMetaNode);
					
					processValueUpdateMsgToMetadataNode(valUpdateMsgToMetaNode);
					
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleValueUpdateMsgToMetadataNode", t0);
					break;
				}
				case VALUE_UPDATE_MSG_TO_VALUENODE:
				{
					long t0 = System.currentTimeMillis();
					
					@SuppressWarnings("unchecked")
					ValueUpdateMsgToValuenode<NodeIDType> valUpdMsgToValnode = (ValueUpdateMsgToValuenode<NodeIDType>)event;
					log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgToValnode);
					
					processValueUpdateMsgToValuenode(valUpdMsgToValnode);
					
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleValueUpdateMsgToValuenode", t0);
					break;
				}
				case VALUE_UPDATE_MSG_TO_VALUENODE_REPLY:
				{
					/*long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					ValueUpdateMsgToValuenodeReply<NodeIDType> valUpdMsgToValnode = (ValueUpdateMsgToValuenodeReply<NodeIDType>)event;
					log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgToValnode);
					processValueUpdateMsgToValuenodeReply(valUpdMsgToValnode);
					
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleValueUpdateMsgToValuenodeReply", t0);*/
					
					break;
				}
				
				case BULK_GET:
				{
					long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					BulkGet<NodeIDType> bulkGetMesg = (BulkGet<NodeIDType>)event;
					//log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgToValnode);
					processBulkGet(bulkGetMesg);
					
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleBulkGet", t0);
					break;
				}
				
				case BULK_GET_REPLY:
				{
					long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					BulkGetReply<NodeIDType> bulkGetRepMesg = (BulkGetReply<NodeIDType>)event;
					processBulkGetReply(bulkGetRepMesg);
					
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleBulkGetReply", t0);
					
					break;
				}
				
				case CONSISTENT_STORAGE_PUT:
				{
					long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					ConsistentStoragePut<NodeIDType> bulkPutMesg = (ConsistentStoragePut<NodeIDType>)event;
					processConsistentStoragePut(bulkPutMesg);
					
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleConsistentStoragePut ", t0);
					break;
				}
				
				case CONSISTENT_STORAGE_PUT_REPLY:
				{
					long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					ConsistentStoragePutReply<NodeIDType> consStoPutReply = (ConsistentStoragePutReply<NodeIDType>)event;
					processConsistentStoragePutReply(consStoPutReply);
					
					if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleConsistentStoragePutReply ", t0);
					break;
				}
			}
		}
	}
	
	public GenericMessagingTask<NodeIDType, ?>[] handleEchoMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks)
	{
		long t0 = System.currentTimeMillis();
		@SuppressWarnings("unchecked")
		EchoMessage<NodeIDType> echoMessage = (EchoMessage<NodeIDType>)event;
		
		EchoReplyMessage<NodeIDType> valUR = 
				new EchoReplyMessage<NodeIDType>(this.getMyID(), "echoReply");
		
		String sourceIP = echoMessage.getSourceIP();
		int sourcePort  = echoMessage.getSourcePort();
		
		try
		{
			log.fine("sendEchoReplyBackToUser "+sourceIP+" "+sourcePort+
				valUR.toJSONObject());
			
			//ContextServiceLogger.getLogger().fine("sendEchoReplyBackToUser "+sourceIP+" "+sourcePort+
			//		valUR.toJSONObject());
		
			this.messenger.sendToAddress(
				new InetSocketAddress(InetAddress.getByName(sourceIP), sourcePort)
							, valUR.toJSONObject());
			
		} catch (UnknownHostException e)
		{
			e.printStackTrace();
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		if(ContextServiceConfig.DELAY_PROFILER_ON) DelayProfiler.updateDelay("handleEchoMessage", t0);
		
		return null;
	}
	
	/*private void sendNotification(String ipPort) throws NumberFormatException, IOException
	{
		String [] parsed = ipPort.split(":");
		byte[] send_data = new byte[1024]; 
		send_data = new String("REFRESH").getBytes();
        DatagramPacket send_packet = new DatagramPacket(send_data, send_data.length, 
                                                        InetAddress.getByName(parsed[0]), Integer.parseInt(parsed[1]));
        client_socket.send(send_packet);
	}*/
	
	private class ProfilerStatClass implements Runnable
	{
		@Override
		public void run()
		{
			while(true)
			{
				try
				{
					Thread.sleep(100000);
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
				//ContextServiceLogger.getLogger().fine( "Pending query requests "+pendingQueryRequests.size() );
				ContextServiceLogger.getLogger().fine("DelayProfiler stats "+DelayProfiler.getStats());
			}
		}
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryMesgToSubspaceRegion(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryMesgToSubspaceRegionReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateToSubspaceRegionMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleGetMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleGetReplyMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleValueUpdateToSubspaceRegionReplyMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleQueryTriggerMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleUpdateTriggerMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleUpdateTriggerReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}
}