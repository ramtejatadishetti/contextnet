package edu.umass.cs.contextservice.schemes.old;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.hash.Hashing;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;

import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.records.AttributeMetaObjectRecord;
import edu.umass.cs.contextservice.database.records.AttributeMetadataInfoRecord;
import edu.umass.cs.contextservice.database.records.GroupGUIDRecord;
import edu.umass.cs.contextservice.database.records.NodeGUIDInfoRecord;
import edu.umass.cs.contextservice.database.records.ValueInfoObjectRecord;
import edu.umass.cs.contextservice.gns.GNSCalls;
import edu.umass.cs.contextservice.gns.GNSCallsOriginal;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.ContextServicePacket.PacketType;
import edu.umass.cs.contextservice.messages.MetadataMsgToValuenode;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgToMetadataNode;
import edu.umass.cs.contextservice.messages.QueryMsgToValuenode;
import edu.umass.cs.contextservice.messages.QueryMsgToValuenodeReply;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateMsgToMetadataNode;
import edu.umass.cs.contextservice.messages.ValueUpdateMsgToValuenode;
import edu.umass.cs.contextservice.messages.ValueUpdateMsgToValuenodeReply;
import edu.umass.cs.contextservice.queryparsing.QueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.queryparsing.QueryParser;
import edu.umass.cs.contextservice.queryparsing.UpdateInfo;
import edu.umass.cs.contextservice.schemes.AbstractScheme;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.InterfaceNodeConfig;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;
import edu.umass.cs.utils.DelayProfiler;

public class MercurySchemeConsistent<Integer> extends AbstractScheme<Integer>
{
	public static final Logger log =Logger.getLogger(MercuryScheme.class.getName());
	
	public static final String DATABASE_NAME					= "contextGUIDDB";
	
	public static final String TABLE_NAME						= "guidRecTable";
	
	public static final int MONGO_PORT_NUM						= 27017;
	
	//FIXME: sourceID is not properly set, it is currently set to sourceID of each node,
	// it needs to be set to the origin sourceID.
	// Any id-based communication requires NodeConfig and Messenger
	public static final int NUM_PARALLEL_CLIENTS				= 9;
	
	//public static final String[] mongoSHosts = {"compute-0-14", "compute-0-15", "compute-0-16", "compute-0-17", "compute-0-18",
	//		"compute-0-19", "compute-0-20", "compute-0-21", "compute-0-22"};
	
	public static final String GUID_COLUMN_NAME					= "guid";
	
	//private MongoClient mongo										= null;
	
	private final MongoClient[] mongoClientArray					= new MongoClient[NUM_PARALLEL_CLIENTS];
	//private DB[] nodeDB												= new DB[NUM_PARALLEL_CLIENTS];
	
	private final ConcurrentLinkedQueue<MongoClient> freeMClientQueue;
	
	private final Object mclientFreeMonitor						= new Object();
	
	//private ConcurrentHashMap<Long, RecordReadStorage<Integer>> pendingRecordReadReqs;
	
	//private long recordGetNum											= 0;
	//private Object recordGetNumMonitor								= new Object();
	
	public MercurySchemeConsistent(InterfaceNodeConfig<Integer> nc, JSONMessenger<Integer> m)
	{
		super(nc, m);
		//recordGetNum = 0;
		//pendingRecordReadReqs = new ConcurrentHashMap<Long, RecordReadStorage<Integer>>();
		//readHyperdexClient  = new Client(HYPERDEX_IP_ADDRESS, HYPERDEX_PORT);
		//writeHyperdexClient = new Client(HYPERDEX_IP_ADDRESS, HYPERDEX_PORT);
		
		freeMClientQueue = new ConcurrentLinkedQueue<MongoClient>();
		
		for(int i=0;i<NUM_PARALLEL_CLIENTS;i++)
		{	
			try 
			{
				mongoClientArray[i] = new MongoClient("localhost", MercurySchemeConsistent.MONGO_PORT_NUM);
				//mongoClientArray[i] = new MongoClient(mongoSHosts[i], MercurySchemeConsistent.MONGO_PORT_NUM);
				
				/**** Get database ****/
				// if database doesn't exists, MongoDB will create it for you
				//this.nodeDB[i] = mongoClientArray[i].getDB(MercurySchemeConsistent.DATABASE_NAME);
				//DBCollection table = nodeDB.getCollection("user");
				freeMClientQueue.add(mongoClientArray[i]);
				//freeHClientQueue.add(hyperdexClientArray[i]);
			} catch (UnknownHostException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public GenericMessagingTask<Integer,?>[] handleMetadataMsgToValuenode(
		ProtocolEvent<ContextServicePacket.PacketType, String> event,
		ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		@SuppressWarnings("unchecked")
		MetadataMsgToValuenode<Integer> metaMsgToValnode = (MetadataMsgToValuenode<Integer>) event;
		// just need to store the val node info in the local storage
		
		String attrName = metaMsgToValnode.getAttrName();
		double rangeStart = metaMsgToValnode.getRangeStart();
		double rangeEnd = metaMsgToValnode.getRangeEnd();
		
		ContextServiceLogger.getLogger().info("METADATA_MSG recvd at node " + 
				this.getMyID()+" attriName "+attrName + 
				" rangeStart "+rangeStart+" rangeEnd "+rangeEnd);
		
		
		ValueInfoObjectRecord<Double> valInfoObjRec = new ValueInfoObjectRecord<Double>
												(rangeStart, rangeEnd, new JSONArray());
		
		this.contextserviceDB.putValueObjectRecord(valInfoObjRec, attrName);
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleQueryMsgFromUser(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * - send it to query processing system, where it parses it
		 * and sends it to corresponding metadata nodes
		 */
		@SuppressWarnings("unchecked")
		QueryMsgFromUser<Integer> queryMsgFromUser = (QueryMsgFromUser<Integer>)event;
		
		GenericMessagingTask<Integer, QueryMsgToMetadataNode<Integer>>[] retMsgs =
				processQueryMsgFromUser(queryMsgFromUser);
		
		synchronized(this.numMesgLock)
		{
			if(retMsgs != null)
			{
				this.numMessagesInSystem += retMsgs.length;
			}
		}
		return retMsgs;
	}
	
	public GenericMessagingTask<Integer,?>[] handleQueryMsgToMetadataNode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * - parse the Query and send QueryMsgToValuenode to all value nodes
		 * involved for the query
		 */
		@SuppressWarnings("unchecked")
		QueryMsgToMetadataNode<Integer> queryMsgToMetaNode = 
				(QueryMsgToMetadataNode<Integer>) event;
		
		GenericMessagingTask<Integer, QueryMsgToValuenode<Integer>>[] retMsgs
			= this.processQueryMsgToMetadataNode(queryMsgToMetaNode);
		
		synchronized(this.numMesgLock)
		{
			if( retMsgs != null )
			{
				this.numMessagesInSystem+=retMsgs.length;
			}
		}
		return retMsgs;
	}
	
	public GenericMessagingTask<Integer,?>[] handleQueryMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * - contacts the local information and sends back the 
		 * QueryMsgToValuenodeReply
		 */
		@SuppressWarnings("unchecked")
		QueryMsgToValuenode<Integer> queryMsgToValnode = 
				(QueryMsgToValuenode<Integer>)event;
		
		GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>>[] retMsgs
			= this.processQueryMsgToValuenode(queryMsgToValnode);
		
		/*synchronized(this.numMesgLock)
		{
			if(retMsgs != null)
			{
				this.numMessagesInSystem += retMsgs.length;
			}
		}*/
		return retMsgs;
	}
	
	public GenericMessagingTask<Integer,?>[] handleQueryMsgToValuenodeReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		@SuppressWarnings("unchecked")
		QueryMsgToValuenodeReply<Integer> queryMsgToValnodeReply = 
				(QueryMsgToValuenodeReply<Integer>)event;
		
		ContextServiceLogger.getLogger().info("Recvd QueryMsgToValuenodeReply at " 
				+ this.getMyID() +" reply "+queryMsgToValnodeReply.toString());
		
		try
		{
			addQueryReply(queryMsgToValnodeReply);
		} catch ( JSONException e )
		{
			e.printStackTrace();
		}
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateMsgToMetadataNode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * - send the update message to the responsible value node
		 */
		@SuppressWarnings("unchecked")
		ValueUpdateMsgToMetadataNode<Integer> valUpdateMsgToMetaNode 
					= (ValueUpdateMsgToMetadataNode<Integer>)event;
		
		GenericMessagingTask<Integer, ?>[] retMsgs
			= this.processValueUpdateMsgToMetadataNode(valUpdateMsgToMetaNode);
		
		synchronized(this.numMesgLock)
		{
			if(retMsgs != null)
			{
				this.numMessagesInSystem+=retMsgs.length;
			}
		}
		return retMsgs;
	}
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * just update / add or remove the entry
		 */
		@SuppressWarnings("unchecked")
		ValueUpdateMsgToValuenode<Integer> valUpdMsgToValnode = (ValueUpdateMsgToValuenode<Integer>)event;
		
		GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>[] retMsgs =
				this.processValueUpdateMsgToValuenode(valUpdMsgToValnode);
		
		/*synchronized(this.numMesgLock)
		{
			if(retMsgs != null)
			{
				this.numMessagesInSystem+=retMsgs.length;
			}
		}*/
		return retMsgs;
	}
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateFromGNS(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * just update / add or remove the entry
		 */
		@SuppressWarnings("unchecked")
		ValueUpdateFromGNS<Integer> valUpdMsgFromGNS = (ValueUpdateFromGNS<Integer>)event;
		
		GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>> [] retMsgs
			= this.processValueUpdateFromGNS(valUpdMsgFromGNS);
		
		synchronized(this.numMesgLock)
		{
			if(retMsgs != null)
			{
				this.numMessagesInSystem += retMsgs.length;
			}
		}
		return retMsgs;
	}
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateMsgToValuenodeReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * just update / add or remove the entry
		 */
		@SuppressWarnings("unchecked")
		ValueUpdateMsgToValuenodeReply<Integer> valUpdMsgToValnode 
							= (ValueUpdateMsgToValuenodeReply<Integer>)event;
		
		this.processValueUpdateMsgToValuenodeReply(valUpdMsgToValnode);
		return null;
	}
	
	/*public GenericMessagingTask<Integer,?>[] handleBulkPut(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		@SuppressWarnings("unchecked")
		BulkPut<Integer> bulkPutMesg = (BulkPut<Integer>) event;
		processBulkPut(bulkPutMesg);
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleBulkGet(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		@SuppressWarnings("unchecked")
		BulkGet<Integer> bulkGetMesg = (BulkGet<Integer>) event;
		return processBulkGet(bulkGetMesg);
	}
	
	public GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>> [] 
			handleBulkGetReply(ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		@SuppressWarnings("unchecked")
		BulkGetReply<Integer> bulkGetReplyMesg = (BulkGetReply<Integer>) event;
		return processBulkGetReply(bulkGetReplyMesg);
	}*/
	
	/**
	 * Takes the attribute name as input and returns the node id 
	 * that is responsible for metadata of that attribute.
	 * @param AttrName
	 * @return
	 */
	public Integer getResponsibleNodeId(String AttrName)
	{
		int numNodes = this.allNodeIDs.size();
		
		//String attributeHash = Utils.getSHA1(attributeName);
		int mapIndex = Hashing.consistentHash(AttrName.hashCode(), numNodes);
		@SuppressWarnings("unchecked")
		Integer[] allNodeIDArr = (Integer[]) this.allNodeIDs.toArray();
		return allNodeIDArr[mapIndex];
	}
	
	@SuppressWarnings("unchecked")
	public GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] initializeScheme()
	{
		LinkedList<GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>> messageList = 
				new  LinkedList<GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>>();
		
		Vector<String> attributes = AttributeTypes.getAllAttributes();
		
		for(int i=0;i<attributes.size(); i++)
		{
			String currAttName = attributes.get(i);
			//ContextServiceLogger.getLogger().fine("initializeMetadataObjects currAttName "+currAttName);
			//String attributeHash = Utils.getSHA1(attributeName);
			Integer respNodeId = getResponsibleNodeId(currAttName);
			//ContextServiceLogger.getLogger().fine("InitializeMetadataObjects currAttName "+currAttName
			//		+" respNodeID "+respNodeId);
			// This node is responsible(meta data)for this Att.
			if(respNodeId == getMyID() )
			{
				ContextServiceLogger.getLogger().info("Node Id "+getMyID() +
						" meta data node for attribute "+currAttName);
				// FIXME: set proper min max value, probably read attribute names and its min max value from file.
				//AttributeMetadataInformation<Integer> attrMeta = 
				//		new AttributeMetadataInformation<Integer>(currAttName, AttributeTypes.MIN_VALUE, 
				//				AttributeTypes.MAX_VALUE, csNode);
				
				AttributeMetadataInfoRecord<Integer, Double> attrMetaRec =
						new AttributeMetadataInfoRecord<Integer, Double>
				(currAttName, AttributeTypes.MIN_VALUE, AttributeTypes.MAX_VALUE);
				
				getContextServiceDB().putAttributeMetaInfoRecord(attrMetaRec);
				
				//csNode.addMetadataInfoRec(attrMetaRec);
				//;addMetadataList(attrMeta);
				//GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] messageTasks = 
				//		attrMeta.assignValueRanges(csNode.getMyID());
				
				GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] messageTasks 
						= assignValueRanges(getMyID(), attrMetaRec);
				
				// add all the messaging tasks at different value nodes
				for(int j=0;j<messageTasks.length;j++)
				{
					messageList.add(messageTasks[j]);
				}
			}
		}
		
		GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] returnArr 
					= new GenericMessagingTask[messageList.size()];
		
		for(int i=0;i<messageList.size();i++)
		{
			returnArr[i] = messageList.get(i);
		}
		return returnArr;
	}
	
	/****************************** End of protocol task handler methods *********************/
	/*********************** Private methods below **************************/
	/**
	 * Query req received here means that
	 * no group exists in the GNS
	 * @param queryMsgFromUser
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private GenericMessagingTask<Integer, QueryMsgToMetadataNode<Integer>>[] 
			processQueryMsgFromUser(QueryMsgFromUser<Integer> queryMsgFromUser)
	{
		LinkedList<GenericMessagingTask<Integer, QueryMsgToMetadataNode<Integer>>> messageList = 
				new  LinkedList<GenericMessagingTask<Integer, QueryMsgToMetadataNode<Integer>>>();
		
		String query = queryMsgFromUser.getQuery();
		long userReqID = queryMsgFromUser.getUserReqNum();
		String userIP = queryMsgFromUser.getSourceIP();
		int userPort = queryMsgFromUser.getSourcePort();
		
		ContextServiceLogger.getLogger().fine("QUERY RECVD QUERY_MSG recvd query recvd "+query);
		
		// create the empty group in GNS
		String grpGUID = GNSCalls.createQueryGroup(query);
		
		Vector<QueryComponent> qcomponents = QueryParser.parseQuery(query);
		
		QueryInfo<Integer> currReq = new QueryInfo<Integer>(query, getMyID(),
				 grpGUID, userReqID, userIP, userPort, qcomponents);
		
		
		synchronized(this.pendingQueryLock)
		{	
			//StartContextServiceNode.sendQueryForProcessing(qinfo);
			//currReq.setRequestId(requestIdCounter);
			//requestIdCounter++;
			
			currReq.setQueryRequestID(queryIdCounter++);
			pendingQueryRequests.put(currReq.getRequestId(), currReq);
		}
		
		if(ContextServiceConfig.EXP_PRINT_ON)
		{
			//ContextServiceLogger.getLogger().fine("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSER REQUEST ID "
			//			+currReq.getRequestId()+" NUMATTR "+qcomponents.size()+" AT "+System.currentTimeMillis()
			//			+" "+qcomponents.get(0).getAttributeName()+" QueryStart "+queryStart);
		}
		
		//for (int i=0;i<qcomponents.size();i++)
		// sending it to the first attribute, as all attributes
		// in the query will be homogeneous for now.
		if( qcomponents.size() > 0 )
		{
			Random rand = new Random();
			int qcIndex = rand.nextInt(qcomponents.size());
			QueryComponent qc = qcomponents.elementAt(qcIndex);
			
			String atrName = qc.getAttributeName();
			Integer respNodeId = getResponsibleNodeId(atrName);
			
			QueryMsgToMetadataNode<Integer> queryMsgToMetaNode = 
					new QueryMsgToMetadataNode<Integer>(getMyID(), qc, currReq.getRequestId(), 
							this.getMyID(), query, grpGUID);
			
			
			GenericMessagingTask<Integer, QueryMsgToMetadataNode<Integer>> mtask = new GenericMessagingTask<Integer, 
					QueryMsgToMetadataNode<Integer>>(respNodeId, queryMsgToMetaNode);
			
			messageList.add(mtask);
			
			ContextServiceLogger.getLogger().info("Sending predicate mesg from " 
					+ getMyID() +" to node "+respNodeId + 
					" predicate "+qc.toString());
		}
		
		return
		(GenericMessagingTask<Integer, QueryMsgToMetadataNode<Integer>>[]) this.convertLinkedListToArray(messageList);
	}
	
	/**
	 * Processes QueryMsgToMetadataNode node and
	 * sends back reply in GenericMessaging tasks
	 * QueryMsgToValuenode
	 * @throws JSONException
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	private GenericMessagingTask<Integer, QueryMsgToValuenode<Integer>>[]
			processQueryMsgToMetadataNode(QueryMsgToMetadataNode<Integer> queryMsgToMetaNode)
	{
		LinkedList<GenericMessagingTask<Integer,QueryMsgToValuenode<Integer>>> msgList
		 = new LinkedList<GenericMessagingTask<Integer,QueryMsgToValuenode<Integer>>>();
		
		
		QueryComponent qc= queryMsgToMetaNode.getQueryComponent();
		String attrName = qc.getAttributeName();
		
		ContextServiceLogger.getLogger().info("Predicate mesg recvd at" 
				+ this.getMyID() +" from node "+queryMsgToMetaNode.getSourceId() +
				" predicate "+qc.toString());
		
		List<AttributeMetaObjectRecord<Integer, Double>> attrMetaObjRecList = 
		this.contextserviceDB.getAttributeMetaObjectRecord(attrName, qc.getLeftValue(), qc.getRightValue());
		
		for( int i=0; i<attrMetaObjRecList.size(); i++ )
		{
			//AttributeMetadataObject<Integer> currObj = resultList.get(j);
			AttributeMetaObjectRecord<Integer, Double> currObj = 
													attrMetaObjRecList.get(i);
		
			if( ContextServiceConfig.GROUP_INFO_COMPONENT )
			{
				GroupGUIDRecord groupGUIDRec = new GroupGUIDRecord(queryMsgToMetaNode.getGroupGUID(),
						queryMsgToMetaNode.getQuery());
				
				try
				{
					JSONObject toJSON = groupGUIDRec.toJSONObject();
					
					//FIXME: synchronization needed
					// update groupGUID in the relevant value partitions
					this.contextserviceDB.updateAttributeMetaObjectRecord(currObj, attrName, 
							toJSON, AttributeMetaObjectRecord.Operations.APPEND, 
							AttributeMetaObjectRecord.Keys.GROUP_GUID_LIST);
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
			}
			
			QueryMsgToValuenode<Integer> queryMsgToValnode 
				= new QueryMsgToValuenode<Integer>(getMyID(), qc,
					queryMsgToMetaNode.getRequestId(), queryMsgToMetaNode.getSourceId(),
					queryMsgToMetaNode.getQuery(), queryMsgToMetaNode.getGroupGUID(), attrMetaObjRecList.size() );
			
			ContextServiceLogger.getLogger().info("Sending ValueNodeMessage from" 
					+ this.getMyID() +" to node "+currObj.getNodeID() + 
					" predicate "+qc.toString());
			
			GenericMessagingTask<Integer, QueryMsgToValuenode<Integer>> mtask = 
			new GenericMessagingTask<Integer, QueryMsgToValuenode<Integer>>(currObj.getNodeID(), queryMsgToValnode);
			//relaying the query to the value nodes of the attribute
			msgList.add(mtask);
		}
		return (GenericMessagingTask<Integer, QueryMsgToValuenode<Integer>>[]) this.convertLinkedListToArray(msgList);
	}
	
	/**
	 * Processes the QueryMsgToValuenode and replies with 
	 * QueryMsgToValuenodeReply, which contains the GUIDs
	 */
	@SuppressWarnings("unchecked")
	private GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>> []
			processQueryMsgToValuenode(QueryMsgToValuenode<Integer> queryMsgToValnode)
	{
		ContextServiceLogger.getLogger().info("QueryMsgToValuenode recvd at " 
				+ this.getMyID() +" from node "+queryMsgToValnode.getSourceId() );
		
		String query = queryMsgToValnode.getQuery();
		
		QueryComponent valueNodePredicate = queryMsgToValnode.getQueryComponent();
		
		//LinkedList<String> resultGUIDs = new LinkedList<String>();
		
		List<ValueInfoObjectRecord<Double>> valInfoObjRecList = 
					this.contextserviceDB.getValueInfoObjectRecord
						(valueNodePredicate.getAttributeName(), valueNodePredicate.getLeftValue(), 
								valueNodePredicate.getRightValue());
		
		//JSONArray getGUIDRecords = new JSONArray();
		
		List<String> getGUIDRecords = new ArrayList<String>();
		
		for(int j=0; j<valInfoObjRecList.size(); j++)
		{
			ValueInfoObjectRecord<Double> valueObjRec = valInfoObjRecList.get(j);
			
			JSONArray nodeGUIDList = valueObjRec.getNodeGUIDList();
			
			for(int k=0;k<nodeGUIDList.length();k++)
			{
				try
				{
					JSONObject nodeGUIDJSON = nodeGUIDList.getJSONObject(k);
					NodeGUIDInfoRecord<Double> nodeGUIDRec = 
							new NodeGUIDInfoRecord<Double>(nodeGUIDJSON);
					
					//getGUIDRecords.put( nodeGUIDRec.getNodeGUID() );
					getGUIDRecords.add( nodeGUIDRec.getNodeGUID() );
				}
				catch( JSONException jso )
				{
					jso.printStackTrace();
				}
			}
		}
		
		JSONArray resultArray = mongoBulkGet(getGUIDRecords, query);
		
		LinkedList<GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>>> msgList
		 = new LinkedList<GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>>>();
   
   
		QueryMsgToValuenodeReply<Integer> queryMsgToValReply 
			= new QueryMsgToValuenodeReply<Integer>(getMyID(), resultArray, 
					queryMsgToValnode.getRequestId(), 0, getMyID(), 
					queryMsgToValnode.getNumValNodesContacted());

		GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>> mtask = 
				new GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>>
					(queryMsgToValnode.getSourceId(), queryMsgToValReply);
		//relaying the query to the value nodes of the attribute
		
		msgList.add(mtask);
		ContextServiceLogger.getLogger().info("Sending QueryMsgToValuenodeReply from " 
				+ this.getMyID() +" to node "+queryMsgToValnode.getSourceId() +
				" reply "+queryMsgToValReply.toString());
	    
		return 
				(GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>>[]) 
					this.convertLinkedListToArray(msgList);
	}
	
	private GenericMessagingTask<Integer, ?>[] 
			processValueUpdateMsgToMetadataNode(ValueUpdateMsgToMetadataNode<Integer> valUpdateMsgToMetaNode)
	{
		LinkedList<GenericMessagingTask<Integer, ?>> msgList
				= new LinkedList<GenericMessagingTask<Integer, ?>>();
		
		long versionNum = valUpdateMsgToMetaNode.getVersionNum();
		String updatedAttrName = valUpdateMsgToMetaNode.getAttrName();
		String GUID = valUpdateMsgToMetaNode.getGUID();
		double oldValue = Double.MIN_VALUE;
		double newValue = valUpdateMsgToMetaNode.getNewValue();
		JSONObject allAttrs = new JSONObject();
		//String taggedAttr = valUpdateMsgToMetaNode.getTaggedAttribute();
		//Integer sourceID = valUpdateMsgToMetaNode.getSourceID();
		long requestID = valUpdateMsgToMetaNode.getRequestID();
		
		ContextServiceLogger.getLogger().fine("processValueUpdateMsgToMetadataNode");
		
		// update in hyperdex
		//Map<String, Object> attrs = new HashMap<String, Object>();
		//attrs.put(updatedAttrName, newValue);
		//Deferred asyncPut = null;
		/*long start = System.currentTimeMillis();
		// send to consistent node.
		BulkPut<Integer> bulkPutMesg 
			= new BulkPut<Integer>(this.getMyID(), GUID, updatedAttrName, newValue);
		
		Integer respGUIDId = this.getResponsibleNodeId(GUID);
		GenericMessagingTask<Integer, BulkPut<Integer>> mtask = 
				new GenericMessagingTask<Integer, BulkPut<Integer>>
				(respGUIDId, bulkPutMesg);
				//relaying the query to the value nodes of the attribute
		msgList.add(mtask);
		long end = System.currentTimeMillis();
		ContextServiceLogger.getLogger().fine("HYPERDEX WRITE TIME "+(end-start));*/
		
		mongoUpdate(updatedAttrName, GUID, newValue);
		
		//if( updatedAttrName.equals(taggedAttr) )
		{
			//LinkedList<AttributeMetaObjectRecord<Integer, Double>> 
			// there should be just one element in the list, or definitely at least one.
			LinkedList<AttributeMetaObjectRecord<Integer, Double>> oldMetaObjRecList = 
				(LinkedList<AttributeMetaObjectRecord<Integer, Double>>) 
				this.getContextServiceDB().getAttributeMetaObjectRecord(updatedAttrName, oldValue, oldValue);
			
			AttributeMetaObjectRecord<Integer, Double> oldMetaObjRec = null;
			
			if(oldMetaObjRecList.size()>0)
			{
				oldMetaObjRec = 
						this.getContextServiceDB().getAttributeMetaObjectRecord(updatedAttrName, oldValue, oldValue).get(0);
			}
			//oldMetaObj = new AttributeMetadataObject<Integer>();
			
			// same thing for the newValue
			AttributeMetaObjectRecord<Integer, Double> newMetaObjRec = 
					this.getContextServiceDB().getAttributeMetaObjectRecord(updatedAttrName, newValue, newValue).get(0);
			
			if( ContextServiceConfig.GROUP_INFO_COMPONENT )
			{
				// do group updates for the old value
				try
				{
					if(oldMetaObjRec!=null)
					{
						LinkedList<GroupGUIDRecord> oldValueGroups = getGroupsAffectedUsingDatabase
								(oldMetaObjRec, allAttrs, updatedAttrName, oldValue);
						
						//oldMetaObj.getGroupsAffected(allAttr, updateAttrName, oldVal);
						
						GNSCalls.userGUIDAndGroupGUIDOperations
						(GUID, oldValueGroups, GNSCallsOriginal.UserGUIDOperations.REMOVE_USER_GUID_FROM_GROUP);
					}
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
				
				// do group  updates for the new value
				try
				{
					if( newMetaObjRec != null )
					{
						LinkedList<GroupGUIDRecord> newValueGroups = getGroupsAffectedUsingDatabase
								(newMetaObjRec, allAttrs, updatedAttrName, newValue);
								
								//newMetaObj.getGroupsAffected(allAttr, updateAttrName, newVal);
						GNSCalls.userGUIDAndGroupGUIDOperations
						(GUID, newValueGroups, GNSCallsOriginal.UserGUIDOperations.ADD_USER_GUID_TO_GROUP);
					} else
					{
						assert(false);
					}
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
			}
			
			// for the new value
			Integer newValueNodeId = newMetaObjRec.getNodeID();
			
			// for the old value
			Integer oldValueNodeId = newValueNodeId;
			if(oldValue != AttributeTypes.NOT_SET)
			{
				oldValueNodeId = oldMetaObjRec.getNodeID();
			}
			
			if( oldValueNodeId.equals(newValueNodeId) )
			{
				ValueUpdateMsgToValuenode<Integer> valueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<Integer>
				(this.getMyID(), versionNum, GUID, updatedAttrName, oldValue, newValue, 
						ValueUpdateMsgToValuenode.REMOVE_ADD_BOTH, requestID);
				
				GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>> mtask1 = 
						new GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>>
						(newValueNodeId, valueUpdateMsgToValnode);
						//relaying the query to the value nodes of the attribute
				msgList.add(mtask1);
				
				ContextServiceLogger.getLogger().info("Sending ValueUpdateMsgToValuenode from" 
						+ this.getMyID() + " to node "+oldValueNodeId +
						" mesg "+valueUpdateMsgToValnode);
			} else
			{
				ValueUpdateMsgToValuenode<Integer> oldValueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<Integer>
				(this.getMyID(), versionNum, GUID, updatedAttrName, oldValue, newValue, 
						ValueUpdateMsgToValuenode.REMOVE_ENTRY, requestID);
				
				GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>> oldmtask = 
						new GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>>
						(oldValueNodeId, oldValueUpdateMsgToValnode);
				
				
				ValueUpdateMsgToValuenode<Integer> newValueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<Integer>
				(this.getMyID(), versionNum, GUID, updatedAttrName, oldValue, newValue, 
						ValueUpdateMsgToValuenode.ADD_ENTRY, requestID);
				
				GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>> newmtask = 
					new GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>>
					(newValueNodeId, newValueUpdateMsgToValnode);
				
				msgList.add(oldmtask);
				msgList.add(newmtask);
				
				ContextServiceLogger.getLogger().info("Sending ValueUpdateMsgToValuenode from" 
						+ this.getMyID() + " to node "+oldValueNodeId+" "+ newValueNodeId+
						" mesg "+oldValueUpdateMsgToValnode);
			}
		}
		return 
		(GenericMessagingTask<Integer, ?>[]) this.convertLinkedListToArray(msgList);
	}
	
	/**
	 * adds the reply of the queryComponent
	 * @throws JSONException
	 */
	private GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>[]
			processValueUpdateMsgToValuenode(ValueUpdateMsgToValuenode<Integer> valUpdateMsgToValnode)
	{
		ContextServiceLogger.getLogger().info("\n\n Recvd ValueUpdateMsgToValuenode at " 
				+ this.getMyID() +" reply "+valUpdateMsgToValnode);
		
		LinkedList<GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>> msgList
			= new LinkedList<GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>>();
		
		String attrName = valUpdateMsgToValnode.getAttrName();
		String GUID = valUpdateMsgToValnode.getGUID();
		double oldValue = Double.MIN_VALUE;
		double newValue = valUpdateMsgToValnode.getNewValue();
		long versionNum = valUpdateMsgToValnode.getVersionNum();
		JSONObject allAttrs = new JSONObject();
		//Integer sourceID = valUpdateMsgToValnode.getSourceID();
		long requestID = valUpdateMsgToValnode.getRequestID();
		
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
									(GUID, newValue, versionNum, allAttrs);
							
							//valInfoObjRec.getNodeGUIDList().put(nodeGUIDInfRec);
							this.contextserviceDB.updateValueInfoObjectRecord
										(valInfoObjRec, attrName, nodeGUIDInfRec.toJSONObject(), 
										ValueInfoObjectRecord.Operations.APPEND, ValueInfoObjectRecord.Keys.NODE_GUID_LIST);
						} catch(JSONException jso)
						{
							jso.printStackTrace();
						}
					}
					
					//NodeGUIDInfo nodeGUIDObj = new NodeGUIDInfo(GUID, newValue, versionNum);
					//valueObj.addNodeGUID(nodeGUIDObj);
					// send reply back
					
//					if(oldValue == AttributeTypes.NOT_SET)
//					{
//						// first write so just 1 mesg from add
//						sendUpdateReplyBackToUser(sourceIP, sourcePort, versionNum, 1);
//					}
//					else
//					{
//						sendUpdateReplyBackToUser(sourceIP, sourcePort, versionNum, 2);
//					}
					
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
					
					ValueUpdateMsgToValuenodeReply<Integer> newValueUpdateMsgReply
						= new ValueUpdateMsgToValuenodeReply<Integer>
					(this.getMyID(), versionNum, numRep, requestID);
				
					/*GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>> newmtask 
						= new GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>
					(sourceID, newValueUpdateMsgReply);
				
					msgList.add(newmtask);*/
				
					break;
				}
				case ValueUpdateMsgToValuenode.REMOVE_ENTRY:
				{	
					List<ValueInfoObjectRecord<Double>> valueInfoObjRecList = 
							this.contextserviceDB.getValueInfoObjectRecord(attrName, newValue, newValue);
					
					if( valueInfoObjRecList.size() != 1 )
					{
						assert false;
					}
					else
					{
						try
						{
							ValueInfoObjectRecord<Double> valInfoObjRec = valueInfoObjRecList.get(0);
							
							NodeGUIDInfoRecord<Double> nodeGUIDInfRec = new NodeGUIDInfoRecord<Double>
									(GUID, newValue, versionNum, allAttrs);
							
							//valInfoObjRec.getNodeGUIDList().put(nodeGUIDInfRec);
							this.contextserviceDB.updateValueInfoObjectRecord
										(valInfoObjRec, attrName, nodeGUIDInfRec.toJSONObject(), 
										ValueInfoObjectRecord.Operations.REMOVE, ValueInfoObjectRecord.Keys.NODE_GUID_LIST);
						} catch(JSONException jso)
						{
							jso.printStackTrace();
						}
					}
					
					// send reply back
					//sendUpdateReplyBackToUser(sourceIP, sourcePort, versionNum, 2);
					// from add and remove so 2 replies
					ValueUpdateMsgToValuenodeReply<Integer> newValueUpdateMsgReply
						= new ValueUpdateMsgToValuenodeReply<Integer>
					(this.getMyID(), versionNum, 2, requestID);
					
					/*GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>> newmtask 
						= new GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>
							(sourceID, newValueUpdateMsgReply);
					
					msgList.add(newmtask);*/
					break;
				}
				case ValueUpdateMsgToValuenode.REMOVE_ADD_BOTH:
				{
					//FIXME: may need atomicity here
					// just a value update, but goes to the same node
					//remove
					/*valueObj.removeNodeGUID(GUID);
					
					// and add
					NodeGUIDInfo nodeGUIDObj = new NodeGUIDInfo(GUID, newValue, versionNum);
					valueObj.addNodeGUID(nodeGUIDObj);*/
					
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
									(GUID, newValue, versionNum, allAttrs);
								
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
					
					//send reply back
					//sendUpdateReplyBackToUser(sourceIP, sourcePort, versionNum, 1);
					// just 1 reply goes, as add and remove happens on this node
					ValueUpdateMsgToValuenodeReply<Integer> newValueUpdateMsgReply
						= new ValueUpdateMsgToValuenodeReply<Integer>
					(this.getMyID(), versionNum, 1, requestID);
		
					/*GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>> newmtask 
						= new GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>
							(sourceID, newValueUpdateMsgReply);
					msgList.add(newmtask);*/
					break;
				}
			}
		}
		return 
				(GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>[]) 
					this.convertLinkedListToArray(msgList);
	}
	
	/**
	 * adds the reply of the queryComponent
	 * @throws JSONException
	 */
	@SuppressWarnings("unchecked")
	private GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>> []
	                   processValueUpdateFromGNS(ValueUpdateFromGNS<Integer> valUpdMsgFromGNS)
	{
		LinkedList<GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>>> msgList
			= new LinkedList<GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>>>();
		
		//ContextServiceLogger.getLogger().fine("\n\n Recvd ValueUpdateFromGNS at " 
		//		+ this.getMyID() +" reply "+valUpdMsgFromGNS);
		
		long versionNum = valUpdMsgFromGNS.getVersionNum();
		String GUID = valUpdMsgFromGNS.getGUID();
		String attrName = "";
		String oldVal   = "";
		String newVal   = "";
		JSONObject allAttrs = new JSONObject();
		
		double oldValD, newValD;
		
		if( oldVal.equals("") )
		{
			oldValD = AttributeTypes.NOT_SET;
		} else
		{
			oldValD = Double.parseDouble(oldVal);
		}
		newValD = Double.parseDouble(newVal );
		
		/*long currReqID = -1;
		
		synchronized(this.pendingUpdateLock)
		{
			UpdateInfo<Integer> currReq 
				= new UpdateInfo<Integer>(valUpdMsgFromGNS, updateIdCounter++);
			currReqID = currReq.getRequestId();
			pendingUpdateRequests.put(currReqID, currReq);
		}*/
		
		// update in mongodb here itself, so metadata node doesn't become a bottleneck.
		// mongo is crashing by multiple concurrent writes, 
		// that's the state of state of the art key value systems :D.
		// updating at metadata node now
		//mongoUpdate(attrName, GUID, newValD);
		
//		ValueUpdateMsgToMetadataNode<Integer> valueUpdMsgToMetanode = 
//			new ValueUpdateMsgToMetadataNode<Integer>(this.getMyID(), versionNum, GUID, attrName, oldValD, 
//					newValD, allAttrs);
//	
//		Integer respMetadataNodeId = this.getResponsibleNodeId(attrName);
//		//nioTransport.sendToID(respMetadataNodeId, valueMeta.getJSONMessage());
//	
//		GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>> mtask = 
//			new GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>>(respMetadataNodeId, 
//					valueUpdMsgToMetanode);
//		
//		GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>> [] returnTaskArr = 
//			new GenericMessagingTask[1];
		
		//Iterator<String> iter = allAttrs.keys();
		//while(iter.hasNext())
		//{
		//String attrNameKey = iter.next();
		
		//String valueString = allAttrs.getString(attrNameKey);
		
		/*ValueUpdateMsgToMetadataNode<Integer> valueUpdMsgToMetanode = 
				new ValueUpdateMsgToMetadataNode<Integer>(this.getMyID(), versionNum, GUID, attrName, 
						newValD, this.getMyID(), currReqID);
		
		Integer respMetadataNodeId = this.getResponsibleNodeId(attrName);
		
		GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>> mtask = 
			new GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>>(respMetadataNodeId, 
						valueUpdMsgToMetanode);
		
		msgList.add(mtask);*/
			//GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>> [] returnTaskArr = 
			//	new GenericMessagingTask[1];
		//}
		return
		(GenericMessagingTask<Integer, ValueUpdateMsgToMetadataNode<Integer>>[]) this.convertLinkedListToArray(msgList);
	}
	
	private void 
		processValueUpdateMsgToValuenodeReply(ValueUpdateMsgToValuenodeReply<Integer> valUpdateMsgToValnodeRep)
	{
		long requestId =  valUpdateMsgToValnodeRep.getRequestID();
		/*UpdateInfo<Integer> updateInfo = pendingUpdateRequests.get(requestId);
		
		if( updateInfo != null )
		{
			updateInfo.incrementNumReplyRecvd();
			//ContextServiceLogger.getLogger().fine("processValueUpdateMsgToValuenodeReply numReplyRecvd "+updateInfo.getNumReplyRecvd() 
			//		+" NumReply "+valUpdateMsgToValnodeRep.getNumReply() );
			if( updateInfo.getNumReplyRecvd() == valUpdateMsgToValnodeRep.getNumReply() )
			{
				synchronized( this.pendingUpdateLock )
				{
					if( this.pendingUpdateRequests.get(updateInfo.getRequestId())  != null )
					{
						sendUpdateReplyBackToUser(updateInfo.getValueUpdateFromGNS().getSourceIP(), 
							updateInfo.getValueUpdateFromGNS().getSourcePort(), updateInfo.getValueUpdateFromGNS().getVersionNum() );
					}
					pendingUpdateRequests.remove(requestId);
				}
			}
		}*/
		//ContextServiceLogger.getLogger().fine("componentReplies.size() "+componentReplies.size() +
		//		" queryComponents.size() "+queryComponents.size());
		// if there is at least one replies recvd for each component
	}
	
	private LinkedList<GroupGUIDRecord> getGroupsAffectedUsingDatabase
		(AttributeMetaObjectRecord<Integer, Double> metaObjRec, JSONObject allAttr, 
			String updateAttrName, double attrVal) throws JSONException
	{
		LinkedList<GroupGUIDRecord> satisfyingGroups = new LinkedList<GroupGUIDRecord>();
		JSONArray groupGUIDList = metaObjRec.getGroupGUIDList();
		
		for (int i=0;i<groupGUIDList.length();i++)
		{
			JSONObject groupGUIDJSON = groupGUIDList.getJSONObject(i);
			
			GroupGUIDRecord groupGUIDRec = new GroupGUIDRecord(groupGUIDJSON);
			
			//this.getContextServiceDB().getGroupGUIDRecord(groupGUID);
			
			boolean groupCheck = Utils.groupMemberCheck(allAttr, updateAttrName, 
					attrVal, groupGUIDRec.getGroupQuery());
			
			if(groupCheck)
			{
				//GroupGUIDInfo guidInfo = new GroupGUIDInfo(groupGUID, groupGUIDRec.getGroupQuery());
				satisfyingGroups.add(groupGUIDRec);
			}
		}
		return satisfyingGroups;
	}
	
	/**
	 * stores the replies for different query components, until all of the query 
	 * component replies arrive and they are returned to the user/application.
	 * @param valueReply
	 * @throws JSONException 
	 */
	private void addQueryReply(QueryMsgToValuenodeReply<Integer> queryMsgToValnodeRep) 
			throws JSONException
	{
		long requestId =  queryMsgToValnodeRep.getRequestID();
		QueryInfo<Integer> queryInfo = pendingQueryRequests.get(requestId);
		
		if(queryInfo != null)
		{
			processReplyInternally(queryMsgToValnodeRep, queryInfo);
		}
	}
	
	public void checkQueryCompletion(QueryInfo<Integer> qinfo)
	{
		// there is only one component
		if( qinfo.componentReplies.size() == 1 )
		{
			// check if all the replies have been received by the value nodes
			if(checkIfAllRepliesRecvd(qinfo))
			{
				//ContextServiceLogger.getLogger().fine("\n\n All replies recvd for each component\n\n");
				LinkedList<LinkedList<String>> doConjuc = new LinkedList<LinkedList<String>>();
				doConjuc.addAll(qinfo.componentReplies.values());
				JSONArray queryAnswer = Utils.doConjuction(doConjuc);
				//ContextServiceLogger.getLogger().fine("\n\nQuery Answer "+queryAnswer);
				
				//FIXME: uncomment this, just for debugging
				GNSCalls.addGUIDsToGroup( Utils.doConjuction(doConjuc), qinfo.getQuery(), qinfo.getGroupGUID() );
				
				/*if(ContextServiceConfig.EXP_PRINT_ON)
				{
					ContextServiceLogger.getLogger().fine("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSERREPLY REQUEST ID "
								+qinfo.getRequestId()+" NUMATTR "+qinfo.queryComponents.size()+" AT "+qprocessingTime+" EndTime "
							+queryEndTime+ " QUERY ANSWER "+queryAnswer);
				}*/
				
				
				synchronized(this.pendingQueryLock)
				{
					if( this.pendingQueryRequests.get(qinfo.getRequestId())  != null )
					{
						sendReplyBackToUser(qinfo, queryAnswer);
					}
					this.pendingQueryRequests.remove(qinfo.getRequestId());
				}
			}
		}
	}
	
	private boolean checkIfAllRepliesRecvd(QueryInfo<Integer> qinfo)
	{
		boolean resultRet = true;
		for(int i=0;i<qinfo.queryComponents.size();i++)
		{
			QueryComponent qc = qinfo.queryComponents.get(i);
			if( qc.getNumCompReplyRecvd() != qc.getTotalCompReply() )
			{
				resultRet = false;
				return resultRet;
			}
		}
		return resultRet;
	}
	
	/**
	 * Function stays here, it will be moved to value partitioner package
	 * whenever that package is decided upon.
	 * Uniformly assigns the value ranges to the nodes in
	 * the system for the given attribute.
	 */
	private GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] 
			assignValueRanges(Integer initiator, AttributeMetadataInfoRecord<Integer, Double> attrMetaRec)
	{
		//int numValueNodes = this.getAllNodeIDs().size();
		int numValueNodes = 1;
		@SuppressWarnings("unchecked")
		GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] mesgArray 
								= new GenericMessagingTask[numValueNodes];
		
		Set<Integer> allNodeIDs = this.getAllNodeIDs();
		
		int numNodes = allNodeIDs.size();
		
		double attributeMin = attrMetaRec.getAttrMin();
		double attributeMax = attrMetaRec.getAttrMax();
		
		
		//String attributeHash = Utils.getSHA1(attributeName);
		int mapIndex = Hashing.consistentHash(attrMetaRec.getAttrName().hashCode(), numNodes);
			
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
			Integer[] allNodeIDArr = (Integer[]) allNodeIDs.toArray();
			
			Integer currNodeID = (Integer)allNodeIDArr[currIndex];
			
			//AttributeMetadataObject<Integer> attObject = new AttributeMetadataObject<Integer>( currMinRange, 
			//		currMaxRange, currNodeID );
			
			//if( ContextServiceConfig.CACHE_ON )
			//{
			//	metadataInformation.add(attObject);
			//}
			//else
			{
				// add this to database, not to memory
				AttributeMetaObjectRecord<Integer, Double> attrMetaObjRec = new
				AttributeMetaObjectRecord<Integer, Double>(currMinRange, currMaxRange,
						currNodeID, new JSONArray());
				
				this.getContextServiceDB().putAttributeMetaObjectRecord(attrMetaObjRec, attrMetaRec.getAttrName());
			}
			
			MetadataMsgToValuenode<Integer> metaMsgToValnode = new MetadataMsgToValuenode<Integer>
							( initiator, attrMetaRec.getAttrName(), currMinRange, currMaxRange);
			
			GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>> mtask = new GenericMessagingTask<Integer, 
					MetadataMsgToValuenode<Integer>>((Integer) currNodeID, metaMsgToValnode);
			
			mesgArray[i] = mtask;
			
			ContextServiceLogger.getLogger().info("csID "+getMyID()+" Metadata Message attribute "+
			attrMetaRec.getAttrName()+"dest "+currNodeID+" min range "+currMinRange+" max range "+currMaxRange);
			
			//JSONObject metadataJSON = metadata.getJSONMessage();
			//ContextServiceLogger.getLogger().info("Metadata Message attribute "+attributeName+
			//		"dest "+currNodeID+" min range "+currMinRange+" max range "+currMaxRange);
			// sending the message
			//StartContextServiceNode.sendToNIOTransport(currNodeID, metadataJSON);
		}
		return mesgArray;
	}
	
	protected void processReplyInternally
	(QueryMsgToValuenodeReply<Integer> queryMsgToValnodeRep, QueryInfo<Integer> queryInfo) 
	{
		// only one component with id 0
		
		int compId = queryMsgToValnodeRep.getComponentID();
		ContextServiceLogger.getLogger().fine("processReplyInternally "+compId);
		LinkedList<String> GUIDs = queryInfo.componentReplies.get(compId);
		if(GUIDs == null)
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
		//ContextServiceLogger.getLogger().fine("componentReplies.size() "+componentReplies.size() +
		//		" queryComponents.size() "+queryComponents.size());
		// if there is at least one replies recvd for each component
	}
	
	private void updateNumberOfRepliesRecvd
		(QueryMsgToValuenodeReply<Integer> queryMsgToValnodeRep, QueryInfo<Integer> queryInfo)
	{
		for(int i=0;i<queryInfo.queryComponents.size();i++)
		{
			QueryComponent qc = queryInfo.queryComponents.get(i);
			if(qc.getComponentID() == queryMsgToValnodeRep.getComponentID())
			{
				qc.updateNumCompReplyRecvd();
				qc.setTotalCompReply(queryMsgToValnodeRep.getNumValNodesContacted());
			}
		}
	}
	
	private JSONObject convertMapIntoJSONObject(Map<String, Object> inputMap)
	{
		Iterator<String> keyIter = inputMap.keySet().iterator();
		
		
		JSONObject toReturnJSON = new JSONObject();
		
		while( keyIter.hasNext() )
		{
			String key = keyIter.next();
			String value = inputMap.get(key).toString();
			
			try
			{
				toReturnJSON.put(key, value);
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		return toReturnJSON;
	}
	
	private JSONArray mongoBulkGet(List<String> guidList, String groupQuery)
	{
		MongoClient MClientFree = null;
		
		JSONArray resultReply = new JSONArray();
		
		while( MClientFree == null )
		{
			MClientFree = freeMClientQueue.poll();
			
			if( MClientFree == null )
			{
				synchronized(mclientFreeMonitor)
				{
					try
					{
						mclientFreeMonitor.wait();
					} catch (InterruptedException e)
					{
						e.printStackTrace();
					}
				}
			}
		}
		
		Vector<QueryComponent> groupQC = QueryParser.parseQuery(groupQuery);
		
		List<BasicDBObject> queryObj = new ArrayList<BasicDBObject>();
		
		BasicDBObject guidQuery = new BasicDBObject();
		//List<String> list = new ArrayList<String>();
		//list.add(2);
		//list.add(4);
		//list.add(5);
		guidQuery.put(MercurySchemeConsistent.GUID_COLUMN_NAME, new BasicDBObject("$in", guidList));
		queryObj.add(guidQuery);
		
		for(int i=0;i<groupQC.size();i++)
		{
			QueryComponent qc = groupQC.get(i);
			BasicDBObject attrQuery = new BasicDBObject();
			attrQuery.put(qc.getAttributeName(), new BasicDBObject("$gt", qc.getLeftValue()).append("$lt", qc.getRightValue()));
			queryObj.add(attrQuery);
		}
		
		BasicDBObject andQuery = new BasicDBObject();
		andQuery.put("$and", queryObj);
		
		DB db = MClientFree.getDB(MercurySchemeConsistent.DATABASE_NAME);
		DBCollection collection = db.getCollection(MercurySchemeConsistent.TABLE_NAME);
		
		// add projection at the right time
		BasicDBObject fields = new BasicDBObject();
		fields.put(MercurySchemeConsistent.GUID_COLUMN_NAME, 1);
		
		long t0= System.currentTimeMillis();
		DBCursor cursor4 = collection.find(andQuery, fields);
		DelayProfiler.updateDelay("mongoBulkGet", t0);
		ContextServiceLogger.getLogger().fine(DelayProfiler.getStats());
		
		while (cursor4.hasNext())
		{
			String readRec = cursor4.next().toString();
			try
			{
				JSONObject readJSON = new JSONObject(readRec);
				
				// adding the guids directly.
				resultReply.put(readJSON.getString(MercurySchemeConsistent.GUID_COLUMN_NAME)); 
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		//ContextServiceLogger.getLogger().fine();
		
		synchronized(mclientFreeMonitor)
		{
			freeMClientQueue.add(MClientFree);
			mclientFreeMonitor.notifyAll();
		}
		
		return resultReply;
	}
	
	private void mongoUpdate(String attribute, String GUID, double value)
	{
		MongoClient MClientFree = null;
		
		while( MClientFree == null )
		{
			MClientFree = freeMClientQueue.poll();
			
			if( MClientFree == null )
			{
				synchronized(mclientFreeMonitor)
				{
					try
					{
						mclientFreeMonitor.wait();
					} catch (InterruptedException e)
					{
						e.printStackTrace();
					}
				}
			}
		}
		
		DB db = MClientFree.getDB(MercurySchemeConsistent.DATABASE_NAME);
		DBCollection collection = db.getCollection(MercurySchemeConsistent.TABLE_NAME);
		
		
		BasicDBObject newDocument = new BasicDBObject();
		newDocument.append("$set", new BasicDBObject().append(attribute, value));
	 
		BasicDBObject searchQuery = new BasicDBObject().append(MercurySchemeConsistent.GUID_COLUMN_NAME, GUID);
	 
		WriteResult writeRes = collection.update(searchQuery, newDocument);
		
		// means update failed need to insert
		if( writeRes.getN() <= 0 )
		{
			BasicDBObject document = new BasicDBObject();
			document.put(MercurySchemeConsistent.GUID_COLUMN_NAME, GUID);
			document.put(attribute, value);
		 
			collection.insert(document);
		}
		
		synchronized(mclientFreeMonitor)
		{
			freeMClientQueue.add(MClientFree);
			mclientFreeMonitor.notifyAll();
		}
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleBulkGet(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleBulkGetReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleConsistentStoragePut(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleConsistentStoragePutReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryMesgToSubspaceRegion(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryMesgToSubspaceRegionReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleValueUpdateToSubspaceRegionMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleGetMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleGetReplyMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleValueUpdateToSubspaceRegionReplyMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleQueryTriggerMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleUpdateTriggerMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleUpdateTriggerReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}
	
	// just to store GUID as well. gObject retrieved
	// from get is not giving key
	/*private class DeferredStorage
	{
		public String GUID;
		public Deferred defObject;
	}*/
	
	/*public void sendNotifications(LinkedList<GroupGUIDRecord> groupLists)
	{
		for(int i=0;i<groupLists.size();i++)
		{
			GroupGUIDRecord curr = groupLists.get(i);
			JSONArray arr = GNSCalls.getNotificationSetOfAGroup(curr.getGroupQuery());
			for(int j=0;j<arr.length();j++)
			{
				try
				{
					String ipport = arr.getString(j);
					sendNotification(ipport);
				} catch (JSONException e)
				{
					e.printStackTrace();
				} catch (NumberFormatException e)
				{
					e.printStackTrace();
				} catch (IOException e)
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	private void sendNotification(String ipPort) throws NumberFormatException, IOException
	{
		String [] parsed = ipPort.split(":");
		byte[] send_data = new byte[1024]; 
		send_data = new String("REFRESH").getBytes();
        DatagramPacket send_packet = new DatagramPacket(send_data, send_data.length, 
                                                        InetAddress.getByName(parsed[0]), Integer.parseInt(parsed[1]));
        client_socket.send(send_packet);
	}*/
	
	/*private GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>> []
	processBulkGetReply( BulkGetReply<Integer> bulkGetReplyMesg )
{
long bulkGetID = bulkGetReplyMesg.getReqID();

RecordReadStorage<Integer> recReadSto 
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
		
		LinkedList<GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>>> msgList
		 = new LinkedList<GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>>>();
    
    
		QueryMsgToValuenodeReply<Integer> queryMsgToValReply 
			= new QueryMsgToValuenodeReply<Integer>(getMyID(), resultArray, 
					recReadSto.getQueryMsgToValnode().getRequestId(), 0, getMyID(), 
					recReadSto.getQueryMsgToValnode().getNumValNodesContacted());

		GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>> mtask = 
				new GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>>
					(recReadSto.getQueryMsgToValnode().getSourceId(), queryMsgToValReply);
		//relaying the query to the value nodes of the attribute

		msgList.add(mtask);
		ContextServiceLogger.getLogger().info("Sending QueryMsgToValuenodeReply from " 
				+ this.getMyID() +" to node "+recReadSto.getQueryMsgToValnode().getSourceId() +
				" reply "+queryMsgToValReply.toString());
		
		this.pendingRecordReadReqs.remove(bulkGetID);
		
		return 
			(GenericMessagingTask<Integer, QueryMsgToValuenodeReply<Integer>>[]) 
				this.convertLinkedListToArray(msgList);
	}
}
return null;
}*/
	
	/*private void processBulkPut(BulkPut<Integer> bulkPutMesg)
	{
		this.contextserviceDB.updateGUIDRecord
			(bulkPutMesg.getGUID(), bulkPutMesg.getAttrName(), bulkPutMesg.getValue());
	}
	
	private GenericMessagingTask<Integer, BulkGetReply<Integer>> []
			processBulkGet(BulkGet<Integer> bulkGetMesg)
	{
		LinkedList<GenericMessagingTask<Integer, BulkGetReply<Integer>>> msgList
			= new LinkedList<GenericMessagingTask<Integer, BulkGetReply<Integer>>>();
		
		JSONArray getGUIDs = bulkGetMesg.getGUIDsToGet();
		String query = bulkGetMesg.getQuery();
		
		JSONArray result = new JSONArray();
		
		
		for(int i=0; i<getGUIDs.length(); i++)
		{
			String currGUID;
			try
			{
				currGUID = getGUIDs.getString(i);
				
				JSONObject currRec = this.contextserviceDB.getGUIDRecord(currGUID);
				
				if(currRec != null)
				{
					// check if it satisfies query.
					// sending "" so that all matches happen with currRec
					boolean groupCheck = Utils.groupMemberCheck(currRec, "", Double.MIN_VALUE, query);
					
					if(groupCheck)
					{
						result.put(currGUID);
					}
				}
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		
		BulkGetReply<Integer> bulkGetReply = new BulkGetReply<Integer>
										(this.getMyID(), bulkGetMesg.getReqID() ,result);
		
		GenericMessagingTask<Integer, BulkGetReply<Integer>> mtask = 
				new GenericMessagingTask<Integer, BulkGetReply<Integer>>(bulkGetMesg.getSender(), 
						bulkGetReply);
		msgList.add(mtask);
		
		return
			(GenericMessagingTask<Integer, BulkGetReply<Integer>>[]) this.convertLinkedListToArray(msgList);
	}*/
}