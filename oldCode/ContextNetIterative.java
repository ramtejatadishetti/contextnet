package edu.umass.cs.contextservice.schemes.old;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Vector;
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
import edu.umass.cs.contextservice.gns.GNSCallsOriginal;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
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
import edu.umass.cs.contextservice.messages.ValueUpdateMsgToValuenodeReply;
import edu.umass.cs.contextservice.messages.ContextServicePacket.PacketType;
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

public class ContextNetIterative<Integer> extends AbstractScheme<Integer>
{
	public static final Logger log = ContextServiceLogger.getLogger();
	
	public static final int THREAD_POOL_SIZE				= 500;
	// we don't want to do any computation in handleEvent method threads.
	private final ExecutorService nodeES;
	
	private SQLContextServiceDB<Integer> sqlDBObject 	= null;
	
	//FIXME: sourceID is not properly set, it is currently set to sourceID of each node,
	// it needs to be set to the origin sourceID.
	// Any id-based communication requires NodeConfig and Messenger
	public ContextNetIterative(InterfaceNodeConfig<Integer> nc, JSONMessenger<Integer> m)
	{
		super(nc, m);
		nodeES = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		new Thread(new ProfilerStatClass()).start();
		
		try
		{
			sqlDBObject = new SQLContextServiceDB<Integer>( this.getMyID() );
		} catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	
	public GenericMessagingTask<Integer,?>[] handleMetadataMsgToValuenode(
		ProtocolEvent<ContextServicePacket.PacketType, String> event,
		ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		long t0 = System.currentTimeMillis();
		
		@SuppressWarnings("unchecked")
		MetadataMsgToValuenode<Integer> metaMsgToValnode = (MetadataMsgToValuenode<Integer>) event;
		// just need to store the val node info in the local storage
		
		String attrName = metaMsgToValnode.getAttrName();
		double rangeStart = metaMsgToValnode.getRangeStart();
		double rangeEnd = metaMsgToValnode.getRangeEnd();
		
		ContextServiceLogger.getLogger().info("METADATA_MSG recvd at node " + 
				this.getMyID()+" attriName "+attrName + 
				" rangeStart "+rangeStart+" rangeEnd "+rangeEnd);
		
		//AttributeValueInformation<Integer> attrValueInfo = 
		//		new AttributeValueInformation<Integer>(attrName, rangeStart, rangeEnd);
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
		
		DelayProfiler.updateDelay("handleMetadataMsgToValuenode", t0);
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleQueryMsgFromUser(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleQueryMsgToMetadataNode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleQueryMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleQueryMsgToValuenodeReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateFromGNS(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateMsgToMetadataNode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		nodeES.submit(new HandleEventThread(event));
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateMsgToValuenodeReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
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
	public Integer getResponsibleNodeId(String AttrName)
	{
		long t0 = System.currentTimeMillis();
		
		int numNodes = this.allNodeIDs.size();
		
		//String attributeHash = Utils.getSHA1(attributeName);
		int mapIndex = Hashing.consistentHash(AttrName.hashCode(), numNodes);
		@SuppressWarnings("unchecked")
		Integer[] allNodeIDArr = (Integer[]) this.allNodeIDs.toArray();
		
		DelayProfiler.updateDelay("getResponsibleNodeId", t0);
		
		return allNodeIDArr[mapIndex];
	}
	
	@SuppressWarnings("unchecked")
	public GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] initializeScheme()
	{
		long t0 = System.currentTimeMillis();	
		log.fine("\n\n\n" +
				"In initializeMetadataObjects NodeId "+getMyID()+"\n\n\n");
		
		LinkedList<GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>> messageList = 
				new  LinkedList<GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>>();
		
		Vector<String> attributes = AttributeTypes.getAllAttributes();
		for(int i=0;i<attributes.size(); i++)
		{
			String currAttName = attributes.get(i);
			log.fine("initializeMetadataObjects currAttName "+currAttName);
			//String attributeHash = Utils.getSHA1(attributeName);
			Integer respNodeId = getResponsibleNodeId(currAttName);
			log.fine("InitializeMetadataObjects currAttName "+currAttName
					+" respNodeID "+respNodeId);
			// This node is responsible(meta data)for this Att.
			//if(respNodeId == getMyID() )
			{
				ContextServiceLogger.getLogger().info("Node Id "+getMyID() +
						" meta data node for attribute "+currAttName);
				// FIXME: set proper min max value, probably read attribute names and its min max value from file.
				//AttributeMetadataInformation<Integer> attrMeta = 
				//		new AttributeMetadataInformation<Integer>(currAttName, AttributeTypes.MIN_VALUE, 
				//				AttributeTypes.MAX_VALUE, csNode);
				
				if(!ContextServiceConfig.USESQL)
				{
					AttributeMetadataInfoRecord<Integer, Double> attrMetaRec =
						new AttributeMetadataInfoRecord<Integer, Double>
					(currAttName, AttributeTypes.MIN_VALUE, AttributeTypes.MAX_VALUE);
				
					getContextServiceDB().putAttributeMetaInfoRecord(attrMetaRec);
				}
				
				//csNode.addMetadataInfoRec(attrMetaRec);
				//;addMetadataList(attrMeta);
				//GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] messageTasks = 
				//		attrMeta.assignValueRanges(csNode.getMyID());
				
				GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] messageTasks 
						= assignValueRanges(getMyID(), currAttName, AttributeTypes.MIN_VALUE, AttributeTypes.MAX_VALUE);
				
				// every node stores metadata information but not send messages to value nodes
				if(respNodeId == getMyID() )
				{
					// add all the messaging tasks at different value nodes
					for(int j=0;j<messageTasks.length;j++)
					{
						messageList.add(messageTasks[j]);
					}
				}
			}
		}
		GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] returnArr 
					= new GenericMessagingTask[messageList.size()];
		
		for(int i=0;i<messageList.size();i++)
		{
			returnArr[i] = messageList.get(i);
		}
		
		log.fine("\n\n csNode.getMyID() "+getMyID()+
				" returnArr size "+returnArr.length +" messageList.size() "+messageList.size()+"\n\n");
		
		DelayProfiler.updateDelay("initializeScheme", t0);
		
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
	private void processQueryMsgFromUser(QueryMsgFromUser<Integer> queryMsgFromUser)
	{
		long t0 = System.currentTimeMillis();
		
		String query = queryMsgFromUser.getQuery();
		long userReqID = queryMsgFromUser.getUserReqNum();
		String userIP = queryMsgFromUser.getSourceIP();
		int userPort = queryMsgFromUser.getSourcePort();
		
		ContextServiceLogger.getLogger().fine("QUERY RECVD: QUERY_MSG recvd query recvd "+query);
		
		// create the empty group in GNS
		String grpGUID = GNSCalls.createQueryGroup(query);
		if( grpGUID.length() <= 0 )
		{
			ContextServiceLogger.getLogger().warning("Query request failed at the recieving node "+queryMsgFromUser);
			return;
		}
		
		// adding user to the notification set
		//GNSCalls.updateNotificationSetOfAGroup(new InetSocketAddress(userIP, userPort), query);
		
		Vector<QueryComponent> qcomponents = QueryParser.parseQuery(query);
		
		for( int i=0;i<qcomponents.size();i++ )
		{
			QueryComponent qc = qcomponents.get(i);
			String attrName = qc.getAttributeName();	
			List<MetadataTableInfo<Integer>> attrMetaObjRecList 
				= this.sqlDBObject.getAttributeMetaObjectRecord(attrName, qc.getLeftValue(), qc.getRightValue());
			
			qc.setValueNodeArray(attrMetaObjRecList);
		}
		
		QueryInfo<Integer> currReq  
			= new QueryInfo<Integer>(query, getMyID(), grpGUID, userReqID, userIP, userPort, qcomponents);
		
		DelayProfiler.updateDelay("processQueryMsgFromUser:Parsing", t0);
		
		long t1 = System.currentTimeMillis();
		
		synchronized(this.pendingQueryLock)
		{
			//currReq = new QueryInfo<Integer>(query, getMyID(),
			//		queryIdCounter++, grpGUID, userReqID, userIP, userPort);
			
			//StartContextServiceNode.sendQueryForProcessing(qinfo);
			//currReq.setRequestId(requestIdCounter);
			//requestIdCounter++;
			//currReq.setQueryComponents(qcomponents);
			currReq.setQueryRequestID(queryIdCounter++);
		}
		
		pendingQueryRequests.put(currReq.getRequestId(), currReq);
		
		DelayProfiler.updateDelay("processQueryMsgFromUser:QueryID", t1);
		
		/*if(ContextServiceConfig.EXP_PRINT_ON)
		{
			ContextServiceLogger.getLogger().fine("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSER REQUEST ID "
						+currReq.getRequestId()+" NUMATTR "+qcomponents.size()+" AT "+System.currentTimeMillis()
						+" "+qcomponents.get(0).getAttributeName()+" QueryStart "+queryStart);
		}*/
		
		long t2 = System.currentTimeMillis();
		
		for (int i=0;i<qcomponents.size();i++)
		{
			QueryComponent qc = qcomponents.elementAt(i);
			
			String atrName = qc.getAttributeName();
			Integer respNodeId = getResponsibleNodeId(atrName);
			
			QueryMsgToMetadataNode<Integer> queryMsgToMetaNode = 
					new QueryMsgToMetadataNode<Integer>(getMyID(), qc, currReq.getRequestId(), 
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
					" predicate "+qc.toString());
		}
		DelayProfiler.updateDelay("processQueryMsgFromUser:Loop", t2);
		DelayProfiler.updateDelay("processQueryMsgFromUser", t0);
	}
	
	/**
	 * Processes QueryMsgToMetadataNode node and 
	 * sends back reply in GenericMessaging tasks
	 * QueryMsgToValuenode
	 * @throws JSONException
	 * @throws IOException
	 */
	private void processQueryMsgToMetadataNode(QueryMsgToMetadataNode<Integer> queryMsgToMetaNode)
	{
		long t0 = System.currentTimeMillis();
		
		log.fine("processQueryMsgToMetadataNode: " +
				"predicate recvd string form "+queryMsgToMetaNode.getQueryComponent());
		
		QueryComponent qc= queryMsgToMetaNode.getQueryComponent();
		String attrName = qc.getAttributeName();
		
		ContextServiceLogger.getLogger().info("Predicate mesg recvd at" 
				+ this.getMyID() +" from node "+queryMsgToMetaNode.getSourceId() +
				" predicate "+qc.toString());
		
		if( !ContextServiceConfig.USESQL )
		{
			List<AttributeMetaObjectRecord<Integer, Double>> attrMetaObjRecList = 
			this.contextserviceDB.getAttributeMetaObjectRecord(attrName, qc.getLeftValue(), qc.getRightValue());
			
			ContextServiceLogger.getLogger().info("Predicate mesg recvd at" 
					+ this.getMyID() +" from node "+queryMsgToMetaNode.getSourceId() +
					" predicate "+qc.toString()+ "attrMetaObjRecList size "+attrMetaObjRecList.size()+attrMetaObjRecList);
			
			for( int i=0; i<attrMetaObjRecList.size(); i++ )
			{
				AttributeMetaObjectRecord<Integer, Double> currObj = 
														attrMetaObjRecList.get(i);
				
				if( ContextServiceConfig.GROUP_INFO_COMPONENT )
				{
					GroupGUIDRecord groupGUIDRec = new GroupGUIDRecord(queryMsgToMetaNode.getGroupGUID(),
							queryMsgToMetaNode.getQuery());
					
					log.fine("\n\n Adding group GUID "+groupGUIDRec.getGroupQuery()+ " to "+currObj.toString());
					
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
				
				//this.contextserviceDB.putGroupGUIDRecord(groupGUIDRec);
				//GroupGUIDInfo grpGUIDInfo = new GroupGUIDInfo(queryMsgToMetaNode.getGroupGUID(),
				//		queryMsgToMetaNode.getQuery());
				
				//currObj.addGroupGUIDInfo(grpGUIDInfo);
				
				QueryMsgToValuenode<Integer> queryMsgToValnode 
					= new QueryMsgToValuenode<Integer>( queryMsgToMetaNode.getSourceId(), qc,
						queryMsgToMetaNode.getRequestId(), queryMsgToMetaNode.getSourceId(),
						queryMsgToMetaNode.getQuery(), queryMsgToMetaNode.getGroupGUID(), attrMetaObjRecList.size() );
				
				ContextServiceLogger.getLogger().info("Sending ValueNodeMessage from" 
						+ this.getMyID() +" to node "+currObj.getNodeID() + 
						" predicate "+qc.toString());
				
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
			}
		}
		else
		{
			long t1 = System.currentTimeMillis();
			List<MetadataTableInfo<Integer>> attrMetaObjRecList 
							= this.sqlDBObject.getAttributeMetaObjectRecord(attrName, qc.getLeftValue(), qc.getRightValue());
			
			DelayProfiler.updateDelay("processQueryMsgToMetadataNode:DatabaseGet", t1);
			
			long t2 = System.currentTimeMillis();
			for( int i=0; i<attrMetaObjRecList.size(); i++ )
			{
				@SuppressWarnings("unchecked")
				MetadataTableInfo<Integer> currObj = (MetadataTableInfo<Integer>) attrMetaObjRecList.get(i);
				
				//this.contextserviceDB.putGroupGUIDRecord(groupGUIDRec);
				//GroupGUIDInfo grpGUIDInfo = new GroupGUIDInfo(queryMsgToMetaNode.getGroupGUID(),
				//		queryMsgToMetaNode.getQuery());
				
				//currObj.addGroupGUIDInfo(grpGUIDInfo);
				
				QueryMsgToValuenode<Integer> queryMsgToValnode 
					= new QueryMsgToValuenode<Integer>( queryMsgToMetaNode.getSourceId(), qc,
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
				DelayProfiler.updateDelay("processQueryMsgToMetadataNode:NIO", t3);
				
				// just send the first one and break;
				break;
			}
			DelayProfiler.updateDelay("processQueryMsgToMetadataNode:Loop", t2);
		}	
		DelayProfiler.updateDelay("processQueryMsgToMetadataNode", t0);
	}
	
	/**
	 * Processes the QueryMsgToValuenode and replies with 
	 * QueryMsgToValuenodeReply, which contains the GUIDs
	 */
	private void processQueryMsgToValuenode(QueryMsgToValuenode<Integer> queryMsgToValnode)
	{
		long t0 = System.currentTimeMillis();
		
		QueryComponent predicate   = queryMsgToValnode.getQueryComponent();
		
		Integer queryRecvNodeId = queryMsgToValnode.getSourceId();
		
		long requestID  = queryMsgToValnode.getRequestId();
		int componentID = predicate.getComponentID();
		
		//  LinkedList<String> resultGUIDs = new LinkedList<String>();
		JSONArray resultGUIDs = new JSONArray();
		
		if( !ContextServiceConfig.USESQL )
		{
		    List<ValueInfoObjectRecord<Double>> valInfoObjRecList = 
					this.contextserviceDB.getValueInfoObjectRecord
						(predicate.getAttributeName(), predicate.getLeftValue(), predicate.getRightValue());
		    
		    ContextServiceLogger.getLogger().info("QueryMsgToValuenode recvd at " 
					+ this.getMyID() +" from node "+queryMsgToValnode.getSourceId() +
					" predicate "+predicate.toString()+"valInfoObjRecList "+valInfoObjRecList.size());
		    
		    DelayProfiler.updateDelay("processQueryMsgToValuenode:DatabaseGet ", t0);
			
		    long t3 = System.currentTimeMillis();
			for(int i=0;i<valInfoObjRecList.size();i++)
			{
				ValueInfoObjectRecord<Double> valueObjRec = valInfoObjRecList.get(i);
				
				ContextServiceLogger.getLogger().fine("valueObjRec "+valueObjRec);
				//resultGUIDs.add(nodeGUIDRecList.get(i).getNodeGUID());
				
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
			DelayProfiler.updateDelay("processQueryMsgToValuenode:Loop ", t3);
		}
		else
		{
			//resultGUIDs = this.sqlDBObject.getValueInfoObjectRecord
			//		( predicate.getAttributeName(), predicate.getLeftValue(), predicate.getRightValue() );
			String attrName 	= predicate.getAttributeName();
			double predicateMin = predicate.getLeftValue();
			double predicateMax = predicate.getRightValue();
			//this.sqlDBObject.getValueInfoObjectRecordCreateTable(attrName, predicateMin, predicateMax);
			//getValueInfoObjectRecordIterative(attrName, queryMin, queryMax);
			
			DelayProfiler.updateDelay("processQueryMsgToValuenode:DatabaseGet ", t0);
		}
		
		long t4 = System.currentTimeMillis();
		
		QueryMsgToValuenodeReply<Integer> queryMsgToValReply 
			= new QueryMsgToValuenodeReply<Integer>(getMyID(), resultGUIDs, requestID, 
					componentID, getMyID(), queryMsgToValnode.getNumValNodesContacted());
		
		try
		{
			this.messenger.sendToID(queryMsgToValnode.getSourceId(), queryMsgToValReply.toJSONObject());
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		DelayProfiler.updateDelay("processQueryMsgToValuenode:Sending ", t4);
		
		ContextServiceLogger.getLogger().info("Sending QueryMsgToValuenodeReply from " 
						+ this.getMyID() +" to node "+queryMsgToValnode.getSourceId()+
						" reply "+queryMsgToValReply.toString());
		
		DelayProfiler.updateDelay("processQueryMsgToValuenode", t0);
	}
	
	private void processValueUpdateMsgToMetadataNode(ValueUpdateMsgToMetadataNode<Integer> valUpdateMsgToMetaNode)
	{
		long t0 = System.currentTimeMillis();
		
		long versionNum = valUpdateMsgToMetaNode.getVersionNum();
		String attrName = valUpdateMsgToMetaNode.getAttrName();
		String GUID = valUpdateMsgToMetaNode.getGUID();
		double oldValue = Double.MIN_VALUE;
		double newValue = valUpdateMsgToMetaNode.getNewValue();
		JSONObject allAttrs = new JSONObject();
		//Integer sourceID = valUpdateMsgToMetaNode.getSourceID();
		Integer sourceID;
		long requestID = valUpdateMsgToMetaNode.getRequestID();
		
		ContextServiceLogger.getLogger().info("ValueUpdateToMetadataMesg recvd at " 
				+ this.getMyID() +" for GUID "+GUID+
				" "+attrName + " "+oldValue+" "+newValue);
	
		if( !ContextServiceConfig.USESQL )
		{
			//LinkedList<AttributeMetaObjectRecord<Integer, Double>> 
			// there should be just one element in the list, or definitely at least one.
			LinkedList<AttributeMetaObjectRecord<Integer, Double>> oldMetaObjRecList = 
				(LinkedList<AttributeMetaObjectRecord<Integer, Double>>) 
				this.getContextServiceDB().getAttributeMetaObjectRecord(attrName, oldValue, oldValue);
			
			AttributeMetaObjectRecord<Integer, Double> oldMetaObjRec = null;
			
			if(oldMetaObjRecList.size()>0)
			{
				oldMetaObjRec = this.getContextServiceDB().getAttributeMetaObjectRecord(attrName, oldValue, oldValue).get(0);
			}
			//oldMetaObj = new AttributeMetadataObject<Integer>();
			
			// same thing for the newValue
			AttributeMetaObjectRecord<Integer, Double> newMetaObjRec = 
					this.getContextServiceDB().getAttributeMetaObjectRecord(attrName, newValue, newValue).get(0);
			
			if( ContextServiceConfig.GROUP_INFO_COMPONENT )
			{
				LinkedList<GroupGUIDRecord> oldValueGroups = null;
				
				// do group updates for the old value
				try
				{
					if(oldMetaObjRec != null)
					{
						oldValueGroups = getGroupsAffectedUsingDatabase
								(oldMetaObjRec, allAttrs, attrName, oldValue);
						
						log.fine("Old Val groups");
						for(int i=0;i<oldValueGroups.size();i++)
						{
							log.finer("\n\n"+oldValueGroups.get(i).toString()+"\n\n");
						}
						
						long rstart = System.currentTimeMillis();
						GNSCalls.userGUIDAndGroupGUIDOperations
						(GUID, oldValueGroups, GNSCallsOriginal.UserGUIDOperations.REMOVE_USER_GUID_FROM_GROUP);
						long rend = System.currentTimeMillis();
						
						// send notifications to the notification set for these affected groups.
						//long nstart = System.currentTimeMillis();
						//sendNotifications(oldValueGroups);
						//long nend = System.currentTimeMillis();
						log.fine( "Remove user time "+(rend-rstart) );
						
						String mesg = "GroupGUIDOper: Remove user time "+(rend-rstart);	
						//Utils.sendUDP(mesg);
					}
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
				
				// do group updates for the new value
				LinkedList<GroupGUIDRecord> newValueGroups = null;
				
				try
				{
					if(newMetaObjRec!=null)
					{
						newValueGroups = getGroupsAffectedUsingDatabase
								(newMetaObjRec, allAttrs, attrName, newValue);
								
								//newMetaObj.getGroupsAffected(allAttr, updateAttrName, newVal);
						
						log.fine("New Val groups");
						for(int i=0;i<newValueGroups.size();i++)
						{
							log.finer("\n\n"+newValueGroups.get(i).toString()+"\n\n");
						}
						
						long astart = System.currentTimeMillis();
						GNSCalls.userGUIDAndGroupGUIDOperations
						(GUID, newValueGroups, GNSCallsOriginal.UserGUIDOperations.ADD_USER_GUID_TO_GROUP);
						long aend = System.currentTimeMillis();
						
						//long nstart = System.currentTimeMillis();
						// send notifications to the notification set for these affected groups.
						//sendNotifications(newValueGroups);
						//long nend = System.currentTimeMillis();
						
						log.fine("GroupGUIDOper: Add user time "+(aend-astart));
						String mesg = "GroupGUIDOper: Add user time "+(aend-astart);
						//Utils.sendUDP(mesg);
					} else
					{
						assert(false);
					}
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
				
				// send notifications to the notification set for these affected groups.
				long nstart = System.currentTimeMillis();
				sendNotifications(oldValueGroups, newValueGroups, versionNum);
				long nend = System.currentTimeMillis();
				log.fine(" notific time "+(nend-nstart));
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
				ValueUpdateMsgToValuenode<Integer> oldValueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<Integer>
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
				
				
				ValueUpdateMsgToValuenode<Integer> newValueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<Integer>
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
		}
		else
		{
			@SuppressWarnings("unchecked")
			MetadataTableInfo<Integer> newMetaObjRec = 
						(MetadataTableInfo<Integer>) 
						this.sqlDBObject.getAttributeMetaObjectRecord(attrName, newValue, newValue).get(0);
				
				// for the new value
				Integer newValueNodeId = newMetaObjRec.getNodeID();
				
				// for the old value
				Integer oldValueNodeId = newValueNodeId;
				if(oldValue != AttributeTypes.NOT_SET)
				{
					@SuppressWarnings("unchecked")
					MetadataTableInfo<Integer> oldMetaObjRec = 
						(MetadataTableInfo<Integer>) 
						this.sqlDBObject.getAttributeMetaObjectRecord(attrName, oldValue, oldValue).get(0);
						
					oldValueNodeId = oldMetaObjRec.getNodeID();
				}
				
				if( oldValueNodeId.equals(newValueNodeId) )
				{
					ValueUpdateMsgToValuenode<Integer> valueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<Integer>
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
					ValueUpdateMsgToValuenode<Integer> oldValueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<Integer>
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
					
					
					ValueUpdateMsgToValuenode<Integer> newValueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<Integer>
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
		}
		DelayProfiler.updateDelay("processValueUpdateMsgToMetadataNode", t0);
	}
	
	/**
	 * adds the reply of the queryComponent
	 * @throws JSONException
	 */
	private void processValueUpdateMsgToValuenode(ValueUpdateMsgToValuenode<Integer> valUpdateMsgToValnode)
	{
		long t0 = System.currentTimeMillis();
		
		ContextServiceLogger.getLogger().info("\n\n Recvd ValueUpdateMsgToValuenode at " 
				+ this.getMyID() +" reply "+valUpdateMsgToValnode);
		
		String attrName = valUpdateMsgToValnode.getAttrName();
		String GUID = valUpdateMsgToValnode.getGUID();
		double oldValue = Double.MIN_VALUE;
		double newValue = valUpdateMsgToValnode.getNewValue();
		long versionNum = valUpdateMsgToValnode.getVersionNum();
		//Integer sourceID = valUpdateMsgToValnode.getSourceID();
		Integer sourceID = null ;
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
					
					ValueUpdateMsgToValuenodeReply<Integer> newValueUpdateMsgReply
						= new ValueUpdateMsgToValuenodeReply<Integer>
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
					}
					
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
					ValueUpdateMsgToValuenodeReply<Integer> newValueUpdateMsgReply
					= new ValueUpdateMsgToValuenodeReply<Integer>
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
					}
					
					break;
				}
				case ValueUpdateMsgToValuenode.REMOVE_ADD_BOTH:
				{
					ContextServiceLogger.getLogger().fine("REMOVE_ADD_BOTH ");
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
					ValueUpdateMsgToValuenodeReply<Integer> newValueUpdateMsgReply
					= new ValueUpdateMsgToValuenodeReply<Integer>
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
					}
					break;
				}
			}
		}
		DelayProfiler.updateDelay("processValueUpdateMsgToValuenode", t0);
	}
	
	private void 
		processValueUpdateMsgToValuenodeReply(ValueUpdateMsgToValuenodeReply<Integer> valUpdateMsgToValnodeRep)
	{
		long t0 = System.currentTimeMillis();
		
		long requestId =  valUpdateMsgToValnodeRep.getRequestID();
		/*UpdateInfo<Integer> updateInfo = pendingUpdateRequests.get(requestId);
		if(updateInfo != null)
		{
			updateInfo.incrementNumReplyRecvd();
			if(updateInfo.getNumReplyRecvd() == valUpdateMsgToValnodeRep.getNumReply())
			{
				//String mesg = "Value update complete";
				//Utils.sendUDP(mesg);
				String sourceIP = updateInfo.getValueUpdateFromGNS().getSourceIP();
				int sourcePort = updateInfo.getValueUpdateFromGNS().getSourcePort();
				//ContextServiceLogger.getLogger().fine("Update time taken "+(System.currentTimeMillis() - updateInfo.getStartTime()));
				
				synchronized(this.pendingUpdateLock)
				{
					if(sourceIP.equals("") && sourcePort == -1)
					{
						// no reply
					} else
					{
						if( this.pendingUpdateRequests.get(updateInfo.getRequestId())  != null )
						{
							sendUpdateReplyBackToUser(sourceIP, 
								sourcePort, updateInfo.getValueUpdateFromGNS().getVersionNum());
						}
					}
					pendingUpdateRequests.remove(requestId);
				}
			}
		}*/
		
		DelayProfiler.updateDelay("processValueUpdateMsgToValuenodeReply", t0);
		
		//ContextServiceLogger.getLogger().fine("componentReplies.size() "+componentReplies.size() +
		//		" queryComponents.size() "+queryComponents.size());
		// if there is at least one replies recvd for each component
	}
	
	/**
	 * adds the reply of the queryComponent
	 * @throws JSONException
	 */
	private void processValueUpdateFromGNS(ValueUpdateFromGNS<Integer> valUpdMsgFromGNS)
	{
		long t0 = System.currentTimeMillis();
		
		ContextServiceLogger.getLogger().info("\n\n Recvd ValueUpdateFromGNS at " 
				+ this.getMyID() +" reply "+valUpdMsgFromGNS);
		
		long versionNum = valUpdMsgFromGNS.getVersionNum();
		String GUID = valUpdMsgFromGNS.getGUID();
		String attrName = "";
		String oldVal = "";
		String newVal = "";
		//JSONObject allAttrs = new JSONObject();
		
		double oldValD, newValD;
		
		if(oldVal.equals(""))
		{
			oldValD = AttributeTypes.NOT_SET;
		} else
		{
			oldValD = Double.parseDouble(oldVal);
		}
		newValD = Double.parseDouble(newVal);
		
		long currReqID = -1;
		
		/*synchronized(this.pendingUpdateLock)
		{
			UpdateInfo<Integer> currReq 
				= new UpdateInfo<Integer>(valUpdMsgFromGNS, updateIdCounter++);
			currReqID = currReq.getRequestId();
			pendingUpdateRequests.put(currReqID, currReq);
		}*/
		
		ValueUpdateMsgToMetadataNode<Integer> valueUpdMsgToMetanode = 
			new ValueUpdateMsgToMetadataNode<Integer>(this.getMyID(), versionNum, GUID, attrName, 
					0,newValD, currReqID);
		
		Integer respMetadataNodeId = this.getResponsibleNodeId(attrName);
		
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
		
		DelayProfiler.updateDelay("processValueUpdateFromGNS", t0);
	}
	
	
	private LinkedList<GroupGUIDRecord> getGroupsAffectedUsingDatabase
	(AttributeMetaObjectRecord<Integer, Double> metaObjRec, JSONObject allAttr, 
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
			
			boolean groupCheck = Utils.groupMemberCheck( allAttr, updateAttrName, 
					attrVal, groupGUIDRec.getGroupQuery() );
			
			log.fine("checking group "+groupGUIDJSON+" groupCheck "+groupCheck);
			if(groupCheck)
			{
				//GroupGUIDInfo guidInfo = new GroupGUIDInfo(groupGUID, groupGUIDRec.getGroupQuery());
				satisfyingGroups.add(groupGUIDRec);
			}
		}
		
		DelayProfiler.updateDelay("getGroupsAffectedUsingDatabase", t0);
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
		long t0 = System.currentTimeMillis();
		
		long requestId =  queryMsgToValnodeRep.getRequestID();
		QueryInfo<Integer> queryInfo = pendingQueryRequests.get(requestId);
		
		if(queryInfo != null)
		{
			processReplyInternally(queryMsgToValnodeRep, queryInfo);
		}
		
		DelayProfiler.updateDelay("addQueryReply", t0);
	}
	
	public void checkQueryCompletion(QueryInfo<Integer> qinfo)
	{
		long t0 = System.currentTimeMillis();
		
		if( qinfo.componentReplies.size() == qinfo.queryComponents.size() )
		{
			// check if all the replies have been received by the value nodes
			if( checkIfAllRepliesRecvd(qinfo) )
			{
				ContextServiceLogger.getLogger().info("\n\n All replies recvd for each component");
				LinkedList<LinkedList<String>> doConjuc = new LinkedList<LinkedList<String>>();
				doConjuc.addAll(qinfo.componentReplies.values());
				JSONArray queryAnswer = Utils.doConjuction(doConjuc);
				ContextServiceLogger.getLogger().info("Query Answer "+queryAnswer);
				
				//FIXME: uncomment this, just for debugging
				GNSCalls.addGUIDsToGroup(Utils.doConjuction(doConjuc), qinfo.getQuery(), qinfo.getGroupGUID());
				
				
				if(ContextServiceConfig.EXP_PRINT_ON)
				{
					//ContextServiceLogger.getLogger().fine("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSERREPLY REQUEST ID "
					//			+qinfo.getRequestId()+" NUMATTR "+qinfo.queryComponents.size()+" AT "+qprocessingTime+" EndTime "
					//		+queryEndTime+ " QUERY ANSWER "+queryAnswer);
					
					long now = System.currentTimeMillis();
					ContextServiceLogger.getLogger().fine("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSERREPLY REQUEST ID "
										+qinfo.getRequestId()+" NUMATTR "+qinfo.queryComponents.size()
										+" TIME "+(now-qinfo.getCreationTime()));
				}
				
				//takes care of not sending two replies, as concurrent queue will return only one non null;
				QueryInfo<Integer> removedQInfo = this.pendingQueryRequests.remove(qinfo.getRequestId());
				if(removedQInfo != null)
				{
					sendReplyBackToUser(qinfo, queryAnswer);
				}
				
				/*synchronized(this.pendingQueryLock)
				{
					// so that no two replies to the user are sent
					if( this.pendingQueryRequests.get(qinfo.getRequestId())  != null )
					{
						sendReplyBackToUser(qinfo, (LinkedList<String>) Utils.JSONArayToList(queryAnswer));
						this.pendingQueryRequests.remove(qinfo.getRequestId());
					}
				}*/
			}
		}
		
		DelayProfiler.updateDelay("checkQueryCompletion", t0);
	}
	
	private boolean checkIfAllRepliesRecvd(QueryInfo<Integer> qinfo)
	{
		long t0 = System.currentTimeMillis();
		boolean resultRet = true;
		for(int i=0;i<qinfo.queryComponents.size();i++)
		{
			QueryComponent qc = qinfo.queryComponents.get(i);
			if( qc.getNumCompReplyRecvd() != qc.getTotalCompReply() )
			{
				resultRet = false;
				
				DelayProfiler.updateDelay("checkIfAllRepliesRecvd", t0);
				
				return resultRet;
			}
		}
		
		DelayProfiler.updateDelay("checkIfAllRepliesRecvd", t0);
		return resultRet;
	}
	
	/**
	 * Function stays here, it will be moved to value partitioner package
	 * whenever that package is decided upon.
	 * Uniformly assigns the value ranges to the nodes in
	 * the system for the given attribute.
	 */
	private GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] 
			assignValueRanges(Integer initiator, String attrName, double attrMin, double attrMax)
	{
		long t0 = System.currentTimeMillis();
		
		//int numValueNodes = this.getAllNodeIDs().size();
		int numValueNodes = 3;
		@SuppressWarnings("unchecked")
		GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] mesgArray 
								= new GenericMessagingTask[numValueNodes];
		
		Set<Integer> allNodeIDs = this.getAllNodeIDs();
		
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
			Integer[] allNodeIDArr = (Integer[]) allNodeIDs.toArray();
			
			Integer currNodeID = (Integer)allNodeIDArr[currIndex];
			
			//AttributeMetadataObject<Integer> attObject = new AttributeMetadataObject<Integer>( currMinRange, 
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
					AttributeMetaObjectRecord<Integer, Double> attrMetaObjRec = new
						AttributeMetaObjectRecord<Integer, Double>(currMinRange, currMaxRange,
						currNodeID, new JSONArray());
				
					this.getContextServiceDB().putAttributeMetaObjectRecord(attrMetaObjRec, attrName);
				}
				else
				{
					this.sqlDBObject.putAttributeMetaObjectRecord(attrName, 
							currMinRange, currMaxRange, currNodeID);
				}
			}
			
			
			MetadataMsgToValuenode<Integer> metaMsgToValnode = new MetadataMsgToValuenode<Integer>
							( initiator, attrName, currMinRange, currMaxRange);
			
			GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>> mtask = new GenericMessagingTask<Integer, 
					MetadataMsgToValuenode<Integer>>((Integer) currNodeID, metaMsgToValnode);
			
			mesgArray[i] = mtask;
			
			ContextServiceLogger.getLogger().info("csID "+getMyID()+" Metadata Message attribute "+
					attrName+"dest "+currNodeID+" min range "+currMinRange+" max range "+currMaxRange);
			
		}
		DelayProfiler.updateDelay("assignValueRanges", t0);
		return mesgArray;
	}
	
	protected void processReplyInternally
	(QueryMsgToValuenodeReply<Integer> queryMsgToValnodeRep, QueryInfo<Integer> queryInfo)
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
		
		DelayProfiler.updateDelay("processReplyInternally", t0);
		
		//ContextServiceLogger.getLogger().fine("componentReplies.size() "+componentReplies.size() +
		//		" queryComponents.size() "+queryComponents.size());
		// if there is at least one replies recvd for each component
	}
	
	private void updateNumberOfRepliesRecvd
		(QueryMsgToValuenodeReply<Integer> queryMsgToValnodeRep, QueryInfo<Integer> queryInfo)
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
		DelayProfiler.updateDelay("updateNumberOfRepliesRecvd", t0);
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
		DelayProfiler.updateDelay("sendNotifications", t0);
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
			switch( event.getType() )
			{
				case  QUERY_MSG_FROM_USER:
				{
					long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					QueryMsgFromUser<Integer> queryMsgFromUser 
											= (QueryMsgFromUser<Integer>)event;
					
					processQueryMsgFromUser(queryMsgFromUser);
					
					DelayProfiler.updateDelay("handleQueryMsgFromUser", t0);
					break;
				}
				case QUERY_MSG_TO_METADATANODE:
				{
					long t0 = System.currentTimeMillis();
					
					@SuppressWarnings("unchecked")
					QueryMsgToMetadataNode<Integer> queryMsgToMetaNode = 
							(QueryMsgToMetadataNode<Integer>) event;
					
					log.fine("CS"+getMyID()+" received " + event.getType() + ": " + event);
					
					processQueryMsgToMetadataNode(queryMsgToMetaNode);
					
					DelayProfiler.updateDelay("handleQueryMsgToMetadataNode", t0);
					break;
				}
				case QUERY_MSG_TO_VALUENODE:
				{
					long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					QueryMsgToValuenode<Integer> queryMsgToValnode = 
							(QueryMsgToValuenode<Integer>)event;
					
					log.fine("CS"+getMyID()+" received " + event.getType() + ": " + queryMsgToValnode);
					
					processQueryMsgToValuenode(queryMsgToValnode);
					
					DelayProfiler.updateDelay("handleQueryMsgToValuenode", t0);
				}
				case QUERY_MSG_TO_VALUENODE_REPLY:
				{	
					long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					QueryMsgToValuenodeReply<Integer> queryMsgToValnodeReply = 
							(QueryMsgToValuenodeReply<Integer>)event;
					
					ContextServiceLogger.getLogger().info("Recvd QueryMsgToValuenodeReply at " 
							+ getMyID() +" reply "+queryMsgToValnodeReply.toString());
					
					try
					{
						addQueryReply(queryMsgToValnodeReply);
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
					DelayProfiler.updateDelay("handleQueryMsgToValuenodeReply", t0);
					break;
				}
				case VALUE_UPDATE_MSG_FROM_GNS:
				{
					long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					ValueUpdateFromGNS<Integer> valUpdMsgFromGNS = (ValueUpdateFromGNS<Integer>)event;
					MSocketLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgFromGNS);
					
					processValueUpdateFromGNS(valUpdMsgFromGNS);
					
					DelayProfiler.updateDelay("handleValueUpdateFromGNS", t0);
					
					break;
				}
				case VALUE_UPDATE_MSG_TO_METADATANODE:
				{
					/* Actions:
					 * - send the update message to the responsible value node
					 */
					long t0 = System.currentTimeMillis();
					
					@SuppressWarnings("unchecked")
					ValueUpdateMsgToMetadataNode<Integer> valUpdateMsgToMetaNode 
								= (ValueUpdateMsgToMetadataNode<Integer>)event;
					log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valUpdateMsgToMetaNode);
					
					processValueUpdateMsgToMetadataNode(valUpdateMsgToMetaNode);
					
					DelayProfiler.updateDelay("handleValueUpdateMsgToMetadataNode", t0);
					break;
				}
				case VALUE_UPDATE_MSG_TO_VALUENODE:
				{
					/* Actions:
					 * just update / add or remove the entry
					 */
					long t0 = System.currentTimeMillis();
					
					@SuppressWarnings("unchecked")
					ValueUpdateMsgToValuenode<Integer> valUpdMsgToValnode = (ValueUpdateMsgToValuenode<Integer>)event;
					log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgToValnode);
					
					processValueUpdateMsgToValuenode(valUpdMsgToValnode);
					
					DelayProfiler.updateDelay("handleValueUpdateMsgToValuenode", t0);
					break;
				}
				case VALUE_UPDATE_MSG_TO_VALUENODE_REPLY:
				{
					long t0 = System.currentTimeMillis();
					@SuppressWarnings("unchecked")
					ValueUpdateMsgToValuenodeReply<Integer> valUpdMsgToValnode = (ValueUpdateMsgToValuenodeReply<Integer>)event;
					log.fine("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgToValnode);
					processValueUpdateMsgToValuenodeReply(valUpdMsgToValnode);
					
					DelayProfiler.updateDelay("handleValueUpdateMsgToValuenodeReply", t0);
					
					break;
				}
			}
		}
	}
	
	public GenericMessagingTask<Integer, ?>[] handleEchoMessage(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks)
	{
		long t0 = System.currentTimeMillis();
		@SuppressWarnings("unchecked")
		EchoMessage<Integer> echoMessage = (EchoMessage<Integer>)event;
		
		EchoReplyMessage<Integer> valUR = 
				new EchoReplyMessage<Integer>(this.getMyID(), "echoReply");
		
		String sourceIP = echoMessage.getSourceIP();
		int sourcePort  = echoMessage.getSourcePort();
		
		try
		{
			log.fine("sendEchoReplyBackToUser "+sourceIP+" "+sourcePort+
				valUR.toJSONObject());
			
			ContextServiceLogger.getLogger().fine("sendEchoReplyBackToUser "+sourceIP+" "+sourcePort+
					valUR.toJSONObject());
		
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
		DelayProfiler.updateDelay("handleEchoMessage", t0);
		
		return null;
	}
	
	private class ProfilerStatClass implements Runnable
	{
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
				ContextServiceLogger.getLogger().fine("DelayProfiler stats "+DelayProfiler.getStats());
			}
		}
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleBulkGet(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleBulkGetReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleConsistentStoragePut(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
		return null;
	}

	@Override
	public GenericMessagingTask<Integer, ?>[] handleConsistentStoragePutReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<Integer, PacketType, String>[] ptasks) 
	{
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
}