package edu.umass.cs.contextservice.schemes;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.AttributeTypes;
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
import edu.umass.cs.contextservice.messages.QueryMsgToValuenode;
import edu.umass.cs.contextservice.messages.QueryMsgToValuenodeReply;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateMsgToValuenode;
import edu.umass.cs.contextservice.processing.QueryComponent;
import edu.umass.cs.contextservice.processing.QueryInfo;
import edu.umass.cs.contextservice.processing.QueryParser;
import edu.umass.cs.contextservice.processing.UpdateInfo;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.InterfaceNodeConfig;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.protocoltask.ProtocolEvent;
import edu.umass.cs.protocoltask.ProtocolTask;

public class QueryAllScheme<NodeIDType> extends AbstractScheme<NodeIDType>
{
	public static final Logger log =Logger.getLogger(QueryAllScheme.class.getName());
	
	private final Object valueNodeReplyLock = new Object();
	
	//FIXME: sourceID is not properly set, it is currently set to sourceID of each node,
	// it needs to be set to the origin sourceID.
	// Any id-based communication requires NodeConfig and Messenger
	public QueryAllScheme(InterfaceNodeConfig<NodeIDType> nc, JSONMessenger<NodeIDType> m)
	{
		super(nc, m);
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleQueryMsgFromUser(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		@SuppressWarnings("unchecked")
		QueryMsgFromUser<NodeIDType> queryMsgFromUser = (QueryMsgFromUser<NodeIDType>)event;
		
		GenericMessagingTask<NodeIDType, QueryMsgToValuenode<NodeIDType>>[] retMsgs =
				processQueryMsgFromUser(queryMsgFromUser);
		
		synchronized(this.numMesgLock)
		{
			if(retMsgs != null)
			{
				this.numMessagesInSystem+=retMsgs.length;
			}
		}
		return retMsgs;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleValueUpdateFromGNS(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		@SuppressWarnings("unchecked")
		ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS = (ValueUpdateFromGNS<NodeIDType>)event;
		//System.out.println("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgFromGNS);
		
		GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenode<NodeIDType>>[] retMsgs 
			= this.processValueUpdateFromGNS(valUpdMsgFromGNS);
		
		synchronized(this.numMesgLock)
		{
			if(retMsgs != null)
			{
				this.numMessagesInSystem+=retMsgs.length;
			}
		}
		return retMsgs;
	}
	
	
	public GenericMessagingTask<NodeIDType,?>[] handleQueryMsgToValuenodeReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * - gets the QueryMsgToValuenodeReply and stores that.
		 */
		@SuppressWarnings("unchecked")
		QueryMsgToValuenodeReply<NodeIDType> queryMsgToValnodeReply = 
				(QueryMsgToValuenodeReply<NodeIDType>)event;
		
		ContextServiceLogger.getLogger().info("Recvd QueryMsgToValuenodeReply at " 
				+ this.getMyID() +" reply "+queryMsgToValnodeReply.toString());
		
		try 
		{
			addQueryReply(queryMsgToValnodeReply);
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleQueryMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * - contacts the local information and sends back the 
		 * QueryMsgToValuenodeReply
		 */
		@SuppressWarnings("unchecked")
		QueryMsgToValuenode<NodeIDType> queryMsgToValnode = 
				(QueryMsgToValuenode<NodeIDType>)event;
		
		//System.out.println("CS"+getMyID()+" received " + event.getType() + ": " + queryMsgToValnode);
		
		
		GenericMessagingTask<NodeIDType, QueryMsgToValuenodeReply<NodeIDType>>[] retMsgs
			= this.processQueryMsgToValuenode(queryMsgToValnode);
		
		/*synchronized(this.numMesgLock)
		{
			if(retMsgs != null)
			{
				this.numMessagesInSystem+=retMsgs.length;
			}
		}*/
		return retMsgs;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleMetadataMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * - Just store the metadata info recvd in the local storage
		 */
		@SuppressWarnings("unchecked")
		MetadataMsgToValuenode<NodeIDType> metaMsgToValnode = (MetadataMsgToValuenode<NodeIDType>) event;
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
	
	
	// may not be used here.
	public GenericMessagingTask<NodeIDType,?>[] handleValueUpdateMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		//@SuppressWarnings("unchecked")
		return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleQueryMsgToMetadataNode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleValueUpdateMsgToMetadataNode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
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
//		int numNodes = this.allNodeIDs.size();
//		
//		//String attributeHash = Utils.getSHA1(attributeName);
//		int mapIndex = Hashing.consistentHash(AttrName.hashCode(), numNodes);
//		@SuppressWarnings("unchecked")
//		NodeIDType[] allNodeIDArr = (NodeIDType[]) this.allNodeIDs.toArray();
//		return allNodeIDArr[mapIndex];
		return this.getMyID();
	}
	

	public void checkQueryCompletion(QueryInfo<NodeIDType> qinfo)
	{
		// in query all, replies from all nodes recvd
		if( qinfo.componentReplies.size() == this.allNodeIDs.size() )
		{
			//System.out.println("\n\n replies recvd from all nodes\n\n");
			LinkedList<LinkedList<String>> doConjuc = new LinkedList<LinkedList<String>>();
			doConjuc.addAll(qinfo.componentReplies.values());
			JSONArray queryAnswer = Utils.doDisjuction(doConjuc);
			//System.out.println("\n\nQuery Answer "+queryAnswer);
			
			//FIXME: uncomment this, just for debugging
			GNSCalls.addGUIDsToGroup(queryAnswer, qinfo.getQuery(), qinfo.getGroupGUID());
				
			/*if(ContextServiceConfig.EXP_PRINT_ON)
			{
				System.out.println("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSERREPLY REQUEST ID "
					+qinfo.getRequestId()+" NUMATTR "+qinfo.queryComponents.size()+" AT "+qprocessingTime+" EndTime "
					+queryEndTime+ " QUERY ANSWER "+queryAnswer);
			}*/
			sendReplyBackToUser(qinfo, queryAnswer);
			
			synchronized(this.pendingQueryLock)
			{
				this.pendingQueryRequests.remove(qinfo.getRequestId());
			}
		}	
	}
	
	public GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] initializeScheme()
	{
		//System.out.println("\n\n\n" +
		//		"In initializeMetadataObjects NodeId "+getMyID()+"\n\n\n");
		
		//LinkedList<GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>> messageList = 
		//		new  LinkedList<GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>>();
		
		Vector<String> attributes = AttributeTypes.getAllAttributes();
		for(int i=0;i<attributes.size(); i++)
		{
			String currAttName = attributes.get(i);
			//System.out.println("initializeMetadataObjects currAttName "+currAttName);
			//String attributeHash = Utils.getSHA1(attributeName);
			NodeIDType respNodeId = getResponsibleNodeId(currAttName);
			//System.out.println("InitializeMetadataObjects currAttName "+currAttName
			//		+" respNodeID "+respNodeId);
			// This node is responsible(meta data)for this Att.
			if(respNodeId == getMyID() )
			{
				ContextServiceLogger.getLogger().info("Node Id "+getMyID() +
						" meta data node for attribute "+currAttName);
				// FIXME: set proper min max value, probably read attribute names and its min max value from file.
				//AttributeMetadataInformation<NodeIDType> attrMeta = 
				//		new AttributeMetadataInformation<NodeIDType>(currAttName, AttributeTypes.MIN_VALUE, 
				//				AttributeTypes.MAX_VALUE, csNode);
				
				AttributeMetadataInfoRecord<NodeIDType, Double> attrMetaRec =
						new AttributeMetadataInfoRecord<NodeIDType, Double>
				(currAttName, AttributeTypes.MIN_VALUE, AttributeTypes.MAX_VALUE);
				
				getContextServiceDB().putAttributeMetaInfoRecord(attrMetaRec);
				
				//csNode.addMetadataInfoRec(attrMetaRec);
				//;addMetadataList(attrMeta);
				//GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] messageTasks = 
				//		attrMeta.assignValueRanges(csNode.getMyID());
				
				assignValueRanges(getMyID(), attrMetaRec);
				
//				// add all the messaging tasks at different value nodes
//				for(int j=0;j<messageTasks.length;j++)
//				{
//					messageList.add(messageTasks[j]);
//				}
			}
		}
//		GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] returnArr 
//					= new GenericMessagingTask[messageList.size()];
//		for(int i=0;i<messageList.size();i++)
//		{
//			returnArr[i] = messageList.get(i);
//		}
//		System.out.println("\n\n csNode.getMyID() "+getMyID()+" returnArr size "+returnArr.length +" messageList.size() "+messageList.size()+"\n\n");
		return null;
	}
	
	/****************************** End of protocol task handler methods *********************/
	/*********************** Private methods below **************************/
	/**
	 * Query req received here means that
	 * no group exists in the GNS
	 * @param queryMsgFromUser
	 * @return
	 */
	private GenericMessagingTask<NodeIDType, QueryMsgToValuenode<NodeIDType>>[] 
			processQueryMsgFromUser(QueryMsgFromUser<NodeIDType> queryMsgFromUser)
	{
		LinkedList<GenericMessagingTask<NodeIDType,QueryMsgToValuenode<NodeIDType>>> msgList
		 = new LinkedList<GenericMessagingTask<NodeIDType,QueryMsgToValuenode<NodeIDType>>>();
		
		String query = queryMsgFromUser.getQuery();
		long userReqID = queryMsgFromUser.getUserReqNum();
		String userIP = queryMsgFromUser.getSourceIP();
		int userPort = queryMsgFromUser.getSourcePort();
		
		System.out.println("QUERY RECVD QUERY_MSG recvd query recvd "+query);
		
		// create the empty group in GNS
		String grpGUID = GNSCalls.createQueryGroup(query);
		
		Vector<QueryComponent> qcomponents = QueryParser.parseQuery(query);
		
		QueryInfo<NodeIDType> currReq = new QueryInfo<NodeIDType>(query, getMyID(),
				grpGUID, userReqID, userIP, userPort, qcomponents);
		
		synchronized(this.pendingQueryLock)
		{	
			//StartContextServiceNode.sendQueryForProcessing(qinfo);
			//currReq.setRequestId(requestIdCounter);
			//requestIdCounter++;
			
			currReq.setQueryRequestID(queryIdCounter++);
			pendingQueryRequests.put(currReq.getRequestId(), currReq);
		}
		
		/*if(ContextServiceConfig.EXP_PRINT_ON)
		{
			System.out.println("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSER REQUEST ID "
						+currReq.getRequestId()+" NUMATTR "+qcomponents.size()+" AT "+System.currentTimeMillis()
						+" "+qcomponents.get(0).getAttributeName()+" QueryStart "+queryStart);
		}*/
		
		
		Iterator<NodeIDType> iter = this.allNodeIDs.iterator();
		while ( iter.hasNext() )
			//for(int i=0;i<this.allNodeIDs.size();i++)
		{
			NodeIDType currNodeID = iter.next();
			
			QueryMsgToValuenode<NodeIDType> queryMsgToValnode 
			= new QueryMsgToValuenode<NodeIDType>( this.getMyID(), qcomponents.get(0),
					currReq.getRequestId(), this.getMyID(),
				query, grpGUID, this.allNodeIDs.size() );
		
			ContextServiceLogger.getLogger().info("Sending ValueNodeMessage from" 
				+ this.getMyID() +" to node "+currNodeID + 
				" query "+query);
		
			GenericMessagingTask<NodeIDType, QueryMsgToValuenode<NodeIDType>> mtask = 
					new GenericMessagingTask<NodeIDType, QueryMsgToValuenode<NodeIDType>>(currNodeID, queryMsgToValnode);
			//relaying the query to the value nodes of the attribute
			msgList.add(mtask);
		}
		return 
		(GenericMessagingTask<NodeIDType, QueryMsgToValuenode<NodeIDType>>[]) this.convertLinkedListToArray(msgList);
	}
	
	/**
	 * adds the reply of the queryComponent
	 * @throws JSONException
	 */
	private GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenode<NodeIDType>>[]
	                   processValueUpdateFromGNS(ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS)
	{
		ContextServiceLogger.getLogger().info("\n\n Recvd ValueUpdateFromGNS at " 
				+ this.getMyID() +" reply "+valUpdMsgFromGNS);
		
		long versionNum = valUpdMsgFromGNS.getVersionNum();
		String GUID = valUpdMsgFromGNS.getGUID();
		String attrName = "";
		String oldVal   = "";
		String newVal   = "";
		JSONObject allAttrs = new JSONObject();
		//String sourceIP = valUpdMsgFromGNS.getSourceIP();
		//int sourcePort = valUpdMsgFromGNS.getSourcePort();
		
		double oldValD, newValD;
		
		if(oldVal.equals(""))
		{
			oldValD = AttributeTypes.NOT_SET;
		} else
		{
			oldValD = Double.parseDouble(oldVal);
		}
		newValD = Double.parseDouble(newVal );
		
		long currReqID = -1;
		
		UpdateInfo<NodeIDType> currReq = null;
		
		/*synchronized(this.pendingUpdateLock)
		{
			currReq 
				= new UpdateInfo<NodeIDType>(valUpdMsgFromGNS, updateIdCounter++);
			currReqID = currReq.getRequestId();
			pendingUpdateRequests.put(currReqID, currReq);
		}*/
		
	
		ContextServiceLogger.getLogger().info("ValueUpdateToMetadataMesg recvd at " 
				+ this.getMyID() +" for GUID "+GUID+
				" "+attrName + " "+oldVal+" "+newVal);
			
		//LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>> 
		// there should be just one element in the list, or definitely at least one.
		LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>> oldMetaObjRecList = 
				(LinkedList<AttributeMetaObjectRecord<NodeIDType, Double>>) 
				this.getContextServiceDB().getAttributeMetaObjectRecord(attrName, 
													oldValD, oldValD);
	
		AttributeMetaObjectRecord<NodeIDType, Double> oldMetaObjRec = null;
	
		if( oldMetaObjRecList.size() > 0 )
		{
			oldMetaObjRec = this.getContextServiceDB().getAttributeMetaObjectRecord(attrName, oldValD, newValD).get(0);
		}
		//oldMetaObj = new AttributeMetadataObject<NodeIDType>();
	
		// same thing for the newValue
		AttributeMetaObjectRecord<NodeIDType, Double> newMetaObjRec = 
				this.getContextServiceDB().getAttributeMetaObjectRecord(attrName, newValD, newValD).get(0);
			
		if( ContextServiceConfig.GROUP_INFO_COMPONENT )
		{
			// do group updates for the old value
			try
			{
				if( oldMetaObjRec != null )
				{
					LinkedList<GroupGUIDRecord> oldValueGroups = getGroupsAffectedUsingDatabase
							(oldMetaObjRec, allAttrs, attrName, oldValD);
					
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
				if(newMetaObjRec!=null)
				{
					LinkedList<GroupGUIDRecord> newValueGroups = getGroupsAffectedUsingDatabase
							(newMetaObjRec, allAttrs, attrName, newValD);
							
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
		
		//FIXME: may need atomicity here
		// just a value update, but goes to the same node
		//remove
		/*valueObj.removeNodeGUID(GUID);
		
		// and add
		NodeGUIDInfo nodeGUIDObj = new NodeGUIDInfo(GUID, newValue, versionNum);
		valueObj.addNodeGUID(nodeGUIDObj);*/
		
		List<ValueInfoObjectRecord<Double>> valueInfoObjRecList = 
				this.contextserviceDB.getValueInfoObjectRecord(attrName, newValD, newValD);
		
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
						(GUID, newValD, versionNum, new JSONObject());
					
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
		//sendUpdateReplyBackToUser(sourceIP, sourcePort, versionNum);
		
		/*synchronized(this.pendingUpdateLock)
		{
			pendingUpdateRequests.remove(currReqID);
		}*/
		return null;
	}
	
	
	private LinkedList<GroupGUIDRecord> getGroupsAffectedUsingDatabase
	(AttributeMetaObjectRecord<NodeIDType, Double> metaObjRec, JSONObject allAttr, 
			String updateAttrName, double attrVal) throws JSONException
	{
		LinkedList<GroupGUIDRecord> satisfyingGroups = new LinkedList<GroupGUIDRecord>();
		JSONArray groupGUIDList = metaObjRec.getGroupGUIDList();
		
		for(int i=0;i<groupGUIDList.length();i++)
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
	 * Processes the QueryMsgToValuenode and replies with 
	 * QueryMsgToValuenodeReply, which contains the GUIDs
	 */
	@SuppressWarnings("unchecked")
	private GenericMessagingTask<NodeIDType, QueryMsgToValuenodeReply<NodeIDType>>[]
			processQueryMsgToValuenode(QueryMsgToValuenode<NodeIDType> queryMsgToValnode)
	{
		LinkedList<GenericMessagingTask<NodeIDType, QueryMsgToValuenodeReply<NodeIDType>>> msgList
		 = new LinkedList<GenericMessagingTask<NodeIDType, QueryMsgToValuenodeReply<NodeIDType>>>();
		
		String query = queryMsgToValnode.getQuery();
		ContextServiceLogger.getLogger().info("QUERY_MSG recvd query recvd "+query);
		
		
		//QueryInfo<NodeIDType> currReq = null;
		Vector<QueryComponent> qcomponents = QueryParser.parseQuery(query);
		
		LinkedList<LinkedList<String>> predicateReplies = new LinkedList<LinkedList<String>>();
		
		for (int i=0;i<qcomponents.size();i++)
		{
			QueryComponent qc = qcomponents.elementAt(i);
			
			LinkedList<String> resultGUIDs = new LinkedList<String>();
			
		    List<ValueInfoObjectRecord<Double>> valInfoObjRecList = 
					this.contextserviceDB.getValueInfoObjectRecord
						(qc.getAttributeName(), qc.getLeftValue(), qc.getRightValue());
			
			for(int j=0;j<valInfoObjRecList.size();j++)
			{
				ValueInfoObjectRecord<Double> valueObjRec = valInfoObjRecList.get(j);
				
				//resultGUIDs.add(nodeGUIDRecList.get(i).getNodeGUID());
				
				JSONArray nodeGUIDList = valueObjRec.getNodeGUIDList();
				
				for(int k=0;k<nodeGUIDList.length();k++)
				{
					try
					{
						JSONObject nodeGUIDJSON = nodeGUIDList.getJSONObject(k);
						NodeGUIDInfoRecord<Double> nodeGUIDRec = 
								new NodeGUIDInfoRecord<Double>(nodeGUIDJSON);
						
						
						if( Utils.checkQCForOverlapWithValue(nodeGUIDRec.getAttrValue(), qc))
						{
									resultGUIDs.add(nodeGUIDRec.getNodeGUID());
						}
					}
					catch(JSONException jso)
					{
						jso.printStackTrace();
					}
				}
			}
			predicateReplies.add(resultGUIDs);
		}
		
		JSONArray queryAnswer = Utils.doConjuction(predicateReplies);
		
		//System.out.println("\n\nQuery Answer "+queryAnswer);
		
		
		QueryMsgToValuenodeReply<NodeIDType> queryMsgToValReply 
			= new QueryMsgToValuenodeReply<NodeIDType>(getMyID(), queryAnswer, 
					queryMsgToValnode.getRequestId(), 0, getMyID(), queryMsgToValnode.getNumValNodesContacted());
	
		GenericMessagingTask<NodeIDType, QueryMsgToValuenodeReply<NodeIDType>> mtask = 
			new GenericMessagingTask<NodeIDType, QueryMsgToValuenodeReply<NodeIDType>>
			(queryMsgToValnode.getSourceId(), queryMsgToValReply);
			//relaying the query to the value nodes of the attribute
	
		msgList.add(mtask);
		ContextServiceLogger.getLogger().info("Sending QueryMsgToValuenodeReply from " 
					+ this.getMyID() +" to node "+queryMsgToValnode.getSourceId()+
					" reply "+queryMsgToValReply.toString());
	
		return 
		(GenericMessagingTask<NodeIDType, QueryMsgToValuenodeReply<NodeIDType>>[]) this.convertLinkedListToArray(msgList);
		//(GenericMessagingTask<NodeIDType, QueryMsgToMetadataNode<NodeIDType>>[]) this.convertLinkedListToArray(messageList);
		//return (GenericMessagingTask<NodeIDType, QueryMsgToMetadataNode<NodeIDType>>[]) messageList.toArray();
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
		long requestId =  queryMsgToValnodeRep.getRequestID();
		synchronized(valueNodeReplyLock)
		{
			QueryInfo<NodeIDType> queryInfo = pendingQueryRequests.get(requestId);
			if(queryInfo != null)
			{
				processReplyInternally(queryMsgToValnodeRep, queryInfo);
			}
		}
	}
	
	/**
	 * For Query-All/Replicate-All it just assigns to itself.
	 */
	private GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] 
			assignValueRanges(NodeIDType initiator, AttributeMetadataInfoRecord<NodeIDType, Double> attrMetaRec)
	{
		int numValueNodes = 1;
		//@SuppressWarnings("unchecked")
		//GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] mesgArray 
		//						= new GenericMessagingTask[numValueNodes];
		
		double attributeMin = attrMetaRec.getAttrMin();
		double attributeMax = attrMetaRec.getAttrMax();
		

		//String attributeHash = Utils.getSHA1(attributeName);
		//int mapIndex = Hashing.consistentHash(attrMetaRec.getAttrName().hashCode(), numNodes);
			
		for(int i=0;i<numValueNodes;i++)
		{
			double rangeSplit = (attributeMax - attributeMin)/numValueNodes;
			double currMinRange = attributeMin + rangeSplit*i;
			double currMaxRange = attributeMin + rangeSplit*(i+1);
			
			if( currMaxRange > attributeMax )
			{
				currMaxRange = attributeMax;
			}
			
			NodeIDType currNodeID = this.getMyID();
			
			// add this to database, not to memory
			AttributeMetaObjectRecord<NodeIDType, Double> attrMetaObjRec = new
			AttributeMetaObjectRecord<NodeIDType, Double>(currMinRange, currMaxRange,
						currNodeID, new JSONArray());
				
			this.getContextServiceDB().putAttributeMetaObjectRecord(attrMetaObjRec, attrMetaRec.getAttrName());
			
			
			MetadataMsgToValuenode<NodeIDType> metaMsgToValnode = new MetadataMsgToValuenode<NodeIDType>
							( initiator, attrMetaRec.getAttrName(), currMinRange, currMaxRange);
			
//			GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>> mtask = new GenericMessagingTask<NodeIDType, 
//					MetadataMsgToValuenode<NodeIDType>>((NodeIDType) currNodeID, metaMsgToValnode);
//			
//			mesgArray[i] = mtask;
			
			String attrName = metaMsgToValnode.getAttrName();
			double rangeStart = metaMsgToValnode.getRangeStart();
			double rangeEnd = metaMsgToValnode.getRangeEnd();
			
			ContextServiceLogger.getLogger().info("METADATA_MSG recvd at node " + 
					this.getMyID()+" attriName "+attrName + 
					" rangeStart "+rangeStart+" rangeEnd "+rangeEnd);
			
			
			ValueInfoObjectRecord<Double> valInfoObjRec = new ValueInfoObjectRecord<Double>
													(rangeStart, rangeEnd, new JSONArray());
			
			this.contextserviceDB.putValueObjectRecord(valInfoObjRec, attrName);
			
			
			ContextServiceLogger.getLogger().info("csID "+getMyID()+" Metadata Message attribute "+
			attrMetaRec.getAttrName()+"dest "+currNodeID+" min range "+currMinRange+" max range "+currMaxRange);
			
			//JSONObject metadataJSON = metadata.getJSONMessage();
			//ContextServiceLogger.getLogger().info("Metadata Message attribute "+attributeName+
			//		"dest "+currNodeID+" min range "+currMinRange+" max range "+currMaxRange);
			// sending the message
			//StartContextServiceNode.sendToNIOTransport(currNodeID, metadataJSON);
		}
		return null;
	}
	
	
	protected void processReplyInternally
			(QueryMsgToValuenodeReply<NodeIDType> queryMsgToValnodeRep, QueryInfo<NodeIDType> queryInfo) 
	{
		int replyId = (Integer)queryMsgToValnodeRep.getSourceID();
		LinkedList<String> GUIDs = queryInfo.componentReplies.get(replyId);
		if(GUIDs == null)
		{
			GUIDs = new LinkedList<String>();
			JSONArray recvArr = queryMsgToValnodeRep.getResultGUIDs();
			//System.out.println("JSONArray size "+recvArr.length() +" "+recvArr);
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
			queryInfo.componentReplies.put(replyId, GUIDs);
		}
		else
		{
			// merge with exiting GUIDs
			JSONArray recvArr = queryMsgToValnodeRep.getResultGUIDs();
			//System.out.println("JSONArray size "+recvArr.length() +" "+recvArr);
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
			queryInfo.componentReplies.put(replyId, GUIDs);
		}
		
		checkQueryCompletion(queryInfo);
		//System.out.println("componentReplies.size() "+componentReplies.size() +
		//		" queryComponents.size() "+queryComponents.size());
		// if there is at least one replies recvd for each component
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
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleBulkGetReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleConsistentStoragePut(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public GenericMessagingTask<NodeIDType, ?>[] handleConsistentStoragePutReply(
			ProtocolEvent<PacketType, String> event,
			ProtocolTask<NodeIDType, PacketType, String>[] ptasks) {
		// TODO Auto-generated method stub
		return null;
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
	
	/*private boolean isExternalRequest(JSONObject json) throws JSONException
	{
		ContextServicePacket.PacketType csType = 
				ContextServicePacket.getContextServicePacketType(json);
		
		// trigger from GNS
		if(csType == ContextServicePacket.PacketType.VALUE_UPDATE_MSG_FROM_GNS)
		{
			return true;
		} else
		{
			return false;
		}
	}*/
}