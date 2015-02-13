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
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ContextServicePacket;
import edu.umass.cs.contextservice.messages.MetadataMsgToValuenode;
import edu.umass.cs.contextservice.messages.QueryMsgFromUser;
import edu.umass.cs.contextservice.messages.QueryMsgToMetadataNode;
import edu.umass.cs.contextservice.messages.QueryMsgToValuenodeReply;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
import edu.umass.cs.contextservice.messages.ValueUpdateMsgToValuenode;
import edu.umass.cs.contextservice.messages.ValueUpdateMsgToValuenodeReply;
import edu.umass.cs.contextservice.processing.QueryComponent;
import edu.umass.cs.contextservice.processing.QueryInfo;
import edu.umass.cs.contextservice.processing.QueryParser;
import edu.umass.cs.contextservice.processing.UpdateInfo;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.gns.main.GNS;
import edu.umass.cs.gns.nio.GenericMessagingTask;
import edu.umass.cs.gns.nio.InterfaceNodeConfig;
import edu.umass.cs.gns.nio.InterfacePacketDemultiplexer;
import edu.umass.cs.gns.nio.JSONMessenger;
import edu.umass.cs.gns.nio.NIOTransport;
import edu.umass.cs.gns.protocoltask.ProtocolEvent;
import edu.umass.cs.gns.protocoltask.ProtocolTask;

public class ReplicateAllScheme<NodeIDType> extends AbstractScheme<NodeIDType> implements InterfacePacketDemultiplexer 
{
	public static final Logger log =
			NIOTransport.LOCAL_LOGGER ? Logger.getLogger(NIOTransport.class.getName())
					: GNS.getLogger();
	
	//FIXME: sourceID is not properly set, it is currently set to sourceID of each node,
	// it needs to be set to the origin sourceID.
	// Any id-based communication requires NodeConfig and Messenger
	public ReplicateAllScheme(InterfaceNodeConfig<NodeIDType> nc, JSONMessenger<NodeIDType> m)
	{
		super(nc, m);
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleQueryMsgFromUser(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		@SuppressWarnings("unchecked")
		QueryMsgFromUser<NodeIDType> queryMsgFromUser = (QueryMsgFromUser<NodeIDType>)event;
		
		GenericMessagingTask<NodeIDType, QueryMsgToMetadataNode<NodeIDType>>[] retMsgs
				= processQueryMsgFromUser(queryMsgFromUser);
		
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
		/* Actions:
		 * just update / add or remove the entry
		 */
		@SuppressWarnings("unchecked")
		ValueUpdateFromGNS<NodeIDType> valUpdMsgFromGNS = (ValueUpdateFromGNS<NodeIDType>)event;
		System.out.println("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgFromGNS);
		
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
	
	public GenericMessagingTask<NodeIDType,?>[] handleValueUpdateMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		@SuppressWarnings("unchecked")
		ValueUpdateMsgToValuenode<NodeIDType> valUpdMsgToValnode = (ValueUpdateMsgToValuenode<NodeIDType>)event;
		System.out.println("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgToValnode);
		
		GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenodeReply<NodeIDType>>[] retMsgs
				= this.processValueUpdateMsgToValuenode(valUpdMsgToValnode);
		
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
	
	public GenericMessagingTask<NodeIDType,?>[] handleValueUpdateMsgToValuenodeReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * just update / add or remove the entry
		 */
		@SuppressWarnings("unchecked")
		ValueUpdateMsgToValuenodeReply<NodeIDType> valUpdMsgToValnode = (ValueUpdateMsgToValuenodeReply<NodeIDType>)event;
		System.out.println("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgToValnode);
		this.processValueUpdateMsgToValuenodeReply(valUpdMsgToValnode);
		return null;
	}
	
	// may not be used here.
	public GenericMessagingTask<NodeIDType,?>[] handleQueryMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
			return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleQueryMsgToMetadataNode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<NodeIDType, ContextServicePacket.PacketType, String>[] ptasks)
	{
		return null;
	}
	
	public GenericMessagingTask<NodeIDType,?>[] handleQueryMsgToValuenodeReply(
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
		return this.getMyID();
	}
	/****************************** End of protocol task handler methods *********************/

	/*********************** Private methods below **************************/
	/**
	 * Query req received here means that
	 * no group exists in the GNS
	 * @param queryMsgFromUser
	 * @return
	 */
	public GenericMessagingTask<NodeIDType, QueryMsgToMetadataNode<NodeIDType>>[] 
			processQueryMsgFromUser(QueryMsgFromUser<NodeIDType> queryMsgFromUser)
	{	
		String query = queryMsgFromUser.getQuery();
		long userReqID = queryMsgFromUser.getUserReqNum();
		String userIP = queryMsgFromUser.getSourceIP();
		int userPort = queryMsgFromUser.getSourcePort();
		
		ContextServiceLogger.getLogger().info("QUERY_MSG recvd query recvd "+query);
		
		long queryStart = System.currentTimeMillis();
		// create the empty group in GNS
		String grpGUID = GNSCalls.createQueryGroup(query);
		
		QueryInfo<NodeIDType> currReq = null;
		Vector<QueryComponent> qcomponents = null;
		
		synchronized(this.pendingQueryLock)
		{
			currReq = new QueryInfo<NodeIDType>(query, getMyID(),
					queryIdCounter++, grpGUID, this, userReqID, userIP, userPort);
			
			//StartContextServiceNode.sendQueryForProcessing(qinfo);
			//currReq.setRequestId(requestIdCounter);
			//requestIdCounter++;
			
			qcomponents = QueryParser.parseQuery(currReq.getQuery());
			currReq.setQueryComponents(qcomponents);
			pendingQueryRequests.put(currReq.getRequestId(), currReq);
		}
		
		if(ContextServiceConfig.EXP_PRINT_ON)
		{
			System.out.println("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSER REQUEST ID "
						+currReq.getRequestId()+" NUMATTR "+qcomponents.size()+" AT "+System.currentTimeMillis()
						+" "+qcomponents.get(0).getAttributeName()+" QueryStart "+queryStart);
		}
		
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
		System.out.println("\n\nQuery Answer "+queryAnswer);
		
		long qprocessingTime = System.currentTimeMillis();
		
		//FIXME: uncomment this, just for debugging
		GNSCalls.addGUIDsToGroup(queryAnswer, query);
		
		long queryEndTime = System.currentTimeMillis();
		
		if(ContextServiceConfig.EXP_PRINT_ON)
		{
			System.out.println("CONTEXTSERVICE EXPERIMENT: QUERYFROMUSERREPLY REQUEST ID "
						+currReq.getRequestId()+" NUMATTR "+qcomponents.size()+" AT "+qprocessingTime+" EndTime "
					+queryEndTime+ " QUERY ANSWER "+queryAnswer);
		}
		sendReplyBackToUser(currReq, (LinkedList<String>) Utils.JSONArayToList(queryAnswer));
		
		synchronized(this.pendingQueryLock)
		{
			this.pendingQueryRequests.remove(currReq.getRequestId());
		}
		
		return null;
		//(GenericMessagingTask<NodeIDType, QueryMsgToMetadataNode<NodeIDType>>[]) this.convertLinkedListToArray(messageList);
		//return (GenericMessagingTask<NodeIDType, QueryMsgToMetadataNode<NodeIDType>>[]) messageList.toArray();
	}
	
	
	public GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] initializeScheme()
	{
		System.out.println("\n\n\n" +
				"In initializeMetadataObjects NodeId "+getMyID()+"\n\n\n");
		
		LinkedList<GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>> messageList = 
				new  LinkedList<GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>>();
		
		Vector<String> attributes = AttributeTypes.getAllAttributes();
		for(int i=0;i<attributes.size(); i++)
		{
			String currAttName = attributes.get(i);
			System.out.println("initializeMetadataObjects currAttName "+currAttName);
			//String attributeHash = Utils.getSHA1(attributeName);
			NodeIDType respNodeId = getResponsibleNodeId(currAttName);
			System.out.println("InitializeMetadataObjects currAttName "+currAttName
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
				
				AttributeMetadataInfoRecord<NodeIDType, Double> attrMetaRec =
						new AttributeMetadataInfoRecord<NodeIDType, Double>
				(currAttName, AttributeTypes.MIN_VALUE, AttributeTypes.MAX_VALUE);
				
				getContextServiceDB().putAttributeMetaInfoRecord(attrMetaRec);
				
				//csNode.addMetadataInfoRec(attrMetaRec);
				//;addMetadataList(attrMeta);
				//GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] messageTasks = 
				//		attrMeta.assignValueRanges(csNode.getMyID());
				
				GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] messageTasks 
						= assignValueRanges(getMyID(), attrMetaRec);
				
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
	
	
	/**
	 * adds the reply of the queryComponent
	 * @throws JSONException
	 */
	private  GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenodeReply<NodeIDType>>[]
			processValueUpdateMsgToValuenode(ValueUpdateMsgToValuenode<NodeIDType> valUpdateMsgToValnode)
	{
		ContextServiceLogger.getLogger().info("\n\n Recvd ValueUpdateMsgToValuenode at " 
				+ this.getMyID() +" reply "+valUpdateMsgToValnode);
		
		LinkedList<GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenodeReply<NodeIDType>>> msgList
			= new LinkedList<GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenodeReply<NodeIDType>>>();
		
		String attrName = valUpdateMsgToValnode.getAttrName();
		String GUID = valUpdateMsgToValnode.getGUID();
		double oldValue = valUpdateMsgToValnode.getOldValue();
		double newValue = valUpdateMsgToValnode.getNewValue();
		long versionNum = valUpdateMsgToValnode.getVersionNum();
		long requestID = valUpdateMsgToValnode.getRequestID();
		NodeIDType sourceID = valUpdateMsgToValnode.getSourceID();
		
		// first check whole value ranges to see if this GUID exists and check the version number
		// of update
		
		//FIXME: need to think about consistency, update only for newer version numbers.
		boolean doOperation = true;
		
		if(doOperation)
		{
			switch(valUpdateMsgToValnode.getOperType())
			{
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
					break;
				}
			}
		}
		
		ValueUpdateMsgToValuenodeReply<NodeIDType> newValueUpdateMsgReply
			= new ValueUpdateMsgToValuenodeReply<NodeIDType>
				(this.getMyID(), versionNum, this.allNodeIDs.size(), requestID);
		
		GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenodeReply<NodeIDType>> newmtask = 
				new GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenodeReply<NodeIDType>>
			(sourceID, newValueUpdateMsgReply);
		
		msgList.add(newmtask);
		return 
		(GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenodeReply<NodeIDType>>[]) this.convertLinkedListToArray(msgList);
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
		String attrName = valUpdMsgFromGNS.getAttrName();
		String oldVal = valUpdMsgFromGNS.getOldVal();
		String newVal = valUpdMsgFromGNS.getNewVal();
		JSONObject allAttrs = valUpdMsgFromGNS.getAllAttrs();
		String sourceIP = valUpdMsgFromGNS.getSourceIP();
		int sourcePort = valUpdMsgFromGNS.getSourcePort();
		
		double oldValD, newValD;
		
		if(oldVal.equals(""))
		{
			oldValD = AttributeTypes.NOT_SET;
		} else
		{
			oldValD = Double.parseDouble(oldVal);
		}
		newValD = Double.parseDouble(newVal );
		
	
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
	
		if(oldMetaObjRecList.size()>0)
		{
			oldMetaObjRec = this.getContextServiceDB().getAttributeMetaObjectRecord(attrName, oldValD, newValD).get(0);
		}
		//oldMetaObj = new AttributeMetadataObject<NodeIDType>();
	
		// same thing for the newValue
		AttributeMetaObjectRecord<NodeIDType, Double> newMetaObjRec = 
				this.getContextServiceDB().getAttributeMetaObjectRecord(attrName, newValD, newValD).get(0);
			
		// do group updates for the old value
		try
		{
			if(oldMetaObjRec!=null)
			{
				LinkedList<GroupGUIDRecord> oldValueGroups = getGroupsAffectedUsingDatabase
						(oldMetaObjRec, allAttrs, attrName, oldValD);
			
				//oldMetaObj.getGroupsAffected(allAttr, updateAttrName, oldVal);
			
				GNSCalls.userGUIDAndGroupGUIDOperations
				(GUID, oldValueGroups, GNSCalls.UserGUIDOperations.REMOVE_USER_GUID_FROM_GROUP);
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
				(GUID, newValueGroups, GNSCalls.UserGUIDOperations.ADD_USER_GUID_TO_GROUP);
			} else
			{
				assert(false);
			}
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
		
		LinkedList<GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenode<NodeIDType>>> msgList
			= new LinkedList<GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenode<NodeIDType>>>();
		
		long currReqID = -1;
		synchronized(this.pendingUpdateLock)
		{
			UpdateInfo<NodeIDType> currReq 
				= new UpdateInfo<NodeIDType>(valUpdMsgFromGNS, updateIdCounter++);
			currReqID = currReq.getRequestId();
			pendingUpdateRequests.put(currReqID, currReq);
		}
		
		Iterator<NodeIDType> iter = this.allNodeIDs.iterator();
		while ( iter.hasNext() )
			//for(int i=0;i<this.allNodeIDs.size();i++)
		{
			NodeIDType currNodeID = iter.next();
		
			ValueUpdateMsgToValuenode<NodeIDType> valueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<NodeIDType>
			(this.getMyID(), versionNum, GUID, attrName, oldValD, newValD, 
				ValueUpdateMsgToValuenode.REMOVE_ADD_BOTH, new JSONObject(), this.getMyID(), currReqID);
			
			GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenode<NodeIDType>> mtask = 
					new GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenode<NodeIDType>>
					(currNodeID, valueUpdateMsgToValnode);
					//relaying the query to the value nodes of the attribute
			msgList.add(mtask);
			
			ContextServiceLogger.getLogger().info("Sending ValueUpdateMsgToValuenode from" 
				+ this.getMyID() + " to node "+ currNodeID +
				" mesg "+valueUpdateMsgToValnode);
		}
		return 
		(GenericMessagingTask<NodeIDType, ValueUpdateMsgToValuenode<NodeIDType>>[]) this.convertLinkedListToArray(msgList);
	}
	
	private void 
	processValueUpdateMsgToValuenodeReply(ValueUpdateMsgToValuenodeReply<NodeIDType> valUpdateMsgToValnodeRep)
	{
		long requestId =  valUpdateMsgToValnodeRep.getRequestID();
		UpdateInfo<NodeIDType> updateInfo = pendingUpdateRequests.get(requestId);
		if(updateInfo != null)
		{
			updateInfo.incrementNumReplyRecvd();
			if(updateInfo.getNumReplyRecvd() == valUpdateMsgToValnodeRep.getNumReply())
			{
				sendUpdateReplyBackToUser(updateInfo.getValueUpdateFromGNS().getSourceIP(), 
						updateInfo.getValueUpdateFromGNS().getSourcePort(), updateInfo.getValueUpdateFromGNS().getVersionNum());
				
				synchronized(this.pendingUpdateLock)
				{
					pendingUpdateRequests.remove(requestId);
				}
			}
		}
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
	 * For Query-All/Replicate-All it just assigns to itself.
	 */
	private GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] 
			assignValueRanges(NodeIDType initiator, AttributeMetadataInfoRecord<NodeIDType, Double> attrMetaRec)
	{
		int numValueNodes = 1;
		@SuppressWarnings("unchecked")
		GenericMessagingTask<NodeIDType, MetadataMsgToValuenode<NodeIDType>>[] mesgArray 
								= new GenericMessagingTask[numValueNodes];
		
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
	

	@Override
	protected void processReplyInternally(
			QueryMsgToValuenodeReply<NodeIDType> queryMsgToValnodeRep,
			QueryInfo<NodeIDType> queryInfo) 
	{
		
	}

	@Override
	public void checkQueryCompletion(QueryInfo<NodeIDType> qinfo) {
		// TODO Auto-generated method stub
		
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