package edu.umass.cs.contextservice.schemes.old;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.logging.Logger;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

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
import edu.umass.cs.contextservice.messages.QueryMsgToValuenodeReply;
import edu.umass.cs.contextservice.messages.ValueUpdateFromGNS;
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

public class ReplicateAllScheme<Integer> extends AbstractScheme<Integer>
{
	public static final Logger log = Logger.getLogger(ReplicateAllScheme.class.getName());
	
	private final Object valueNodeReplyLock = new Object();
	
	//FIXME: sourceID is not properly set, it is currently set to sourceID of each node,
	// it needs to be set to the origin sourceID.
	// Any id-based communication requires NodeConfig and Messenger
	public ReplicateAllScheme(InterfaceNodeConfig<Integer> nc, JSONMessenger<Integer> m)
	{
		super(nc, m);
	}
	
	public GenericMessagingTask<Integer,?>[] handleQueryMsgFromUser(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		@SuppressWarnings("unchecked")
		QueryMsgFromUser<Integer> queryMsgFromUser = (QueryMsgFromUser<Integer>)event;
		
		GenericMessagingTask<Integer, QueryMsgToMetadataNode<Integer>>[] retMsgs
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
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateFromGNS(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * just update / add or remove the entry
		 */
		@SuppressWarnings("unchecked")
		ValueUpdateFromGNS<Integer> valUpdMsgFromGNS = (ValueUpdateFromGNS<Integer>)event;
		//ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgFromGNS);
		
		GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>>[] retMsgs
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
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		@SuppressWarnings("unchecked")
		ValueUpdateMsgToValuenode<Integer> valUpdMsgToValnode = (ValueUpdateMsgToValuenode<Integer>)event;
		//ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgToValnode);
		
		GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>[] retMsgs
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
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateMsgToValuenodeReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		/* Actions:
		 * just update / add or remove the entry
		 */
		@SuppressWarnings("unchecked")
		ValueUpdateMsgToValuenodeReply<Integer> valUpdMsgToValnode = (ValueUpdateMsgToValuenodeReply<Integer>)event;
		//ContextServiceLogger.getLogger().fine("CS"+getMyID()+" received " + event.getType() + ": " + valUpdMsgToValnode);
		this.processValueUpdateMsgToValuenodeReply(valUpdMsgToValnode);
		return null;
	}
	
	// may not be used here.
	public GenericMessagingTask<Integer,?>[] handleQueryMsgToValuenode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
			return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleQueryMsgToMetadataNode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleQueryMsgToValuenodeReply(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
		return null;
	}
	
	public GenericMessagingTask<Integer,?>[] handleValueUpdateMsgToMetadataNode(
			ProtocolEvent<ContextServicePacket.PacketType, String> event,
			ProtocolTask<Integer, ContextServicePacket.PacketType, String>[] ptasks)
	{
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
	public GenericMessagingTask<Integer, QueryMsgToMetadataNode<Integer>>[] 
			processQueryMsgFromUser(QueryMsgFromUser<Integer> queryMsgFromUser)
	{
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
		//ContextServiceLogger.getLogger().fine("\n\nQuery Answer "+queryAnswer);
		
		//FIXME: uncomment this, just for debugging
		GNSCalls.addGUIDsToGroup(queryAnswer, query, grpGUID);
		
		sendReplyBackToUser(currReq, queryAnswer);
		
		synchronized(this.pendingQueryLock)
		{
			this.pendingQueryRequests.remove(currReq.getRequestId());
		}
		
		return null;
	}
	
	
	public GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] initializeScheme()
	{	
		//LinkedList<GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>> messageList = 
		//		new  LinkedList<GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>>();
		
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
				
				assignValueRanges(getMyID(), attrMetaRec);
				
//				// add all the messaging tasks at different value nodes
//				for(int j=0;j<messageTasks.length;j++)
//				{
//					messageList.add(messageTasks[j]);
//				}
			}
		}
//		GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] returnArr 
//					= new GenericMessagingTask[messageList.size()];
//		for(int i=0;i<messageList.size();i++)
//		{
//			returnArr[i] = messageList.get(i);
//		}
//		ContextServiceLogger.getLogger().fine("\n\n csNode.getMyID() "+getMyID()+" returnArr size "+returnArr.length +" messageList.size() "+messageList.size()+"\n\n");
		return null;
	}
	
	
	/**
	 * adds the reply of the queryComponent
	 * @throws JSONException
	 */
	private  GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>[]
			processValueUpdateMsgToValuenode(ValueUpdateMsgToValuenode<Integer> valUpdateMsgToValnode)
	{
		ContextServiceLogger.getLogger().info("\n\n Recvd ValueUpdateMsgToValuenode at " 
				+ this.getMyID() +" reply "+valUpdateMsgToValnode);
		
		LinkedList<GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>> msgList
			= new LinkedList<GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>>();
		
		String attrName = valUpdateMsgToValnode.getAttrName();
		String GUID = valUpdateMsgToValnode.getGUID();
		//double oldValue = valUpdateMsgToValnode.getOldValue();
		double newValue = valUpdateMsgToValnode.getNewValue();
		long versionNum = valUpdateMsgToValnode.getVersionNum();
		long requestID = valUpdateMsgToValnode.getRequestID();
		//Integer sourceID = valUpdateMsgToValnode.getSourceID();
		
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
		
		ValueUpdateMsgToValuenodeReply<Integer> newValueUpdateMsgReply
			= new ValueUpdateMsgToValuenodeReply<Integer>
				(this.getMyID(), versionNum, this.allNodeIDs.size(), requestID);
		
		/*GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>> newmtask = 
				new GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>
			(sourceID, newValueUpdateMsgReply);
		
		msgList.add(newmtask);*/
		return 
		(GenericMessagingTask<Integer, ValueUpdateMsgToValuenodeReply<Integer>>[]) this.convertLinkedListToArray(msgList);
	}
	
	/**
	 * adds the reply of the queryComponent
	 * @throws JSONException
	 */
	private GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>>[]
	                   processValueUpdateFromGNS(ValueUpdateFromGNS<Integer> valUpdMsgFromGNS)
	{
		ContextServiceLogger.getLogger().info("\n\n Recvd ValueUpdateFromGNS at " 
				+ this.getMyID() +" reply "+valUpdMsgFromGNS);
		
		long versionNum = valUpdMsgFromGNS.getVersionNum();
		String GUID = valUpdMsgFromGNS.getGUID();
		String attrName = "";
		String oldVal = "";
		String newVal = "";
		JSONObject allAttrs = new JSONObject();
		
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
			
		//LinkedList<AttributeMetaObjectRecord<Integer, Double>> 
		// there should be just one element in the list, or definitely at least one.
		LinkedList<AttributeMetaObjectRecord<Integer, Double>> oldMetaObjRecList = 
				(LinkedList<AttributeMetaObjectRecord<Integer, Double>>) 
				this.getContextServiceDB().getAttributeMetaObjectRecord(attrName, 
													oldValD, oldValD);
	
		AttributeMetaObjectRecord<Integer, Double> oldMetaObjRec = null;
	
		if(oldMetaObjRecList.size()>0)
		{
			oldMetaObjRec = this.getContextServiceDB().getAttributeMetaObjectRecord(attrName, oldValD, newValD).get(0);
		}
		//oldMetaObj = new AttributeMetadataObject<Integer>();
	
		// same thing for the newValue
		AttributeMetaObjectRecord<Integer, Double> newMetaObjRec = 
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
		
		LinkedList<GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>>> msgList
			= new LinkedList<GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>>>();
		
		long currReqID = -1;
		/*synchronized(this.pendingUpdateLock)
		{
			UpdateInfo<Integer> currReq 
				= new UpdateInfo<Integer>(valUpdMsgFromGNS, updateIdCounter++);
			currReqID = currReq.getRequestId();
			pendingUpdateRequests.put(currReqID, currReq);
		}*/
		
		Iterator<Integer> iter = this.allNodeIDs.iterator();
		while ( iter.hasNext() )
			//for(int i=0;i<this.allNodeIDs.size();i++)
		{
			Integer currNodeID = iter.next();
		
			ValueUpdateMsgToValuenode<Integer> valueUpdateMsgToValnode = new ValueUpdateMsgToValuenode<Integer>
			(this.getMyID(), versionNum, GUID, attrName, oldValD, newValD, 
				ValueUpdateMsgToValuenode.REMOVE_ADD_BOTH, currReqID);
			
			GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>> mtask = 
					new GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>>
					(currNodeID, valueUpdateMsgToValnode);
					//relaying the query to the value nodes of the attribute
			msgList.add(mtask);
			
			ContextServiceLogger.getLogger().info("Sending ValueUpdateMsgToValuenode from" 
				+ this.getMyID() + " to node "+ currNodeID +
				" mesg "+valueUpdateMsgToValnode);
		}
		return 
		(GenericMessagingTask<Integer, ValueUpdateMsgToValuenode<Integer>>[]) this.convertLinkedListToArray(msgList);
	}
	
	private void 
	processValueUpdateMsgToValuenodeReply(ValueUpdateMsgToValuenodeReply<Integer> valUpdateMsgToValnodeRep)
	{
		/*long requestId =  valUpdateMsgToValnodeRep.getRequestID();
		synchronized(valueNodeReplyLock)
		{
			UpdateInfo<Integer> updateInfo = pendingUpdateRequests.get(requestId);
			if(updateInfo != null)
			{
				updateInfo.incrementNumReplyRecvd();
				ContextServiceLogger.getLogger().fine("processValueUpdateMsgToValuenodeReply numReplyRecvd "+updateInfo.getNumReplyRecvd() 
						+" NumReply "+valUpdateMsgToValnodeRep.getNumReply() );
				
				if(updateInfo.getNumReplyRecvd() == valUpdateMsgToValnodeRep.getNumReply())
				{
					sendUpdateReplyBackToUser( updateInfo.getValueUpdateFromGNS().getSourceIP(), 
							updateInfo.getValueUpdateFromGNS().getSourcePort(), updateInfo.getValueUpdateFromGNS().getVersionNum() );
					
					synchronized(this.pendingUpdateLock)
					{
						pendingUpdateRequests.remove(requestId);
					}
				}
			}
		}*/
		
	}
	
	
	private LinkedList<GroupGUIDRecord> getGroupsAffectedUsingDatabase
	(AttributeMetaObjectRecord<Integer, Double> metaObjRec, JSONObject allAttr, 
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
	private GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] 
			assignValueRanges(Integer initiator, AttributeMetadataInfoRecord<Integer, Double> attrMetaRec)
	{
		int numValueNodes = 1;
		//@SuppressWarnings("unchecked")
		//GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>>[] mesgArray 
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
			
			Integer currNodeID = this.getMyID();
			
			// add this to database, not to memory
			AttributeMetaObjectRecord<Integer, Double> attrMetaObjRec = new
			AttributeMetaObjectRecord<Integer, Double>(currMinRange, currMaxRange,
						currNodeID, new JSONArray());
				
			this.getContextServiceDB().putAttributeMetaObjectRecord(attrMetaObjRec, attrMetaRec.getAttrName());
			
			
			MetadataMsgToValuenode<Integer> metaMsgToValnode = new MetadataMsgToValuenode<Integer>
							( initiator, attrMetaRec.getAttrName(), currMinRange, currMaxRange);
			
//			GenericMessagingTask<Integer, MetadataMsgToValuenode<Integer>> mtask = new GenericMessagingTask<Integer, 
//					MetadataMsgToValuenode<Integer>>((Integer) currNodeID, metaMsgToValnode);
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
			QueryMsgToValuenodeReply<Integer> queryMsgToValnodeRep,
			QueryInfo<Integer> queryInfo) 
	{
		
	}

	@Override
	public void checkQueryCompletion(QueryInfo<Integer> qinfo) 
	{	
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