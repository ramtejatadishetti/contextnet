package edu.umass.cs.contextservice.schemes;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.database.triggers.GroupGUIDInfoClass;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.RefreshTrigger;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.nio.JSONMessenger;

/**
 * Implements trigger processing interface.
 * Implements hyperspace trigger processing interface.
 * @author adipc
 *
 */
public class TriggerProcessing<NodeIDType> implements 
								TriggerProcessingInterface<NodeIDType>
{
//	private final HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> 
//													subspaceInfoMap;

	private final HyperspaceMySQLDB<NodeIDType> hyperspaceDB;
	
	//private final Random replicaChoosingRand;
	
	private final NodeIDType myID;
	
	private final JSONMessenger<NodeIDType> messenger;
	
	public TriggerProcessing(NodeIDType myID, HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> 
						subspaceInfoMap , HyperspaceMySQLDB<NodeIDType> hyperspaceDB, 
						JSONMessenger<NodeIDType> messenger )
	{
		this.myID = myID;
		this.messenger = messenger;
//		replicaChoosingRand = new Random(myID.hashCode());
//
//		this.subspaceInfoMap = subspaceInfoMap;
		this.hyperspaceDB = hyperspaceDB;
		
		//generateTriggerPartitions();
		
		ContextServiceLogger.getLogger().fine("generateSubspacePartitions completed");
	

		new Thread( new DeleteExpiredSearchesThread<NodeIDType>(subspaceInfoMap, myID, hyperspaceDB) ).start();
	}
	
	public boolean processTriggerOnQueryMsgFromUser(QueryInfo<NodeIDType> currReq)
	{
		String groupGUID = currReq.getGroupGUID();
		String userIP = currReq.getUserIP();
		int userPort = currReq.getUserPort();
		boolean found = false;
		
		try
		{
			found = this.hyperspaceDB.checkAndInsertSearchQueryRecordFromPrimaryTriggerSubspace
					(groupGUID, userIP, userPort);
			
			ContextServiceLogger.getLogger().fine(" search query "+currReq.getQuery()+" found "+found
					+" groupGUID "+groupGUID+" userIP "+userIP+" userPort "+userPort);
			
			/*if( !found )
			{
				HashMap<Integer, Vector<ProcessingQueryComponent>> overlappingSubspaces =
		    			new HashMap<Integer, Vector<ProcessingQueryComponent>>();
				
				getAllUniqueOverlappingSubspaces( currReq.getProcessingQC(), overlappingSubspaces );
				
				
		    	Iterator<Integer> overlapSubspaceIter = overlappingSubspaces.keySet().iterator();
		    	
		    	
		    	while( overlapSubspaceIter.hasNext() )
		    	{
		    		int subspaceId = overlapSubspaceIter.next();
		    		Vector<SubspaceInfo<NodeIDType>> replicasVect 
		    										= subspaceInfoMap.get(subspaceId);
		    		
		    		// trigger info on a query just goes to any one random replica of a subspace
		    		// it doesn't need to be stored on all replicas of a subspace
		    		SubspaceInfo<NodeIDType> currSubInfo 
		    				= replicasVect.get(this.replicaChoosingRand.nextInt(replicasVect.size()));
		    		int replicaNum = currSubInfo.getReplicaNum();
		    		Vector<ProcessingQueryComponent> matchingComp = overlappingSubspaces.get(subspaceId);
		    		
		    		for(int i=0; i<matchingComp.size(); i++)
		    		{
		    			ProcessingQueryComponent matchingQComp = matchingComp.get(i);
		    			
		    			String currMatchingAttr = matchingQComp.getAttributeName();
		    			
		    			
		    			ProcessingQueryComponent qcomponent = new ProcessingQueryComponent( currMatchingAttr, matchingQComp.getLowerBound(), 
								matchingQComp.getUpperBound() );
		    			
						HashMap<Integer, OverlappingInfoClass> overlappingRegion = 
								this.hyperspaceDB.getOverlappingPartitionsInTriggers
								(subspaceId, replicaNum, currMatchingAttr, qcomponent);
						
						Iterator<Integer> overlapIter = overlappingRegion.keySet().iterator();
						
						while( overlapIter.hasNext() )
					    {
					    	Integer respNodeId = overlapIter.next();
					    	
					    	QueryTriggerMessage<NodeIDType> queryTriggerMessage = 
									new QueryTriggerMessage<NodeIDType>
					    				( myID, currReq.getRequestId(), currReq.getQuery(), 
					    						currReq.getGroupGUID(), subspaceId, replicaNum, 
					    						currMatchingAttr, currReq.getUserIP(), currReq.getUserPort());
					    	
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
							ContextServiceLogger.getLogger().info("Sending QueryTriggerMessage mesg from " 
									+ myID +" to node "+respNodeId);
					    }
		    		}
		    	}
			}*/
		}
		catch( Exception ex )
		{
			ex.printStackTrace();
		}
		return found;
	}
	
	public void processQuerySubspaceRegionMessageForTrigger
				( QueryMesgToSubspaceRegion<NodeIDType> queryMesgToSubspaceRegion )
	{
		String query 		= queryMesgToSubspaceRegion.getQuery();
		String groupGUID 	= queryMesgToSubspaceRegion.getGroupGUID();
		int subspaceId 		= queryMesgToSubspaceRegion.getSubspaceNum();
		String userIP       = queryMesgToSubspaceRegion.getUserIP();
		int userPort        = queryMesgToSubspaceRegion.getUserPort();
		long expiryTime		= queryMesgToSubspaceRegion.getExpiryTime();
		//ContextServiceLogger.getLogger().fine("QueryTriggerMessag recvd "+ queryTriggerMessage);
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			long expiryTimeFromNow = System.currentTimeMillis() + expiryTime;
			this.hyperspaceDB.insertIntoSubspaceTriggerDataInfo( subspaceId, 
					query, groupGUID, userIP, userPort, expiryTimeFromNow);
		}
	}
	
//	public void processQueryTriggerMessage(QueryTriggerMessage<NodeIDType> queryTriggerMessage)
//	{
//		String query 		= queryTriggerMessage.getQuery();
//		String groupGUID 	= queryTriggerMessage.getGroupGUID();
//		int subspaceId 		= queryTriggerMessage.getSubspaceNum();
//		int replicaNum		= queryTriggerMessage.getReplicaNum();
//		String attrName 	= queryTriggerMessage.getAttrName();
//		String userIP       = queryTriggerMessage.getUserIP();
//		int userPort        = queryTriggerMessage.getUserPort();
//		
//		ContextServiceLogger.getLogger().fine("QueryTriggerMessag recvd "+ queryTriggerMessage);
//		
//		if( ContextServiceConfig.TRIGGER_ENABLED )
//		{
//			long expiryTime = System.currentTimeMillis() + ContextServiceConfig.modelSearchRes;
//			this.hyperspaceDB.insertIntoSubspaceTriggerDataInfo( subspaceId, replicaNum, 
//					attrName, query, groupGUID, userIP, userPort, expiryTime);
//		}
//	}
	
	public void processTriggerForValueUpdateToSubspaceRegion
		(ValueUpdateToSubspaceRegionMessage<NodeIDType> 
		valueUpdateToSubspaceRegionMessage, HashMap<String, GroupGUIDInfoClass> removedGroups, 
		HashMap<String, GroupGUIDInfoClass> addedGroups ) throws InterruptedException
	{
		int subspaceId  = valueUpdateToSubspaceRegionMessage.getSubspaceNum();
		JSONObject oldValJSON = valueUpdateToSubspaceRegionMessage.getOldValJSON();
		JSONObject newJSONToWrite = valueUpdateToSubspaceRegionMessage.getJSONToWrite();
		int requestType = valueUpdateToSubspaceRegionMessage.getOperType();
		JSONObject newUnsetAttr = valueUpdateToSubspaceRegionMessage.getNewUnsetAttrs();
		boolean firstTimeInsert = valueUpdateToSubspaceRegionMessage.getFirstTimeInsert();
		
		
		this.hyperspaceDB.getTriggerDataInfo(subspaceId, 
				oldValJSON, newJSONToWrite, 
				removedGroups, 
				addedGroups, 
				requestType, newUnsetAttr, firstTimeInsert); 
		
		
		
//		this.hyperspaceDB.getTriggerDataInfo(subspaceId, oldValJSON, 
//				newUpdateVal, oldValGroupGUIDMap, newValGroupGUIDMap, requestType, newUnsetAttr);
		
		/*ContextServiceLogger.getLogger().fine("processUpdateTriggerMessage oldValGroupGUIDMap size "
				+oldValGroupGUIDMap.size()+" newValGroupGUIDMap size "+newValGroupGUIDMap.size() );
		
		JSONArray toBeRemoved = new JSONArray();
		JSONArray toBeAdded = new JSONArray();
		
		// if both then get the real trigger group guids
		// otherwise it can only be computed when the sender 
		// recvs replies for both old and new values.
		if( requestType == UpdateTriggerMessage.BOTH )
		{
			Iterator<String> oldValGrpGUIDIter = oldValGroupGUIDMap.keySet().iterator();
			while( oldValGrpGUIDIter.hasNext() )
			{
				String currGrpGUID = oldValGrpGUIDIter.next();
				// if grp guid not satisfied with new group then a 
				// removed notificated to be sent
				if( !newValGroupGUIDMap.containsKey(currGrpGUID) )
				{
					toBeRemoved.put(oldValGroupGUIDMap.get(currGrpGUID));
				}
			}
			
			Iterator<String> newValGrpGUIDIter = newValGroupGUIDMap.keySet().iterator();
			while( newValGrpGUIDIter.hasNext() )
			{
				String currGrpGUID = newValGrpGUIDIter.next();
				// if grp guid not satisfied with the old group then a 
				// addition notificated to be sent
				if( !oldValGroupGUIDMap.containsKey(currGrpGUID) )
				{
					toBeAdded.put(newValGroupGUIDMap.get(currGrpGUID));
				}
			}
		}
		else if( requestType == UpdateTriggerMessage.OLD_VALUE )
		{
			Iterator<String> oldValGrpGUIDIter = oldValGroupGUIDMap.keySet().iterator();
			while( oldValGrpGUIDIter.hasNext() )
			{
				String currGrpGUID = oldValGrpGUIDIter.next();
				toBeRemoved.put(oldValGroupGUIDMap.get(currGrpGUID));
			}
		}
		else if( requestType == UpdateTriggerMessage.NEW_VALUE )
		{
			Iterator<String> newValGrpGUIDIter = newValGroupGUIDMap.keySet().iterator();
			while( newValGrpGUIDIter.hasNext() )
			{
				String currGrpGUID = newValGrpGUIDIter.next();
				toBeAdded.put(newValGroupGUIDMap.get(currGrpGUID));
			}
		}
		
		ContextServiceLogger.getLogger().fine("processUpdateTriggerMessage "
				+ " toBeRemoved size "+toBeRemoved.length()+" toBeAdded size "+toBeAdded.length());
		
		UpdateTriggerReply<NodeIDType> updTriggerRep = 
				new UpdateTriggerReply<NodeIDType>( myID, requestID, subspaceId, replicaNum, 
						toBeRemoved, toBeAdded, updateTriggerMessage.getNumReplies(), requestType, attrName );
		
		try
		{
			this.messenger.sendToID
			( updateTriggerMessage.getSender(), updTriggerRep.toJSONObject() );
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}*/
	}
	
	/*public void processUpdateTriggerMessage(UpdateTriggerMessage<NodeIDType> updateTriggerMessage) throws InterruptedException
	{
		long requestID  = updateTriggerMessage.getRequestId();
		int subspaceId  = updateTriggerMessage.getSubspaceNum();
		int replicaNum  = updateTriggerMessage.getReplicaNum();
		JSONObject oldValJSON = updateTriggerMessage.getOldUpdateValPair();
		JSONObject newUpdateVal = updateTriggerMessage.getNewUpdateValPair();
		int requestType = updateTriggerMessage.getRequestType();
		String attrName    = updateTriggerMessage.getAttrName();
		JSONObject newUnsetAttr = updateTriggerMessage.getUnsetAttrs();
		
		
		HashMap<String, JSONObject> oldValGroupGUIDMap = new HashMap<String, JSONObject>();
		HashMap<String, JSONObject> newValGroupGUIDMap = new HashMap<String, JSONObject>();
		
		this.hyperspaceDB.getTriggerDataInfo(subspaceId, replicaNum, attrName, oldValJSON, 
				newUpdateVal, oldValGroupGUIDMap, newValGroupGUIDMap, requestType, newUnsetAttr);
		
		ContextServiceLogger.getLogger().fine("processUpdateTriggerMessage oldValGroupGUIDMap size "
				+oldValGroupGUIDMap.size()+" newValGroupGUIDMap size "+newValGroupGUIDMap.size() );
		
		JSONArray toBeRemoved = new JSONArray();
		JSONArray toBeAdded = new JSONArray();
		
		// if both then get the real trigger group guids
		// otherwise it can only be computed when the sender 
		// recvs replies for both old and new values.
		if( requestType == UpdateTriggerMessage.BOTH )
		{
			Iterator<String> oldValGrpGUIDIter = oldValGroupGUIDMap.keySet().iterator();
			while( oldValGrpGUIDIter.hasNext() )
			{
				String currGrpGUID = oldValGrpGUIDIter.next();
				// if grp guid not satisfied with new group then a 
				// removed notificated to be sent
				if( !newValGroupGUIDMap.containsKey(currGrpGUID) )
				{
					toBeRemoved.put(oldValGroupGUIDMap.get(currGrpGUID));
				}
			}
			
			Iterator<String> newValGrpGUIDIter = newValGroupGUIDMap.keySet().iterator();
			while( newValGrpGUIDIter.hasNext() )
			{
				String currGrpGUID = newValGrpGUIDIter.next();
				// if grp guid not satisfied with the old group then a 
				// addition notificated to be sent
				if( !oldValGroupGUIDMap.containsKey(currGrpGUID) )
				{
					toBeAdded.put(newValGroupGUIDMap.get(currGrpGUID));
				}
			}
		}
		else if( requestType == UpdateTriggerMessage.OLD_VALUE )
		{
			Iterator<String> oldValGrpGUIDIter = oldValGroupGUIDMap.keySet().iterator();
			while( oldValGrpGUIDIter.hasNext() )
			{
				String currGrpGUID = oldValGrpGUIDIter.next();
				toBeRemoved.put(oldValGroupGUIDMap.get(currGrpGUID));
			}
		}
		else if( requestType == UpdateTriggerMessage.NEW_VALUE )
		{
			Iterator<String> newValGrpGUIDIter = newValGroupGUIDMap.keySet().iterator();
			while( newValGrpGUIDIter.hasNext() )
			{
				String currGrpGUID = newValGrpGUIDIter.next();
				toBeAdded.put(newValGroupGUIDMap.get(currGrpGUID));
			}
		}
		
		ContextServiceLogger.getLogger().fine("processUpdateTriggerMessage "
				+ " toBeRemoved size "+toBeRemoved.length()+" toBeAdded size "+toBeAdded.length());
		
		UpdateTriggerReply<NodeIDType> updTriggerRep = 
				new UpdateTriggerReply<NodeIDType>( myID, requestID, subspaceId, replicaNum, 
						toBeRemoved, toBeAdded, updateTriggerMessage.getNumReplies(), requestType, attrName );
		
		try
		{
			this.messenger.sendToID
			( updateTriggerMessage.getSender(), updTriggerRep.toJSONObject() );
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		}
	}*/
	
	/*public void triggerProcessingOnUpdate( JSONObject attrValuePairs, 
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo, 
			int subspaceId, int replicaNum, JSONObject oldValueJSON, 
			long requestID, JSONObject  primarySubspaceJSON , boolean firstTimeInsert )
					throws JSONException
	{
		// only check the new values and notify groups where this
		// GUID is added.
		if( firstTimeInsert )
		{	
			processTriggerOnFirstInsert( attrValuePairs, 
					attrsSubspaceInfo, requestID, 
					subspaceId, replicaNum, primarySubspaceJSON );
		}
		else // check both old and the new value and send removal triggers
			 // to groups from which this GUID is removed. and based on the new
			 // value send addition triggers to groups to which this GUID is added.
		{
			processTriggerOnUpdate( attrValuePairs, oldValueJSON, 
					attrsSubspaceInfo, requestID, subspaceId, 
					replicaNum, primarySubspaceJSON );
		}
	}
	
	private void processTriggerOnFirstInsert( JSONObject attrValuePairs, 
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo, long requestID, 
			int subspaceId, int replicaNum, JSONObject  primarySubspaceJSON )
					throws JSONException
	{
		// update can be over multiple attributes
		Iterator<String> attrIter = attrValuePairs.keys();
		
		while( attrIter.hasNext() )
		{
			String currAttrName = attrIter.next();
			String currValue = attrValuePairs.getString(currAttrName);
			
			// current attribute is contained 
			// in the attribute subspace
			if( attrsSubspaceInfo.containsKey(currAttrName) )
			{
				//find old overlapping groups
				ProcessingQueryComponent newTriggerComponent 
						= new ProcessingQueryComponent( currAttrName, currValue, currValue );
				
				Integer newRespNodeId = -1;
				
				// find new overlapping groups
				
				HashMap<Integer, OverlappingInfoClass> newOverlappingRegion = 
				this.hyperspaceDB.getOverlappingPartitionsInTriggers
					(subspaceId, replicaNum, currAttrName, newTriggerComponent);
				
				
				if( newOverlappingRegion.size() != 1 )
				{
					assert(false);
				}
				else
				{
					newRespNodeId = newOverlappingRegion.keySet().iterator().next();
				}
				
				JSONObject unsetAttrJSON = HyperspaceHashing.getUnsetAttrJSON(primarySubspaceJSON);
				
				assert(unsetAttrJSON != null);
				
				
				UpdateTriggerMessage<NodeIDType>  newUpdateTriggerMessage 
					= new UpdateTriggerMessage<NodeIDType>( myID, requestID, subspaceId, 
							replicaNum, new JSONObject(), attrValuePairs, 
							UpdateTriggerMessage.NEW_VALUE, 1, currAttrName, unsetAttrJSON);
				
				try
				{
					this.messenger.sendToID( (NodeIDType) newRespNodeId, 
							newUpdateTriggerMessage.toJSONObject() );
				} catch (IOException e)
				{
					e.printStackTrace();
				} catch (JSONException e)
				{
					e.printStackTrace();
				}
				
				ContextServiceLogger.getLogger().fine("Sending UpdateTriggerMessage from "
						+myID+" to "+newRespNodeId);	
			}
		}
	}
	
	
	private void processTriggerOnUpdate( JSONObject attrValuePairs, JSONObject oldValueJSON, 
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo, long requestID, 
			int subspaceId, int replicaNum, JSONObject  primarySubspaceJSON ) throws JSONException
	{
		assert( oldValueJSON != null );
		// update can be over multiple attributes
		JSONObject newUnsetAttrsJSON 
				= HyperspaceHashing.getUnsetAttrJSON(primarySubspaceJSON);
		
		assert(newUnsetAttrsJSON != null);
		
		Iterator<String> attrIter = attrValuePairs.keys();
		while( attrIter.hasNext() )
		{
			String currAttrName = attrIter.next();
			String currValue = attrValuePairs.getString(currAttrName);
			
			assert(oldValueJSON.has(currAttrName));
			
			String oldValue  = oldValueJSON.getString(currAttrName);
			
			// current attribute is contained 
			// in the attribute subspace
			if( attrsSubspaceInfo.containsKey(currAttrName) )
			{
				//find old overlapping groups
				ProcessingQueryComponent oldTriggerComponent 
						= new ProcessingQueryComponent( currAttrName, oldValue, oldValue );
				
				ProcessingQueryComponent newTriggerComponent 
						= new ProcessingQueryComponent( currAttrName, currValue, currValue );
				
				Integer oldRespNodeId = -1, newRespNodeId = -1;
				
				HashMap<Integer, OverlappingInfoClass> oldOverlappingRegion = 
							this.hyperspaceDB.getOverlappingPartitionsInTriggers
							(subspaceId, replicaNum, currAttrName, oldTriggerComponent);
				
				if( oldOverlappingRegion.size() != 1 )
				{
					// it should fall in exactly one region/node
					assert(false);
				}
				else
				{
					oldRespNodeId = oldOverlappingRegion.keySet().iterator().next();
				}
				// find new overlapping groups
				
				HashMap<Integer, OverlappingInfoClass> newOverlappingRegion = 
				this.hyperspaceDB.getOverlappingPartitionsInTriggers
					(subspaceId, replicaNum, currAttrName, newTriggerComponent);
				
				
				if( newOverlappingRegion.size() != 1 )
				{
					assert(false);
				}
				else
				{
					newRespNodeId = newOverlappingRegion.keySet().iterator().next();
				}
				
				// old and new both lie on same node
				if( oldRespNodeId == newRespNodeId )
				{
					// 1 reply to expect as both old and new go to same ndoe
					UpdateTriggerMessage<NodeIDType>  updateTriggerMessage 
					 = new UpdateTriggerMessage<NodeIDType>( myID, requestID, subspaceId, 
							 replicaNum, oldValueJSON, attrValuePairs, 
							 UpdateTriggerMessage.BOTH, 1, currAttrName, newUnsetAttrsJSON);
					
					try
					{
						this.messenger.sendToID((NodeIDType) oldRespNodeId, 
								updateTriggerMessage.toJSONObject());
					} catch (IOException e)
					{
						e.printStackTrace();
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
					
					ContextServiceLogger.getLogger().fine("Sending UpdateTriggerMessage from "
					+myID+" to "+oldRespNodeId);
				}
				else
				{
					// 2 replies to expect as old and new go to different nodes
					UpdateTriggerMessage<NodeIDType>  oldUpdateTriggerMessage 
					 = new UpdateTriggerMessage<NodeIDType>( myID, requestID, subspaceId, 
							 replicaNum, oldValueJSON, attrValuePairs, 
							 UpdateTriggerMessage.OLD_VALUE, 2, currAttrName, newUnsetAttrsJSON);
					
					try
					{
						this.messenger.sendToID((NodeIDType) oldRespNodeId, 
								oldUpdateTriggerMessage.toJSONObject());
					} catch (IOException e)
					{
						e.printStackTrace();
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
					
					ContextServiceLogger.getLogger().fine("Sending UpdateTriggerMessage from "
							+myID+" to "+oldRespNodeId);
					
					UpdateTriggerMessage<NodeIDType>  newUpdateTriggerMessage 
					 = new UpdateTriggerMessage<NodeIDType>( myID, requestID, subspaceId, 
							 replicaNum, oldValueJSON, attrValuePairs, 
							 UpdateTriggerMessage.NEW_VALUE, 2, currAttrName, 
							 newUnsetAttrsJSON);
					
					try
					{
						this.messenger.sendToID( (NodeIDType) newRespNodeId, 
								newUpdateTriggerMessage.toJSONObject() );
					} catch (IOException e)
					{
						e.printStackTrace();
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
					ContextServiceLogger.getLogger().fine("Sending UpdateTriggerMessage from "
							+myID+" to "+newRespNodeId);
				}
			}
		}
	}*/
	
	public void sendOutAggregatedRefreshTrigger
				( HashMap<String, GroupGUIDInfoClass> removedGroups, 
				HashMap<String, GroupGUIDInfoClass> addedGroups, String updateGUID, 
				long versionNum,
				long updateStartTime)
							throws JSONException
	{
		HashMap<String, JSONArray> sameClientRemovedTrigger 
												= new HashMap<String, JSONArray>();
		HashMap<String, JSONArray> sameClientAddedTrigger 
												= new HashMap<String, JSONArray>();
		
		Iterator<String> removedIter = removedGroups.keySet().iterator();
		while( removedIter.hasNext() )
		{
			String groupGUID = removedIter.next();
			GroupGUIDInfoClass groupInfo = removedGroups.get(groupGUID);
			String ipPortKey = groupInfo.getUserIP()+":"+groupInfo.getUserPort();
			
			JSONArray removedGroupGUIDArray = sameClientRemovedTrigger.get(ipPortKey);
			
			if( removedGroupGUIDArray == null )
			{
				removedGroupGUIDArray = new JSONArray();
				removedGroupGUIDArray.put(groupGUID);
				sameClientRemovedTrigger.put(ipPortKey, removedGroupGUIDArray);
			}
			else
			{
				removedGroupGUIDArray.put(groupGUID);
			}
		}
		
		
		Iterator<String> addedIter = addedGroups.keySet().iterator();
		while( addedIter.hasNext() )
		{
			String groupGUID = addedIter.next();
			GroupGUIDInfoClass groupInfo = addedGroups.get(groupGUID);
			String ipPortKey = groupInfo.getUserIP()+":"+groupInfo.getUserPort();
			
			JSONArray addedGroupGUIDArray = sameClientAddedTrigger.get(ipPortKey);
			
			if( addedGroupGUIDArray == null )
			{
				addedGroupGUIDArray = new JSONArray();
				addedGroupGUIDArray.put(groupGUID);
				sameClientAddedTrigger.put(ipPortKey, addedGroupGUIDArray);
			}
			else
			{
				addedGroupGUIDArray.put(groupGUID);
			}
		}
		
		
//		while(attrIter.hasNext())
//		{
//			String currAttrName = attrIter.next();
//			JSONArray removedGrpForAttr = updInfo.getRemovedGroupsForAttr(currAttrName);
//			JSONArray addedGrpForAttr = updInfo.getToBeAddedGroupsForAttr(currAttrName);
//			
//			// just batching trigger for the same client with same ipAddr:Port	
//			for(int i=0;i<removedGrpForAttr.length();i++)
//			{
//				JSONObject groupInfo = removedGrpForAttr.getJSONObject(i);
//				String userIP = groupInfo.getString(HyperspaceMySQLDB.userIP);
//				int userPort  = groupInfo.getInt(HyperspaceMySQLDB.userPort);
//				String ipPort = userIP+":"+userPort;
//				String groupGUID = groupInfo.getString(HyperspaceMySQLDB.groupGUID);
//				
//				if( sameClientRemovedTrigger.containsKey(ipPort) )
//				{
//					sameClientRemovedTrigger.get(ipPort).put(groupGUID);
//				}
//				else
//				{
//					JSONArray groupGUIDArr = new JSONArray();
//					groupGUIDArr.put(groupGUID);
//					sameClientRemovedTrigger.put( ipPort, groupGUIDArr );
//				}
//			}
//						
//			for(int i=0;i<addedGrpForAttr.length();i++)
//			{
//				JSONObject groupInfo = addedGrpForAttr.getJSONObject(i);
//				String userIP = groupInfo.getString(HyperspaceMySQLDB.userIP);
//				int userPort = groupInfo.getInt(HyperspaceMySQLDB.userPort);
//				String ipPort = userIP+":"+userPort;
//				String groupGUID = groupInfo.getString(HyperspaceMySQLDB.groupGUID);
//				
//				if( sameClientAddedTrigger.containsKey(ipPort) )
//				{
//					sameClientAddedTrigger.get(ipPort).put(groupGUID);
//				}
//				else
//				{
//					JSONArray groupGUIDArr = new JSONArray();
//					groupGUIDArr.put(groupGUID);
//					sameClientAddedTrigger.put( ipPort, groupGUIDArr );
//				}
//			}
//		}
		
		Iterator<String> sameClientIter = sameClientRemovedTrigger.keySet().iterator();
		
		while( sameClientIter.hasNext() )
		{
			String ipPort = sameClientIter.next();
			JSONArray toBeRemovedGroupGUIDs = sameClientRemovedTrigger.get(ipPort);
			JSONArray toBeAddedGroupGUIDs = sameClientAddedTrigger.remove(ipPort);
			
			
			RefreshTrigger<NodeIDType> refTrig 
			= new RefreshTrigger<NodeIDType>
			(myID, toBeRemovedGroupGUIDs, 
					(toBeAddedGroupGUIDs!=null)?toBeAddedGroupGUIDs:new JSONArray(),
					versionNum, updateGUID, updateStartTime);
			
			String[] parsed = ipPort.split(":");
			
			String userIP 	= parsed[0];
			int userPort  	= Integer.parseInt(parsed[1]);
			
			ContextServiceLogger.getLogger().fine("processUpdateTriggerReply removed grps "
					+" userIP "+userIP+" userPort "+userPort);
			
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
		}
		
		
		// sending for remaining
		sameClientIter = sameClientAddedTrigger.keySet().iterator();
		while( sameClientIter.hasNext() )
		{
			String ipPort = sameClientIter.next();
			
			RefreshTrigger<NodeIDType> refTrig 
			= new RefreshTrigger<NodeIDType>
			(myID, new JSONArray(), 
					sameClientAddedTrigger.get(ipPort),
					versionNum, updateGUID, updateStartTime);			

			
			String[] parsed = ipPort.split(":");
			
			String userIP 	= parsed[0];
			int userPort  	= Integer.parseInt(parsed[1]);
			
			ContextServiceLogger.getLogger().fine("processUpdateTriggerReply removed grps "
					+" userIP "+userIP+" userPort "+userPort);
			
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
		}
	}
	
	
	
	/*public void sendOutAggregatedRefreshTrigger(UpdateInfo<NodeIDType> updInfo) throws JSONException
	{
		JSONObject updatedAttrValuePairs = updInfo.getValueUpdateFromGNS().getAttrValuePairs();
		long updateStartTime = updInfo.getValueUpdateFromGNS().getUpdateStartTime();
		
		Iterator<String> attrIter = updatedAttrValuePairs.keys();
		HashMap<String, JSONArray> sameClientRemovedTrigger = new HashMap<String, JSONArray>();
		HashMap<String, JSONArray> sameClientAddedTrigger = new HashMap<String, JSONArray>();
		
		while(attrIter.hasNext())
		{
			String currAttrName = attrIter.next();
			JSONArray removedGrpForAttr = updInfo.getRemovedGroupsForAttr(currAttrName);
			JSONArray addedGrpForAttr = updInfo.getToBeAddedGroupsForAttr(currAttrName);
			
			// just batching trigger for the same client with same ipAddr:Port	
			for(int i=0;i<removedGrpForAttr.length();i++)
			{
				JSONObject groupInfo = removedGrpForAttr.getJSONObject(i);
				String userIP = groupInfo.getString(HyperspaceMySQLDB.userIP);
				int userPort  = groupInfo.getInt(HyperspaceMySQLDB.userPort);
				String ipPort = userIP+":"+userPort;
				String groupGUID = groupInfo.getString(HyperspaceMySQLDB.groupGUID);
				
				if( sameClientRemovedTrigger.containsKey(ipPort) )
				{
					sameClientRemovedTrigger.get(ipPort).put(groupGUID);
				}
				else
				{
					JSONArray groupGUIDArr = new JSONArray();
					groupGUIDArr.put(groupGUID);
					sameClientRemovedTrigger.put( ipPort, groupGUIDArr );
				}
			}
						
			for(int i=0;i<addedGrpForAttr.length();i++)
			{
				JSONObject groupInfo = addedGrpForAttr.getJSONObject(i);
				String userIP = groupInfo.getString(HyperspaceMySQLDB.userIP);
				int userPort = groupInfo.getInt(HyperspaceMySQLDB.userPort);
				String ipPort = userIP+":"+userPort;
				String groupGUID = groupInfo.getString(HyperspaceMySQLDB.groupGUID);
				
				if( sameClientAddedTrigger.containsKey(ipPort) )
				{
					sameClientAddedTrigger.get(ipPort).put(groupGUID);
				}
				else
				{
					JSONArray groupGUIDArr = new JSONArray();
					groupGUIDArr.put(groupGUID);
					sameClientAddedTrigger.put( ipPort, groupGUIDArr );
				}
			}
		}
		
		Iterator<String> sameClientIter = sameClientRemovedTrigger.keySet().iterator();
		
		while( sameClientIter.hasNext() )
		{
			String ipPort = sameClientIter.next();
			
			RefreshTrigger<NodeIDType> refTrig = new RefreshTrigger<NodeIDType>
			(myID, sameClientRemovedTrigger.get(ipPort), updInfo.getValueUpdateFromGNS().getVersionNum(),
					updInfo.getValueUpdateFromGNS().getGUID(), RefreshTrigger.REMOVE, updateStartTime);
			
			String[] parsed = ipPort.split(":");
			
			String userIP 	= parsed[0];
			int userPort  	= Integer.parseInt(parsed[1]);
			
			ContextServiceLogger.getLogger().fine("processUpdateTriggerReply removed grps "
					+" userIP "+userIP+" userPort "+userPort);
			
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
		}
		
		sameClientIter = sameClientAddedTrigger.keySet().iterator();
		while( sameClientIter.hasNext() )
		{
			String ipPort = sameClientIter.next();
			
			RefreshTrigger<NodeIDType> refTrig = new RefreshTrigger<NodeIDType>
			(myID, sameClientAddedTrigger.get(ipPort), updInfo.getValueUpdateFromGNS().getVersionNum(),
					updInfo.getValueUpdateFromGNS().getGUID(), RefreshTrigger.ADD, updateStartTime);
			
			String[] parsed = ipPort.split(":");
			
			String userIP 	= parsed[0];
			int userPort  	= Integer.parseInt(parsed[1]);
			
			ContextServiceLogger.getLogger().fine("processUpdateTriggerReply removed grps "
					+" userIP "+userIP+" userPort "+userPort);
			
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
		}
	}*/
	
	/**
	 * returns subspacenum of all the subspaces a query overlaps with. 
	 * But returns only uniquer subspaces, not all the replicas of the overlapiing subspaces.
	 * @return
	 */
	/*private void getAllUniqueOverlappingSubspaces( HashMap<String, ProcessingQueryComponent> pqueryComponents, 
			HashMap<Integer, Vector<ProcessingQueryComponent>> overlappingSubspaces )
	{
		assert(pqueryComponents != null);
		assert(overlappingSubspaces != null);
		
		Iterator<Integer> keyIter   	= subspaceInfoMap.keySet().iterator();
		
		while( keyIter.hasNext() )
		{
			int subspaceId = keyIter.next();
			Vector<SubspaceInfo<NodeIDType>> replicaVect = subspaceInfoMap.get(subspaceId);
			SubspaceInfo<NodeIDType> currSubInfo 
			= replicaVect.get(replicaChoosingRand.nextInt(replicaVect.size()));
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
	}*/
	
	
	/**
	 * generates trigger single attribute partitions
	 */
//	private void generateTriggerPartitions()
//	{
//		ContextServiceLogger.getLogger().fine(" generateTriggerPartitions() entering " );
//		
//		Iterator<Integer> subspaceIter = subspaceInfoMap.keySet().iterator();
//		
//		while( subspaceIter.hasNext() )
//		{
//			int subspaceId = subspaceIter.next();
//			Vector<SubspaceInfo<NodeIDType>> replicaVect 
//								= subspaceInfoMap.get(subspaceId);
//			
//			for( int i=0; i<replicaVect.size(); i++ )
//			{
//				SubspaceInfo<NodeIDType> subspaceInfo = replicaVect.get(i);
//				int replicaNum = subspaceInfo.getReplicaNum();
//				HashMap<String, AttributePartitionInfo> attrsOfSubspace 
//										= subspaceInfo.getAttributesOfSubspace();
//				
//				Vector<NodeIDType> nodesOfSubspace = subspaceInfo.getNodesOfSubspace();
//				
//				Iterator<String> attrIter = attrsOfSubspace.keySet().iterator();
//				// Print the result
//				int nodeIdCounter = 0;
//				int sizeOfNumNodes = nodesOfSubspace.size();
//				
//				while( attrIter.hasNext() )
//				{
//					String attrName = attrIter.next();
//					AttributePartitionInfo currPartInfo = attrsOfSubspace.get(attrName);
//					
//					int numTriggerPartitions = currPartInfo.getTriggerNumPartitions();
//					ContextServiceLogger.getLogger().fine(" numTriggerPartitions "
//							+numTriggerPartitions );
//					
//					int j =0;
//					while(j < numTriggerPartitions)
//					{
//						NodeIDType respNodeId = nodesOfSubspace.get(nodeIdCounter%sizeOfNumNodes);
//						this.hyperspaceDB.insertIntoTriggerPartitionInfo
//						(subspaceId, replicaNum, attrName, j, respNodeId);
//						nodeIdCounter++;
//						j++;
//					}
//				}
//			}
//		}
//		ContextServiceLogger.getLogger().fine(" generateTriggerPartitions() completed " );
//	}
}