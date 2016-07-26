package edu.umass.cs.contextservice.schemes;

import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.database.triggers.GroupGUIDInfoClass;
import edu.umass.cs.contextservice.messages.QueryMesgToSubspaceRegion;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;

/**
 * 
 * Defines the methods for trigger processing.
 * @author adipc
 */
public interface TriggerProcessingInterface<NodeIDType> 
{
	public boolean processTriggerOnQueryMsgFromUser(QueryInfo<NodeIDType> currReq);
	
	public void processQuerySubspaceRegionMessageForTrigger
		( QueryMesgToSubspaceRegion<NodeIDType> queryMesgToSubspaceRegion );
	//public void processQueryTriggerMessage(QueryTriggerMessage<NodeIDType> queryTriggerMessage);
	
	public void processTriggerForValueUpdateToSubspaceRegion
	(ValueUpdateToSubspaceRegionMessage<NodeIDType> 
	valueUpdateToSubspaceRegionMessage, HashMap<String, GroupGUIDInfoClass> removedGroups, 
	HashMap<String, GroupGUIDInfoClass> addedGroups ) throws InterruptedException;
	
//	public void processUpdateTriggerMessage(UpdateTriggerMessage<NodeIDType> 
									// updateTriggerMessage) throws InterruptedException;
//	public void triggerProcessingOnUpdate( JSONObject attrValuePairs, 
//			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo, 
//			int subspaceId, int replicaNum, JSONObject  oldValueJSON, 
//			long requestID, JSONObject primarySubspaceJSON, boolean firstTimeInsert) 
//  throws JSONException;
//	public void sendOutAggregatedRefreshTrigger(UpdateInfo<NodeIDType> updInfo)
//																throws JSONException;
	
	public void sendOutAggregatedRefreshTrigger
	( HashMap<String, GroupGUIDInfoClass> removedGroups, 
			HashMap<String, GroupGUIDInfoClass> addedGroups, String updateGUID, 
			long versionNum,
			long updateStartTime) throws JSONException;
}