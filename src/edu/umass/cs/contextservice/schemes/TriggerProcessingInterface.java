package edu.umass.cs.contextservice.schemes;

import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.messages.QueryTriggerMessage;
import edu.umass.cs.contextservice.messages.UpdateTriggerMessage;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.updates.UpdateInfo;

/**
 * Defines the methods for trigger processing.
 * @author adipc
 *
 */
public interface TriggerProcessingInterface<NodeIDType> 
{
	public void processTriggerOnQueryMsgFromUser(QueryInfo<NodeIDType> currReq);
	
	public void processQueryTriggerMessage(QueryTriggerMessage<NodeIDType> queryTriggerMessage);
	
	public void processUpdateTriggerMessage(UpdateTriggerMessage<NodeIDType> updateTriggerMessage) throws InterruptedException;
	
	public void triggerProcessingOnUpdate( JSONObject attrValuePairs, 
			HashMap<String, AttributePartitionInfo> attrsSubspaceInfo, 
			int subspaceId, int replicaNum, JSONObject  oldValueJSON, 
			long requestID, JSONObject primarySubspaceJSON, boolean firstTimeInsert) throws JSONException;
	
	public void sendOutAggregatedRefreshTrigger(UpdateInfo<NodeIDType> updInfo) throws JSONException;
}