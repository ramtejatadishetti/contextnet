package edu.umass.cs.contextservice.schemes;

import java.util.HashMap;

import org.json.JSONException;

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
	
	public void processTriggerForValueUpdateToSubspaceRegion
	(ValueUpdateToSubspaceRegionMessage<NodeIDType> 
	valueUpdateToSubspaceRegionMessage, HashMap<String, GroupGUIDInfoClass> removedGroups, 
	HashMap<String, GroupGUIDInfoClass> addedGroups ) throws InterruptedException;
	
	public void sendOutAggregatedRefreshTrigger
	( HashMap<String, GroupGUIDInfoClass> removedGroups, 
			HashMap<String, GroupGUIDInfoClass> addedGroups, String updateGUID, 
			long versionNum,
			long updateStartTime) throws JSONException;
}