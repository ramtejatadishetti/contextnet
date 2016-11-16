package edu.umass.cs.contextservice.schemes.components;

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
public interface TriggerProcessingInterface
{
	public boolean processTriggerOnQueryMsgFromUser(QueryInfo currReq );
	
	public void processQuerySubspaceRegionMessageForTrigger
		( QueryMesgToSubspaceRegion queryMesgToSubspaceRegion );
	
	public void processTriggerForValueUpdateToSubspaceRegion
		( ValueUpdateToSubspaceRegionMessage 
		valueUpdateToSubspaceRegionMessage, HashMap<String, GroupGUIDInfoClass> removedGroups, 
		HashMap<String, GroupGUIDInfoClass> addedGroups ) throws InterruptedException;
	
	public void sendOutAggregatedRefreshTrigger
		( HashMap<String, GroupGUIDInfoClass> removedGroups, 
			HashMap<String, GroupGUIDInfoClass> addedGroups, String updateGUID, 
			long versionNum,
			long updateStartTime) throws JSONException;
}