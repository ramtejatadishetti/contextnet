package edu.umass.cs.contextservice.database;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.database.triggers.GroupGUIDInfoClass;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.schemes.helperclasses.RegionInfoClass;


public abstract class AbstractDB<NodeIDType>
{
	public abstract HashMap<Integer, RegionInfoClass> 
			getOverlappingRegionsInSubspace(int subspaceId, int replicaNum, 
				HashMap<String, ProcessingQueryComponent> matchingQueryComponents);
	
	public abstract int processSearchQueryInSubspaceRegion(int subspaceId, 
			HashMap<String, ProcessingQueryComponent> queryComponents, 
			JSONArray resultArray);
	
	public abstract void insertIntoSubspacePartitionInfo(int subspaceId, int replicaNum,
			List<Integer> subspaceVector, NodeIDType respNodeId);
	
	public abstract void bulkInsertIntoSubspacePartitionInfo( int subspaceId, int replicaNum,
			List<List<Integer>> subspaceVectorList, List<NodeIDType> respNodeIdList );
	
	public abstract JSONObject getGUIDStoredInPrimarySubspace( String guid );
	
	public abstract void insertIntoSubspaceTriggerDataInfo( int subspaceId, 
			String userQuery, String groupGUID, String userIP, 
			int userPort, long expiryTimeFromNow );
	
	public abstract void getTriggerDataInfo(int subspaceId, 
			JSONObject oldValJSON, JSONObject newJSONToWrite, 
			HashMap<String, GroupGUIDInfoClass> oldValGroupGUIDMap, 
			HashMap<String, GroupGUIDInfoClass> newValGroupGUIDMap, 
			int requestType, JSONObject newUnsetAttrs, boolean firstTimeInsert) 
					throws InterruptedException;
	
	public abstract int deleteExpiredSearchQueries( int subspaceId );
	
	public abstract void storeGUIDInPrimarySubspace( String nodeGUID, 
    		JSONObject jsonToWrite, int updateOrInsert ) throws JSONException;
	
	public abstract void storeGUIDInSecondarySubspace( String tableName, String nodeGUID, 
    		JSONObject jsonToWrite, int updateOrInsert 
    		, int subspaceId ) throws JSONException;
	
	public abstract void deleteGUIDFromSubspaceRegion(String tableName, String nodeGUID, 
			int subspaceId);
	
	public abstract boolean checkAndInsertSearchQueryRecordFromPrimaryTriggerSubspace
			( String groupGUID, String userIP, int userPort ) throws UnknownHostException;
}