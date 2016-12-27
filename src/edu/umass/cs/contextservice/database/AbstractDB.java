package edu.umass.cs.contextservice.database;

import java.net.UnknownHostException;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.database.triggers.GroupGUIDInfoClass;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;

public abstract class AbstractDB
{
//	public abstract HashMap<Integer, RegionInfoClass> 
//			getOverlappingRegionsInSubspace(int subspaceId, int replicaNum, 
//				HashMap<String, ProcessingQueryComponent> matchingQueryComponents);
	
	public abstract int processSearchQueryUsingAttrIndex(ValueSpaceInfo queryValueSpace, 
			JSONArray resultArray);
	
//	public abstract void insertIntoSubspacePartitionInfo(int subspaceId, int replicaNum,
//			List<Integer> subspaceVector, Integer respNodeId);
	
//	public abstract void bulkInsertIntoSubspacePartitionInfo( int subspaceId, int replicaNum,
//			List<List<Integer>> subspaceVectorList, List<Integer> respNodeIdList );
	
	public abstract JSONObject getGUIDStoredUsingHashIndex( String guid );
	
	public abstract void insertIntoTriggerDataStorage( String userQuery, 
			String groupGUID, String userIP, 
			int userPort, long expiryTimeFromNow );
	
	public abstract void getTriggerDataInfo( JSONObject oldValJSON, JSONObject newJSONToWrite, 
			HashMap<String, GroupGUIDInfoClass> oldValGroupGUIDMap, 
			HashMap<String, GroupGUIDInfoClass> newValGroupGUIDMap, 
			int requestType, JSONObject newUnsetAttrs, boolean firstTimeInsert) 
					throws InterruptedException;
	
	public abstract int deleteExpiredSearchQueries();
	
	public abstract void storeGUIDUsingHashIndex( String nodeGUID, 
    		JSONObject jsonToWrite, int updateOrInsert ) throws JSONException;
	
	public abstract void storeGUIDUsingAttrIndex( String tableName, String nodeGUID, 
    		JSONObject jsonToWrite, int updateOrInsert) throws JSONException;
	
	public abstract void deleteGUIDFromTable(String tableName, String nodeGUID);
	
	public abstract boolean checkAndInsertSearchQueryRecordFromPrimaryTriggerSubspace
			( String groupGUID, String userIP, int userPort ) throws UnknownHostException;
}