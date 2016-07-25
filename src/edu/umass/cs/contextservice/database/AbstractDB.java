package edu.umass.cs.contextservice.database;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;

public abstract class AbstractDB<NodeIDType>
{
	public abstract HashMap<Integer, OverlappingInfoClass> 
			getOverlappingRegionsInSubspace(int subspaceId, int replicaNum, 
				Vector<ProcessingQueryComponent> matchingQueryComponents);
	
	public abstract HashMap<Integer, OverlappingInfoClass> 
		getOverlappingPartitionsInTriggers( int subspaceId, int replicaNum, 
			String attrName, ProcessingQueryComponent matchingQueryComponent );
	
	public abstract int processSearchQueryInSubspaceRegion(int subspaceId, String query, 
			JSONArray resultArray);
	
	public abstract void insertIntoSubspacePartitionInfo(int subspaceId, int replicaNum,
			List<Integer> subspaceVector, NodeIDType respNodeId);
	
	public abstract void bulkInsertIntoSubspacePartitionInfo( int subspaceId, int replicaNum,
			List<List<Integer>> subspaceVectorList, List<NodeIDType> respNodeIdList );
	
	public abstract void insertIntoTriggerPartitionInfo(int subspaceId, int replicaNum, 
			String attrName, int partitionNum, NodeIDType respNodeId);
	
	public abstract JSONObject getGUIDStoredInPrimarySubspace( String guid );
	
	public abstract void insertIntoSubspaceTriggerDataInfo( int subspaceId, int replicaNum, 
			String attrName, String userQuery, String groupGUID, String userIP, 
			int userPort, long expiryTimeFromNow );
	
	public abstract void getTriggerDataInfo(int subspaceId, int replicaNum, String attrName, 
			JSONObject oldValJSON, JSONObject newUpdateVal, HashMap<String, JSONObject> oldValGroupGUIDMap, 
				HashMap<String, JSONObject> newValGroupGUIDMap, int oldOrNewOrBoth, JSONObject newUnsetAttrs) throws InterruptedException;
	
	
	public abstract int deleteExpiredSearchQueries( int subspaceId, int replicaNum, String attrName );
	
	public abstract void storeGUIDInPrimarySubspace( String nodeGUID, 
    		JSONObject jsonToWrite, int updateOrInsert ) throws JSONException;
	
	
	public abstract void storeGUIDInSecondarySubspace( String tableName, String nodeGUID, 
    		JSONObject jsonToWrite, int updateOrInsert 
    		, int subspaceId ) throws JSONException;
	
	public abstract void deleteGUIDFromSubspaceRegion(String tableName, String nodeGUID, 
			int subspaceId);
	
	public abstract boolean getSearchQueryRecordFromPrimaryTriggerSubspace( String groupGUID, 
			String userIP, int userPort ) throws UnknownHostException;
}