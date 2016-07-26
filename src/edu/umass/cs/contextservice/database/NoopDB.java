package edu.umass.cs.contextservice.database;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.database.triggers.GroupGUIDInfoClass;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;

public class NoopDB<NodeIDType> extends AbstractDB<NodeIDType>
{
	@Override
	public HashMap<Integer, OverlappingInfoClass> getOverlappingRegionsInSubspace
		(int subspaceId, int replicaNum, 
				Vector<ProcessingQueryComponent> matchingQueryComponents) 
	{
		return null;
	}

//	@Override
//	public HashMap<Integer, OverlappingInfoClass> getOverlappingPartitionsInTriggers
//		(int subspaceId, int replicaNum, String attrName, 
//				ProcessingQueryComponent matchingQueryComponent) 
//	{
//		return null;
//	}
	
	@Override
	public int processSearchQueryInSubspaceRegion
		(int subspaceId, String query, JSONArray resultArray) 
	{
		return 0;
	}

	@Override
	public void insertIntoSubspacePartitionInfo(int subspaceId, int replicaNum, 
			List<Integer> subspaceVector, NodeIDType respNodeId) 
	{	
	}

	@Override
	public void bulkInsertIntoSubspacePartitionInfo(int subspaceId, int replicaNum,
			List<List<Integer>> subspaceVectorList, List<NodeIDType> respNodeIdList) 
	{	
	}

//	@Override
//	public void insertIntoTriggerPartitionInfo(int subspaceId, int replicaNum, 
//			String attrName, int partitionNum,
//			NodeIDType respNodeId) 
//	{	
//	}

	@Override
	public JSONObject getGUIDStoredInPrimarySubspace(String guid) 
	{
		return null;
	}

	@Override
	public void insertIntoSubspaceTriggerDataInfo(int subspaceId, int replicaNum, String attrName, String userQuery,
			String groupGUID, String userIP, int userPort, long expiryTimeFromNow) 
	{
	}

	@Override
	public int deleteExpiredSearchQueries(int subspaceId) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void storeGUIDInPrimarySubspace(String nodeGUID, JSONObject jsonToWrite, int updateOrInsert)
			throws JSONException 
	{	
	}

	@Override
	public void storeGUIDInSecondarySubspace(String tableName, String nodeGUID, JSONObject jsonToWrite,
			int updateOrInsert, int subspaceId) throws JSONException 
	{	
	}

	@Override
	public void deleteGUIDFromSubspaceRegion(String tableName, String nodeGUID, int subspaceId) 
	{	
	}

	@Override
	public boolean checkAndInsertSearchQueryRecordFromPrimaryTriggerSubspace( String groupGUID, 
			String userIP, int userPort ) throws UnknownHostException 
	{
		return false;
	}

	@Override
	public void getTriggerDataInfo(int subspaceId, JSONObject oldValJSON, JSONObject newJSONToWrite,
			HashMap<String, GroupGUIDInfoClass> oldValGroupGUIDMap,
			HashMap<String, GroupGUIDInfoClass> newValGroupGUIDMap, int requestType, JSONObject newUnsetAttrs,
			boolean firstTimeInsert) throws InterruptedException {
		// TODO Auto-generated method stub
		
	}
}