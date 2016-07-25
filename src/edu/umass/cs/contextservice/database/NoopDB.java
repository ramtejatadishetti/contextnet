package edu.umass.cs.contextservice.database;

import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.utils.DelayProfiler;

public class NoopDB<NodeIDType> extends AbstractDB<NodeIDType>
{
	@Override
	public HashMap<Integer, OverlappingInfoClass> getOverlappingRegionsInSubspace
		(int subspaceId, int replicaNum, 
				Vector<ProcessingQueryComponent> matchingQueryComponents) 
	{
		return null;
	}

	@Override
	public HashMap<Integer, OverlappingInfoClass> getOverlappingPartitionsInTriggers
		(int subspaceId, int replicaNum, String attrName, 
				ProcessingQueryComponent matchingQueryComponent) 
	{
		return null;
	}

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

	@Override
	public void insertIntoTriggerPartitionInfo(int subspaceId, int replicaNum, 
			String attrName, int partitionNum,
			NodeIDType respNodeId) 
	{
		
	}

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
	public void getTriggerDataInfo(int subspaceId, int replicaNum, String attrName, JSONObject oldValJSON,
			JSONObject newUpdateVal, HashMap<String, JSONObject> oldValGroupGUIDMap,
			HashMap<String, JSONObject> newValGroupGUIDMap, int oldOrNewOrBoth, JSONObject newUnsetAttrs) throws InterruptedException 
	{
		
	}

	@Override
	public int deleteExpiredSearchQueries(int subspaceId, int replicaNum, String attrName) {
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
	public boolean getSearchQueryRecordFromPrimaryTriggerSubspace(String groupGUID, String userIP, int userPort)
			throws UnknownHostException 
	{
		return false;
	}
}