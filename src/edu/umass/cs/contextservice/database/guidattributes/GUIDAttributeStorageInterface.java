package edu.umass.cs.contextservice.database.guidattributes;

import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.schemes.helperclasses.RegionInfoClass;

/**
 * This interface defines the attributes and guid storage 
 * tables, methods to search and update those tables.
 * This interface defines the tables for context service
 * that supports updates and range queries.
 * @author adipc
 */
public interface GUIDAttributeStorageInterface<NodeIDType> 
{
	public void createTables();
	
	public int processSearchQueryInSubspaceRegion
							(int subspaceId, HashMap<String, ProcessingQueryComponent> queryComponents, 
									JSONArray resultArray);
	
	public JSONObject getGUIDStoredInPrimarySubspace( String guid );
	
	public void insertIntoSubspacePartitionInfo(int subspaceId, int replicaNum,
			List<Integer> subspaceVector, NodeIDType respNodeId);
	
	public void bulkInsertIntoSubspacePartitionInfo( int subspaceId, int replicaNum,
			List<List<Integer>> subspaceVectorList, List<NodeIDType> respNodeIdList );
	
	public void storeGUIDInPrimarySubspace(String nodeGUID, JSONObject jsonToWrite, 
    		int updateOrInsert ) throws JSONException;
	
	public void storeGUIDInSecondarySubspace( String tableName, String nodeGUID, 
    		JSONObject updatedAttrValMap, int updateOrInsert )
    					throws JSONException;
	
	public void deleteGUIDFromSubspaceRegion(String tableName, String nodeGUID);
	
	public HashMap<Integer, RegionInfoClass> 
		getOverlappingRegionsInSubspace(int subspaceId, int replicaNum, 
			HashMap<String, ProcessingQueryComponent> matchingQueryComponents);
	
	public String getMySQLQueryForProcessSearchQueryInSubspaceRegion
								(int subspaceId, HashMap<String, ProcessingQueryComponent> queryComponents);
}