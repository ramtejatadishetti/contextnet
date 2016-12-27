package edu.umass.cs.contextservice.database.guidattributes;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;


/**
 * This interface defines the attributes and guid storage 
 * tables, methods to search and update those tables.
 * This interface defines the tables for context service
 * that supports updates and range queries.
 * @author adipc
 */
public interface GUIDStorageInterface 
{
	public void createDataStorageTables();
	
	public int processSearchQueryUsingAttrIndex
						( ValueSpaceInfo queryValueSpace, JSONArray resultArray);
	
	public JSONObject getGUIDStoredUsingHashIndex( String guid );
	
//	public void insertIntoSubspacePartitionInfo(int subspaceId, int replicaNum,
//			List<Integer> subspaceVector, Integer respNodeId);
	
//	public void bulkInsertIntoSubspacePartitionInfo( int subspaceId, int replicaNum,
//			List<List<Integer>> subspaceVectorList, List<Integer> respNodeIdList );
	
	public void storeGUIDUsingHashIndex(String nodeGUID, JSONObject jsonToWrite, 
    		int updateOrInsert ) throws JSONException;
	
	public void storeGUIDUsingAttrIndex( String tableName, String nodeGUID, 
    		JSONObject updatedAttrValMap, int updateOrInsert )
    					throws JSONException;
	
	public void deleteGUIDFromTable(String tableName, String nodeGUID);
	
//	public HashMap<Integer, RegionInfoClass> 
//		getOverlappingRegionsInSubspace(int subspaceId, int replicaNum, 
//			HashMap<String, ProcessingQueryComponent> matchingQueryComponents);
	
	//FIXME: not clear where this is used.
//	public String getMySQLQueryForProcessSearchQueryInSubspaceRegion
//								(int subspaceId, HashMap<String, ProcessingQueryComponent> queryComponents);
}