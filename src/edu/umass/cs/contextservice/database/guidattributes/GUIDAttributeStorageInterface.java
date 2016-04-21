package edu.umass.cs.contextservice.database.guidattributes;

import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;

/**
 * This interface defines the attributes and guid storage 
 * tables, methods to search and update those tables.
 * This interface defines the tables for context service
 * that supports updates and range queries.
 * @author adipc
 *
 */
public interface GUIDAttributeStorageInterface<NodeIDType> 
{
	public void createTables();
	
	public int processSearchQueryInSubspaceRegion(int subspaceId, String query, JSONArray resultArray);
	
	public JSONObject getGUIDStoredInPrimarySubspace( String guid );
	
	public void insertIntoSubspacePartitionInfo(int subspaceId, int replicaNum,
			List<Integer> subspaceVector, NodeIDType respNodeId);
	
	public void bulkInsertIntoSubspacePartitionInfo( int subspaceId, int replicaNum,
			List<List<Integer>> subspaceVectorList, List<NodeIDType> respNodeIdList );	
	
	public void storeGUIDInSubspace(String tableName, String nodeGUID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep, 
    		int updateOrInsert, JSONObject oldValJSON ) throws JSONException;
	
	public void deleteGUIDFromSubspaceRegion(String tableName, String nodeGUID);
	
	public HashMap<Integer, OverlappingInfoClass> 
		getOverlappingRegionsInSubspace(int subspaceId, int replicaNum, 
			Vector<ProcessingQueryComponent> matchingQueryComponents);
	
	public String getMySQLQueryForProcessSearchQueryInSubspaceRegion
								(int subspaceId, String query);
}