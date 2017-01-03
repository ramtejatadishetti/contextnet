package edu.umass.cs.contextservice.database.guidattributes;

import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;


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
						( HashMap<String, AttributeValueRange> queryAttrValMap, 
								JSONArray resultArray);
	
	public JSONObject getGUIDStoredUsingHashIndex( String guid );
	
	public void storeGUIDUsingHashIndex(String nodeGUID, JSONObject jsonToWrite, 
    		int updateOrInsert ) throws JSONException;
	
	public void storeGUIDUsingAttrIndex( String tableName, String nodeGUID, 
    		JSONObject updatedAttrValMap, int updateOrInsert )
    					throws JSONException;
	
	public void deleteGUIDFromTable(String tableName, String nodeGUID);
}