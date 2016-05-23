package edu.umass.cs.contextservice.database.privacy;

import java.util.HashMap;

import org.json.JSONObject;

import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;

/**
 * This interface defines mysql DB table for privacy information
 * storage and also defines function doing select queries and updates.
 * @author adipc
 */
public interface PrivacyInformationStorageInterface
{
	/**
	 * Defines the privacy table creation. 
	 * Returns the table creation command as string.
	 * @return
	 */
	public void createTables();
	
	/**
	 * Returns the partial join query, to be completed with 
	 * the processSearchQueryInSubspaceRegion query before execution.
	 * @param query
	 * @return
	 */
	public String getMySQLQueryForFetchingRealIDMappingForQuery(String query, int subspaceId);
	
	public void bulkInsertPrivacyInformationBlocking( String ID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep , int subsapceId);
	
	public void deleteAnonymizedIDFromPrivacyInfoStorageBlocking(String nodeGUID, 
			int deleteSubspaceId);
}