package edu.umass.cs.contextservice.database.privacy;

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
	public String getMySQLQueryForFetchingRealIDMappingForQuery(String query);
}