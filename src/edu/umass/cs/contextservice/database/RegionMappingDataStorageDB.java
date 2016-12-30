package edu.umass.cs.contextservice.database;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.guidattributes.SQLGUIDStorage;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource;
import edu.umass.cs.contextservice.database.datasource.MySQLDataSource;
import edu.umass.cs.contextservice.database.datasource.SQLiteDataSource;
import edu.umass.cs.contextservice.database.guidattributes.GUIDStorageInterface;
import edu.umass.cs.contextservice.database.triggers.GroupGUIDInfoClass;
import edu.umass.cs.contextservice.database.triggers.TriggerInformationStorage;
import edu.umass.cs.contextservice.database.triggers.TriggerInformationStorageInterface;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;


public class RegionMappingDataStorageDB extends AbstractDataStorageDB
{
	public static final int UPDATE_REC 								= 1;
	public static final int INSERT_REC 								= 2;
	
	// maximum query length of 1000bytes
	public static final int MAX_QUERY_LENGTH						= 1000;
	
	//public static final String userQuery = "userQuery";
	public static final String groupGUID 							= "groupGUID";
	public static final String userIP 								= "userIP";
	public static final String userPort 							= "userPort";
	
	public static final String anonymizedIDToGUIDMappingColName     = "anonymizedIDToGUIDMapping";
	
	// this col stores attrs which are not set by the user.
	// this information is used in indexing scheme.
	public static final String unsetAttrsColName					= "unsetAttrs";
	
	public static final String GUID_HASH_TABLE_NAME					= "guidHashDataStorage";
	
	public static final String ATTR_INDEX_TABLE_NAME				= "attrIndexDataStorage";
	
	public static final String ATTR_INDEX_TRIGGER_TABLE_NAME		= "triggerDataStorage";
	
	public static final String HASH_INDEX_TRIGGER_TABLE_NAME		= "queryHashTriggerDataStorage";
	
	
	//unsetAttrsColName is varchar type for now.
	// FIXME: currently JSONObject is stored as string, but in future
	// it should be changed to bitmap to save space and stringification overhead.
	public static final int varcharSizeForunsetAttrsCol				= 1000;
	
	private AbstractDataSource abstractDataSource;
	
	private final GUIDStorageInterface guidAttributesStorage;
	private  TriggerInformationStorageInterface triggerInformationStorage;
	
	//private final Random randomGen;
	
	public RegionMappingDataStorageDB( Integer myNodeID )
			throws Exception
	{
//		if(ContextServiceConfig.disableMySQLDB)
//		{
//			randomGen = new Random((Integer)myNodeID);
//		}
//		else
//		{
//			randomGen = null;
//		}
		
		if(ContextServiceConfig.sqlDBType == ContextServiceConfig.SQL_DB_TYPE.MYSQL)
		{
			this.abstractDataSource = new MySQLDataSource(myNodeID);
		}
		else if(ContextServiceConfig.sqlDBType == ContextServiceConfig.SQL_DB_TYPE.SQLITE)
		{
			this.abstractDataSource = new SQLiteDataSource(myNodeID);
		}
		
		guidAttributesStorage = new SQLGUIDStorage
							(myNodeID, abstractDataSource);
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			// Currently it is assumed that there are only conjunctive queries
			// DNF form queries can be added by inserting its multiple conjunctive 
			// components.
			ContextServiceLogger.getLogger().fine( "HyperspaceMySQLDB "
					+ " TRIGGER_ENABLED "+ContextServiceConfig.TRIGGER_ENABLED );
			triggerInformationStorage = new TriggerInformationStorage
											(myNodeID , abstractDataSource);
		}
		
		createTables();
	}
	
	/**
	 * Creates tables needed for 
	 * the database.
	 * @throws SQLException
	 */
	private void createTables()
	{	
		// slightly inefficient way of creating tables
		// as it loops through subspaces three times
		// instead of one, but it only happens in the beginning
		// so not a bottleneck.
		guidAttributesStorage.createDataStorageTables();
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			// currently it is assumed that there are only conjunctive queries
			// DNF form queries can be added by inserting its multiple conjunctive components.			
			triggerInformationStorage.createTriggerStorageTables();
		}
	}
	
	/**
	 * Returns a list of regions/nodes that overlap with a query in a given subspace.
	 * @param subspaceNum
	 * @param qcomponents, takes matching attributes as input
	 * @return
	 */
//	public HashMap<Integer, RegionInfoClass> 
//		getOverlappingRegionsInSubspace(int subspaceId, int replicaNum, 
//				HashMap<String, ProcessingQueryComponent> matchingQueryComponents)
//	{
//		return this.guidAttributesStorage.getOverlappingRegionsInSubspace
//							(subspaceId, replicaNum, matchingQueryComponents);
//	}
	
	/**
	 * This function is implemented here as it involves 
	 * joining guidAttrValueStorage and privacy storage tables.
	 * @param subspaceId
	 * @param query
	 * @param resultArray
	 * @return
	 */
	public int processSearchQueryUsingAttrIndex( ValueSpaceInfo queryValueSpace, 
			JSONArray resultArray )
	{
		long start = System.currentTimeMillis();
		int resultSize 
			= this.guidAttributesStorage.processSearchQueryUsingAttrIndex
												(queryValueSpace, resultArray);
		
		long end = System.currentTimeMillis();
		
		if( ContextServiceConfig.DEBUG_MODE )
		{
			System.out.println("TIME_DEBUG: processSearchQueryInSubspaceRegion "
					+ " without privacy time "
					+ (end-start));
		}
		return resultSize;	
	}
	
	public JSONObject getGUIDStoredUsingHashIndex( String guid )
	{
		JSONObject valueJSON 
						= this.guidAttributesStorage.getGUIDStoredUsingHashIndex(guid);
		return valueJSON;
	}
	
	/**
	 * Inserts trigger info on a query into the table
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoTriggerDataStorage(  
			String userQuery, String groupGUID, String userIP, 
			int userPort, long expiryTimeFromNow )
	{
		this.triggerInformationStorage.insertIntoTriggerDataStorage
			(userQuery, groupGUID, userIP, userPort, expiryTimeFromNow);
	}
	
	/**
	 * Returns a JSONArray of JSONObjects denoting each row in the table
	 * @param subspaceNum
	 * @param hashCode
	 * @return
	 * @throws InterruptedException 
	 */
	public void getTriggerDataInfo( JSONObject oldValJSON, JSONObject updateAttrJSON, 
		HashMap<String, GroupGUIDInfoClass> oldValGroupGUIDMap, 
		HashMap<String, GroupGUIDInfoClass> newValGroupGUIDMap, 
		int requestType, JSONObject newUnsetAttrs, boolean firstTimeInsert)
				throws InterruptedException
	{
		this.triggerInformationStorage.getTriggerDataInfo
			( oldValJSON, updateAttrJSON, oldValGroupGUIDMap, 
				newValGroupGUIDMap, requestType, newUnsetAttrs, firstTimeInsert );
	}
	
	/**
	 * this function runs independently on every node 
	 * and deletes expired queries.
	 * @return
	 */
	public int deleteExpiredSearchQueries()
	{
		return this.triggerInformationStorage.deleteExpiredSearchQueries();
	}
	
	public void storeGUIDUsingHashIndex( String nodeGUID, 
    		JSONObject jsonToWrite, int updateOrInsert ) throws JSONException
	{	
		long start = System.currentTimeMillis();
		this.guidAttributesStorage.storeGUIDUsingHashIndex
			( nodeGUID, jsonToWrite, updateOrInsert);
		
		long end = System.currentTimeMillis();
		
		if( ContextServiceConfig.DEBUG_MODE )
		{
			System.out.println
				( "storeGUIDInPrimarySubspace "+(end-start) );
		}
	}
	
	/**
     * Stores GUID in a subspace. The decision to store a guid on this node
     * in this subspace is not made in this function.
     * @param subspaceNum
     * @param nodeGUID
     * @param attrValuePairs
     * @param primaryOrSecondarySubspaces true if update is happening 
     * to primary subspace, false if update is for subspaces.
     * @return
     * @throws JSONException
     */
    public void storeGUIDUsingAttrIndex( String tableName, String nodeGUID, 
    		JSONObject jsonToWrite, int updateOrInsert) throws JSONException
    {	
		// no need to add realIDEntryption Info in primary subspaces.
		long start = System.currentTimeMillis();
		this.guidAttributesStorage.storeGUIDUsingAttrIndex
					(tableName, nodeGUID, jsonToWrite, updateOrInsert);
		long end = System.currentTimeMillis();
		
		if( ContextServiceConfig.DEBUG_MODE )
		{
			System.out.println
				( "storeGUIDInSubspace without privacy storage "+(end-start) );
		}
    }
	
	public void deleteGUIDFromTable(String tableName, String nodeGUID)
	{	
		long start = System.currentTimeMillis();
		this.guidAttributesStorage.deleteGUIDFromTable(tableName, nodeGUID);
		long end = System.currentTimeMillis();
		
		if(ContextServiceConfig.DEBUG_MODE)
		{
			System.out.println("deleteGUIDFromSubspaceRegion without "
					+ "privacy storage "+(end-start));
		}
	}
	
	public boolean checkAndInsertSearchQueryRecordFromPrimaryTriggerSubspace( String groupGUID, 
			String userIP, int userPort ) throws UnknownHostException
	{
		return triggerInformationStorage.checkAndInsertSearchQueryRecordFromPrimaryTriggerSubspace
				(groupGUID, userIP, userPort);
	}
}