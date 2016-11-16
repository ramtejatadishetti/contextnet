package edu.umass.cs.contextservice.database;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Vector;


import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.guidattributes.GUIDAttributeStorage;
import edu.umass.cs.contextservice.database.guidattributes.GUIDAttributeStorageInterface;
import edu.umass.cs.contextservice.database.triggers.GroupGUIDInfoClass;
import edu.umass.cs.contextservice.database.triggers.TriggerInformationStorage;
import edu.umass.cs.contextservice.database.triggers.TriggerInformationStorageInterface;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.schemes.helperclasses.RegionInfoClass;


public class HyperspaceMySQLDB extends AbstractDB
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
	
	public static final String PRIMARY_SUBSPACE_TABLE_NAME			= "primarySubspaceDataStorage";
	
	//unsetAttrsColName is varchar type for now.
	// FIXME: currently JSONObject is stored as string, but in future
	// it should be changed to bitmap to save space and stringification overhead.
	public static final int varcharSizeForunsetAttrsCol				= 1000;
	
	private final DataSource mysqlDataSource;
	
	private final GUIDAttributeStorageInterface guidAttributesStorage;
	private  TriggerInformationStorageInterface triggerInformationStorage;
	
	private final Random randomGen;
	
	public HyperspaceMySQLDB( Integer myNodeID, 
			HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap )
			throws Exception
	{
		if(ContextServiceConfig.disableMySQLDB)
		{
			randomGen = new Random((Integer)myNodeID);
		}
		else
		{
			randomGen = null;
		}
		
		this.mysqlDataSource = new DataSource(myNodeID);
		
		guidAttributesStorage = new GUIDAttributeStorage
							(myNodeID, subspaceInfoMap , mysqlDataSource);
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			// Currently it is assumed that there are only conjunctive queries
			// DNF form queries can be added by inserting its multiple conjunctive 
			// components.
			ContextServiceLogger.getLogger().fine( "HyperspaceMySQLDB "
					+ " TRIGGER_ENABLED "+ContextServiceConfig.TRIGGER_ENABLED );
			triggerInformationStorage = new TriggerInformationStorage
											(myNodeID, subspaceInfoMap , mysqlDataSource);
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
		guidAttributesStorage.createTables();
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			// currently it is assumed that there are only conjunctive queries
			// DNF form queries can be added by inserting its multiple conjunctive components.			
			triggerInformationStorage.createTables();
		}
	}
	
	/**
	 * Returns a list of regions/nodes that overlap with a query in a given subspace.
	 * @param subspaceNum
	 * @param qcomponents, takes matching attributes as input
	 * @return
	 */
	public HashMap<Integer, RegionInfoClass> 
		getOverlappingRegionsInSubspace(int subspaceId, int replicaNum, 
				HashMap<String, ProcessingQueryComponent> matchingQueryComponents)
	{
		return this.guidAttributesStorage.getOverlappingRegionsInSubspace
							(subspaceId, replicaNum, matchingQueryComponents);
	}
	
	/**
	 * This function is implemented here as it involves 
	 * joining guidAttrValueStorage and privacy storage tables.
	 * @param subspaceId
	 * @param query
	 * @param resultArray
	 * @return
	 */
	public int processSearchQueryInSubspaceRegion(int subspaceId, 
			HashMap<String, ProcessingQueryComponent> queryComponents, 
			JSONArray resultArray)
	{
		if(ContextServiceConfig.disableMySQLDB)
		{
			return 1;
		}
		else
		{
			long start = System.currentTimeMillis();
			int resultSize 
				= this.guidAttributesStorage.processSearchQueryInSubspaceRegion
				(subspaceId, queryComponents, resultArray);
			long end = System.currentTimeMillis();
			
			if( ContextServiceConfig.DEBUG_MODE )
			{
				System.out.println("TIME_DEBUG: processSearchQueryInSubspaceRegion without privacy time "
						+(end-start));
			}
			return resultSize;
		}
	}
	
	/**
	 * Inserts a subspace region denoted by subspace vector, 
	 * integer denotes partition num in partition info 
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoSubspacePartitionInfo(int subspaceId, int replicaNum,
			List<Integer> subspaceVector, Integer respNodeId)
	{
		this.guidAttributesStorage.insertIntoSubspacePartitionInfo
						(subspaceId, replicaNum, subspaceVector, respNodeId);
	}
	
	public void bulkInsertIntoSubspacePartitionInfo( int subspaceId, int replicaNum,
			List<List<Integer>> subspaceVectorList, List<Integer> respNodeIdList )
	{
		this.guidAttributesStorage.bulkInsertIntoSubspacePartitionInfo
				(subspaceId, replicaNum, subspaceVectorList, respNodeIdList);
	}
	
	public JSONObject getGUIDStoredInPrimarySubspace( String guid )
	{
		if(ContextServiceConfig.disableMySQLDB)
		{
			assert(!ContextServiceConfig.PRIVACY_ENABLED);
			return getARandomJSON();
		}
		else
		{
			JSONObject valueJSON 
						= this.guidAttributesStorage.getGUIDStoredInPrimarySubspace(guid);
			return valueJSON;
		}
	}
	
	private JSONObject getARandomJSON()
	{
		Map<String, AttributeMetaInfo> attributeMap = AttributeTypes.attributeMap;
		Iterator<String> attrIter = attributeMap.keySet().iterator();
		JSONObject jsonObj = new JSONObject();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			AttributeMetaInfo attrMeta = attributeMap.get(attrName);
			String randVal = attrMeta.getARandomValue(this.randomGen);
			
			try 
			{
				jsonObj.put(attrName, randVal);
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
		}	
		return jsonObj;
	}
	
	/**
	 * Inserts trigger info on a query into the table
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoSubspaceTriggerDataInfo( int subspaceId, 
			String userQuery, String groupGUID, String userIP, 
			int userPort, long expiryTimeFromNow )
	{
		this.triggerInformationStorage.insertIntoSubspaceTriggerDataInfo
			(subspaceId, userQuery, groupGUID, userIP, userPort, expiryTimeFromNow);
	}
	
	/**
	 * Returns a JSONArray of JSONObjects denoting each row in the table
	 * @param subspaceNum
	 * @param hashCode
	 * @return
	 * @throws InterruptedException 
	 */
	public void getTriggerDataInfo(int subspaceId, 
		JSONObject oldValJSON, JSONObject updateAttrJSON, 
		HashMap<String, GroupGUIDInfoClass> oldValGroupGUIDMap, 
		HashMap<String, GroupGUIDInfoClass> newValGroupGUIDMap, 
		int requestType, JSONObject newUnsetAttrs, boolean firstTimeInsert) 
				throws InterruptedException
	{
		this.triggerInformationStorage.getTriggerDataInfo
			(subspaceId, oldValJSON, updateAttrJSON, oldValGroupGUIDMap, 
				newValGroupGUIDMap, requestType, newUnsetAttrs, firstTimeInsert);
	}
	
	/**
	 * this function runs independently on every node 
	 * and deletes expired queries.
	 * @return
	 */
	public int deleteExpiredSearchQueries( int subspaceId )
	{
		return this.triggerInformationStorage.deleteExpiredSearchQueries
										(subspaceId);
	}
	
	public void storeGUIDInPrimarySubspace( String nodeGUID, 
    		JSONObject jsonToWrite, int updateOrInsert ) throws JSONException
	{
		if(ContextServiceConfig.disableMySQLDB)
			return; 
		
		long start = System.currentTimeMillis();
		this.guidAttributesStorage.storeGUIDInPrimarySubspace
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
    public void storeGUIDInSecondarySubspace( String tableName, String nodeGUID, 
    		JSONObject jsonToWrite, int updateOrInsert 
    		, int subspaceId ) throws JSONException
    {
    	if(ContextServiceConfig.disableMySQLDB)
			return; 
    	
		// no need to add realIDEntryption Info in primary subspaces.
		long start = System.currentTimeMillis();
		this.guidAttributesStorage.storeGUIDInSecondarySubspace
					(tableName, nodeGUID, jsonToWrite, updateOrInsert);
		long end = System.currentTimeMillis();
		
		if( ContextServiceConfig.DEBUG_MODE )
		{
			System.out.println
				( "storeGUIDInSubspace without privacy storage "+(end-start) );
		}
    }
	
	public void deleteGUIDFromSubspaceRegion(String tableName, String nodeGUID, 
			int subspaceId)
	{
		if(ContextServiceConfig.disableMySQLDB)
			return; 
		
		long start = System.currentTimeMillis();
		this.guidAttributesStorage.deleteGUIDFromSubspaceRegion(tableName, nodeGUID);
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