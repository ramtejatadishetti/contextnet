package edu.umass.cs.contextservice.database;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;


import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.guidattributes.GUIDAttributeStorage;
import edu.umass.cs.contextservice.database.guidattributes.GUIDAttributeStorageInterface;
import edu.umass.cs.contextservice.database.privacy.PrivacyInformationStorage;
import edu.umass.cs.contextservice.database.privacy.PrivacyInformationStorageInterface;
import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.database.triggers.TriggerInformationStorage;
import edu.umass.cs.contextservice.database.triggers.TriggerInformationStorageInterface;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;

public class HyperspaceMySQLDB<NodeIDType>
{
	public static final int UPDATE_REC 								= 1;
	public static final int INSERT_REC 								= 2;
	
	// maximum query length of 1000bytes
	public static final int MAX_QUERY_LENGTH						= 1000;
	
	private final DataSource<NodeIDType> mysqlDataSource;
	
	private final GUIDAttributeStorageInterface<NodeIDType> guidAttributesStorage;
	private  TriggerInformationStorageInterface<NodeIDType> triggerInformationStorage;
	
	private  PrivacyInformationStorageInterface privacyInformationStroage;
	
	//public static final String userQuery = "userQuery";
	public static final String groupGUID = "groupGUID";
	public static final String userIP = "userIP";
	public static final String userPort = "userPort";
	
	
	public HyperspaceMySQLDB(NodeIDType myNodeID, 
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap)
			throws Exception
	{
		this.mysqlDataSource = new DataSource<NodeIDType>(myNodeID);
		
		guidAttributesStorage = new GUIDAttributeStorage<NodeIDType>
							( myNodeID, subspaceInfoMap , mysqlDataSource);
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			// currently it is assumed that there are only conjunctive queries
			// DNF form queries can be added by inserting its multiple conjunctive components.
			ContextServiceLogger.getLogger().fine( "HyperspaceMySQLDB "
					+ " TRIGGER_ENABLED "+ContextServiceConfig.TRIGGER_ENABLED );
			triggerInformationStorage = new TriggerInformationStorage<NodeIDType>
											(myNodeID, subspaceInfoMap , mysqlDataSource);
		}
		
		if( ContextServiceConfig.PRIVACY_ENABLED )
		{
			privacyInformationStroage = new PrivacyInformationStorage<NodeIDType>
										(subspaceInfoMap, mysqlDataSource);
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
		// slightly ineffcient way of creating tables
		// as it loops through subspaces three times
		// instead of one, but it only happens in the begginning
		// so not a bottleneck.
		guidAttributesStorage.createTables();
		
		if( ContextServiceConfig.TRIGGER_ENABLED )
		{
			// currently it is assumed that there are only conjunctive queries
			// DNF form queries can be added by inserting its multiple conjunctive components.			
			triggerInformationStorage.createTables();
		}
		
		if( ContextServiceConfig.PRIVACY_ENABLED )
		{
			privacyInformationStroage.createTables();
		}
	}
	
	/**
	 * Returns a list of regions/nodes that overlap with a query in a given subspace.
	 * @param subspaceNum
	 * @param qcomponents, takes matching attributes as input
	 * @return
	 */
	public HashMap<Integer, OverlappingInfoClass> 
		getOverlappingRegionsInSubspace(int subspaceId, int replicaNum, Vector<ProcessingQueryComponent> matchingQueryComponents)
	{
		return this.guidAttributesStorage.getOverlappingRegionsInSubspace
							(subspaceId, replicaNum, matchingQueryComponents);
	}
	
	/**
	 * Returns a list of nodes that overlap with a query in a trigger paritions single subspaces
	 * @param subspaceNum
	 * @param qcomponents, takes matching attributes as input
	 * @return
	 */
	public HashMap<Integer, OverlappingInfoClass> 
		getOverlappingPartitionsInTriggers(int subspaceId, int replicaNum, String attrName, 
				ProcessingQueryComponent matchingQueryComponent)
	{
		HashMap<Integer, OverlappingInfoClass> answerlist =
				triggerInformationStorage.getOverlappingPartitionsInTriggers
				(subspaceId, replicaNum, attrName, matchingQueryComponent);
		return answerlist;
	}
	
	
	
	public int processSearchQueryInSubspaceRegion(int subspaceId, String query, JSONArray resultArray)
	{
		int resultSize = this.guidAttributesStorage.processSearchQueryInSubspaceRegion
				(subspaceId, query, resultArray);
		return resultSize;
	}
	
	/**
	 * Inserts a subspace region denoted by subspace vector, 
	 * integer denotes partition num in partition info 
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoSubspacePartitionInfo(int subspaceId, int replicaNum,
			List<Integer> subspaceVector, NodeIDType respNodeId)
	{
		this.guidAttributesStorage.insertIntoSubspacePartitionInfo
		(subspaceId, replicaNum, subspaceVector, respNodeId);
	}
	
	public void bulkInsertIntoSubspacePartitionInfo( int subspaceId, int replicaNum,
			List<List<Integer>> subspaceVectorList, List<NodeIDType> respNodeIdList )
	{
		this.guidAttributesStorage.bulkInsertIntoSubspacePartitionInfo
		(subspaceId, replicaNum, subspaceVectorList, respNodeIdList);
	}
	
	/**
	 * Inserts a subspace region denoted by subspace vector, 
	 * integer denotes partition num in partition info 
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoTriggerPartitionInfo(int subspaceId, int replicaNum, String attrName, 
			int partitionNum, NodeIDType respNodeId)
	{
		this.triggerInformationStorage.insertIntoTriggerPartitionInfo
			(subspaceId, replicaNum, attrName, partitionNum, respNodeId);
	}
	
	public JSONObject getGUIDStoredInPrimarySubspace( String guid )
	{
		JSONObject valueJSON = this.guidAttributesStorage.getGUIDStoredInPrimarySubspace(guid);
		return valueJSON;
	}
	
	/**
	 * Inserts trigger info on a query into the table
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoSubspaceTriggerDataInfo( int subspaceId, int replicaNum, 
			String attrName, String userQuery, String groupGUID, String userIP, int userPort, long expiryTimeFromNow )
	{
		this.triggerInformationStorage.insertIntoSubspaceTriggerDataInfo
		(subspaceId, replicaNum, attrName, userQuery, groupGUID, userIP, userPort, expiryTimeFromNow);
	}
	
	/**
	 * returns a JSONArray of JSONObjects denoting each row in the table
	 * @param subspaceNum
	 * @param hashCode
	 * @return
	 * @throws InterruptedException 
	 */
	public void getTriggerDataInfo(int subspaceId, int replicaNum, String attrName, 
		JSONObject oldValJSON, JSONObject newUpdateVal, HashMap<String, JSONObject> oldValGroupGUIDMap, 
			HashMap<String, JSONObject> newValGroupGUIDMap, int oldOrNewOrBoth) throws InterruptedException
	{
		this.triggerInformationStorage.getTriggerDataInfo
		(subspaceId, replicaNum, attrName, oldValJSON, newUpdateVal, oldValGroupGUIDMap, 
				newValGroupGUIDMap, oldOrNewOrBoth);
	}
	
	/**
	 * this function runs independently on every node 
	 * and deletes expired queries.
	 * @return
	 */
	public int deleteExpiredSearchQueries( int subspaceId, int replicaNum, String attrName )
	{
		return this.triggerInformationStorage.deleteExpiredSearchQueries
										(subspaceId, replicaNum, attrName);
	}
	
	/**
     * stores GUID in a subspace. The decision to store a guid on this node
     * in this subspace is not made in this function.
     * @param subspaceNum
     * @param nodeGUID
     * @param attrValuePairs
     * @return
     * @throws JSONException
     */
    public void storeGUIDInSubspace(String tableName, String nodeGUID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep, int updateOrInsert ) throws JSONException
    {
    	this.guidAttributesStorage.storeGUIDInSubspace
    			(tableName, nodeGUID, atrToValueRep, updateOrInsert);
    }
	
	public void deleteGUIDFromSubspaceRegion(String tableName, String nodeGUID)
	{
		this.guidAttributesStorage.deleteGUIDFromSubspaceRegion(tableName, nodeGUID);
	}
	
	public boolean getSearchQueryRecordFromPrimaryTriggerSubspace(String groupGUID, 
			String userIP, int userPort) throws UnknownHostException
	{
		return this.getSearchQueryRecordFromPrimaryTriggerSubspace
				(groupGUID, userIP, userPort);
	}
}