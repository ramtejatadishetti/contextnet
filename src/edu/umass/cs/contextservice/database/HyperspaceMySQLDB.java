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
import edu.umass.cs.contextservice.messages.dataformat.SearchReplyGUIDRepresentationJSON;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.utils.Utils;


public class HyperspaceMySQLDB<NodeIDType>
{
	public static final int UPDATE_REC 								= 1;
	public static final int INSERT_REC 								= 2;
	
	// maximum query length of 1000bytes
	public static final int MAX_QUERY_LENGTH						= 1000;
	
	//public static final String userQuery = "userQuery";
	public static final String groupGUID 							= "groupGUID";
	public static final String userIP 								= "userIP";
	public static final String userPort 							= "userPort";
	
	private final DataSource<NodeIDType> mysqlDataSource;
	
	private final GUIDAttributeStorageInterface<NodeIDType> guidAttributesStorage;
	private  TriggerInformationStorageInterface<NodeIDType> triggerInformationStorage;
	
	private  PrivacyInformationStorageInterface privacyInformationStorage;
	//private ExecutorService eservice;
	
	
	public HyperspaceMySQLDB( NodeIDType myNodeID, 
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap )
			throws Exception
	{
		//this.eservice = Executors.newCachedThreadPool();
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
			privacyInformationStorage = new PrivacyInformationStorage<NodeIDType>
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
		
		if( ContextServiceConfig.PRIVACY_ENABLED )
		{
			privacyInformationStorage.createTables();
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
	 * Returns a list of nodes that overlap with a query in a trigger 
	 * partitions single subspaces
	 * @param subspaceNum
	 * @param qcomponents, takes matching attributes as input
	 * @return
	 */
	public HashMap<Integer, OverlappingInfoClass> 
		getOverlappingPartitionsInTriggers( int subspaceId, int replicaNum, 
				String attrName, ProcessingQueryComponent matchingQueryComponent )
	{
		HashMap<Integer, OverlappingInfoClass> answerlist = 
				triggerInformationStorage.getOverlappingPartitionsInTriggers
				( subspaceId, replicaNum, attrName, matchingQueryComponent );
		return answerlist;
	}
	
	/**
	 * This function is implemented here as it involves joining guidAttrValueStorage
	 * and privacy storage tables.
	 * @param subspaceId
	 * @param query
	 * @param resultArray
	 * @return
	 */
	public int processSearchQueryInSubspaceRegion(int subspaceId, String query, 
			JSONArray resultArray)
	{
		if( !ContextServiceConfig.PRIVACY_ENABLED )
		{
			
			long start = System.currentTimeMillis();
			int resultSize 
				= this.guidAttributesStorage.processSearchQueryInSubspaceRegion
				(subspaceId, query, resultArray);
			long end = System.currentTimeMillis();
			
			if( ContextServiceConfig.DEBUG_MODE )
			{
				System.out.println("TIME_DEBUG: processSearchQueryInSubspaceRegion without privacy time "
						+(end-start));
			}
			
			return resultSize;
		}
		else
		{
			// get nested search query for subspace region
			String nestedSearchQuery = guidAttributesStorage.getMySQLQueryForProcessSearchQueryInSubspaceRegion
					(subspaceId, query);
			
			String joinQuery = 
					privacyInformationStorage.getMySQLQueryForFetchingRealIDMappingForQuery(query, subspaceId);
			
			// just ordering by nodeGUID so that we can aggregate without creating an additional map
			// not sure what overhead it adds, as it adds sorting overhead.
			String fullQuery = joinQuery + nestedSearchQuery + " ) ORDER BY nodeGUID";
			
			
			Connection myConn  = null;
			Statement stmt     = null;
			int resultSize = 0;
			try
			{
				myConn = this.mysqlDataSource.getConnection();
				// for row by row fetching, otherwise default is fetching whole result
				// set in memory. http://dev.mysql.com/doc/connector-j/en/connector-j-reference-implementation-notes.html
				stmt   = myConn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, 
						java.sql.ResultSet.CONCUR_READ_ONLY);
				stmt.setFetchSize(ContextServiceConfig.MYSQL_CURSOR_FETCH_SIZE);
				
				String currID = "";
				
				long start = System.currentTimeMillis();
				
				ResultSet rs = stmt.executeQuery(fullQuery);
				JSONArray encryptedReadIDArray = null;
				
				while( rs.next() )
				{
					byte[] nodeGUIDBytes = rs.getBytes("nodeGUID");
					// it is actually a JSONArray in hexformat byte array representation.
					// reverse conversion is byte array to String and then string to JSONArray.
					byte[] realIDEncryptedBytes = rs.getBytes("realIDEncryption");
					//ValueTableInfo valobj = new ValueTableInfo(value, nodeGUID);
					//answerList.add(valobj);
					if(ContextServiceConfig.sendFullReplies)
					{
						String nodeGUID = Utils.bytArrayToHex(nodeGUIDBytes);
						
						if( currID.equals(nodeGUID) )
						{
							if( realIDEncryptedBytes != null )
							{
								String encryptedHex = Utils.bytArrayToHex(realIDEncryptedBytes);
								// ignore warning, will not be null here
								encryptedReadIDArray.put(encryptedHex);
							}
						}
						else
						{
							// ignore the starting with empty string  case
							if( currID.length() > 0 )
							{
								SearchReplyGUIDRepresentationJSON searchReplyRep 
									= new SearchReplyGUIDRepresentationJSON(currID, encryptedReadIDArray);
								resultArray.put(searchReplyRep.toJSONObject());
							}
							
							currID = nodeGUID;
							// old reference gets copied in the SearchReplyGUIDRepresentationJSON
							// and just recreating a new JSONArray for the new anonymizedID
							encryptedReadIDArray = new JSONArray();
							
							
							if( realIDEncryptedBytes != null )
							{
								String encryptedHex = Utils.bytArrayToHex(realIDEncryptedBytes);
								encryptedReadIDArray.put(encryptedHex);
							}
						}
						resultSize++;
					}
					else
					{
						resultSize++;
					}
				}
				long end = System.currentTimeMillis();
				
				if( ContextServiceConfig.DEBUG_MODE )
				{
					System.out.println("TIME_DEBUG: processSearchQueryInSubspaceRegion with privacy time "
							+(end-start));
				}
				
				// do the last anonymized ID
				if(ContextServiceConfig.sendFullReplies)
				{
					if( currID.length() > 0 )
					{
						SearchReplyGUIDRepresentationJSON searchReplyRep 
							= new SearchReplyGUIDRepresentationJSON(currID, 
									encryptedReadIDArray);
						resultArray.put(searchReplyRep.toJSONObject());
					}
				}
				
				rs.close();
				stmt.close();
			} catch(SQLException sqlex)
			{
				sqlex.printStackTrace();
			} catch (JSONException e) 
			{
				e.printStackTrace();
			}
			finally
			{
				try
				{
					if( stmt != null )
						stmt.close();
					if( myConn != null )
						myConn.close();
				} catch(SQLException sqlex)
				{
					sqlex.printStackTrace();
				}
			}
			return resultSize;
		}
	}
	
	public void storePrivacyInformationOnUpdate( String nodeGUID , 
			HashMap<String, AttrValueRepresentationJSON> atrToValueRep , int subspaceId)
	{
		// FIXME: PrivacyUpdateCallBack will be removed and the code inside it 
		// will be pasted here.
//		PrivacyUpdateCallBack privacyThread 
//			= new PrivacyUpdateCallBack( nodeGUID, 
//				atrToValueRep, subspaceId, oldValJSON, 
//				privacyInformationStorage );
		privacyInformationStorage.bulkInsertPrivacyInformationBlocking
											(nodeGUID, atrToValueRep, subspaceId);
		//privacyThread.run();
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
	
	public void storeGUIDInPrimarySubspace( String tableName, String nodeGUID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep, int updateOrInsert, 
    		JSONObject oldValJSON ) throws JSONException
	{
		long start = System.currentTimeMillis();
		this.guidAttributesStorage.storeGUIDInPrimarySubspace
			(tableName, nodeGUID, atrToValueRep, updateOrInsert, oldValJSON);
		
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
    public void storeGUIDInSecondarySubspace(String tableName, String nodeGUID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep, int updateOrInsert 
    		, int subspaceId , JSONObject oldValJSON) throws JSONException
    {
    	//if( !ContextServiceConfig.PRIVACY_ENABLED )
    	{
    		// no need to add realIDEntryption Info in primary subspaces.
    		long start = System.currentTimeMillis();
    		this.guidAttributesStorage.storeGUIDInSecondarySubspace
						(tableName, nodeGUID, atrToValueRep, updateOrInsert, oldValJSON);
    		long end = System.currentTimeMillis();
    		
    		if( ContextServiceConfig.DEBUG_MODE )
    		{
    			System.out.println
    				( "storeGUIDInSubspace without privacy storage "+(end-start) );
    		}
    	}
    	/*else
    	{
    		//FIXME: need to think about updating privacy info, which is change in ACLs
    		// I think there are no updates in privacy info.
    		// if ACL changes then old anonymized IDs are removed and new ones are inserted.
    		
    		if(ContextServiceConfig.DEBUG_MODE)
    		{
    			System.out.println("STARTED storeGUIDInSecondarySubspace with privacy storage "
    								+nodeGUID);
    		}
    		// do both in parallel.
    		long start = System.currentTimeMillis();
//    		PrivacyUpdateCallBack callBack = privacyInformationStroage.bulkInsertPrivacyInformationNonBlocking
//    		(nodeGUID, atrToValueRep, subspaceId, oldValJSON);
//    		this.privacyInformationStroage.bulkInsertPrivacyInformationBlocking
//    		(nodeGUID, atrToValueRep, subspaceId, oldValJSON);
    		
    		PrivacyUpdateCallBack privacyThread 
				= new PrivacyUpdateCallBack( nodeGUID, 
						atrToValueRep, subspaceId, oldValJSON, 
						this.privacyInformationStroage );
    		
    		//privacyThread.run();
    		this.eservice.execute(privacyThread);
    		this.guidAttributesStorage.storeGUIDInSecondarySubspace
				(tableName, nodeGUID, atrToValueRep, updateOrInsert, oldValJSON);
    		
    		// wait for privacy update to finish
    		privacyThread.waitForFinish();
    		
    		long end = System.currentTimeMillis();
    		
    		if( ContextServiceConfig.DEBUG_MODE )
    		{
    			System.out.println("FINISHED storeGUIDInSecondarySubspace "
    					+ " with privacy storage "+nodeGUID+" "+(end-start) );
    		}
    	}*/
    }
	
	public void deleteGUIDFromSubspaceRegion(String tableName, String nodeGUID, 
			int subspaceId)
	{
		//if( !ContextServiceConfig.PRIVACY_ENABLED )
		//{
			long start = System.currentTimeMillis();
			this.guidAttributesStorage.deleteGUIDFromSubspaceRegion(tableName, nodeGUID);
			long end = System.currentTimeMillis();
			
			if(ContextServiceConfig.DEBUG_MODE)
    		{
    			System.out.println("deleteGUIDFromSubspaceRegion without "
    					+ "privacy storage "+(end-start));
    		}
		//}
//		else
//		{
//			long start = System.currentTimeMillis();
//			// do both in parallel.
////			PrivacyUpdateCallBack callback 
////				= privacyInformationStroage.deleteAnonymizedIDFromPrivacyInfoStorageNOnBlocking
////																		(nodeGUID, subspaceId);
//			
//			PrivacyUpdateCallBack privacyThread 
//				= new PrivacyUpdateCallBack( nodeGUID, subspaceId, 
//		    		this.privacyInformationStroage );
//			
//			//privacyThread.run();
//			this.eservice.execute(privacyThread);
//			
//			
//    		this.guidAttributesStorage.deleteGUIDFromSubspaceRegion(tableName, nodeGUID);
//    		
//    		// wait for privacy update to finish
//    		privacyThread.waitForFinish();
//    		
//    		long end = System.currentTimeMillis();
//    		
//    		if( ContextServiceConfig.DEBUG_MODE )
//    		{
//    			System.out.println("deleteGUIDFromSubspaceRegion with privacy storage "
//    									+(end-start));
//    		}
//		}
	}
	
	public boolean getSearchQueryRecordFromPrimaryTriggerSubspace( String groupGUID, 
			String userIP, int userPort ) throws UnknownHostException
	{
		return this.getSearchQueryRecordFromPrimaryTriggerSubspace
				(groupGUID, userIP, userPort);
	}
}