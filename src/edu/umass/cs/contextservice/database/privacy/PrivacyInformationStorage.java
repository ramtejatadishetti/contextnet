package edu.umass.cs.contextservice.database.privacy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.DataSource;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;

/**
 * Implements the Privacy information storage interface.
 * Implements the methods to create table and do search 
 * and updates.
 * @author adipc
 */
public class PrivacyInformationStorage<NodeIDType> 
									implements PrivacyInformationStorageInterface
{
	private final HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap;
	private final DataSource<NodeIDType> dataSource;
	//private ExecutorService eservice;
	
	public PrivacyInformationStorage(
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap , 
			DataSource<NodeIDType> dataSource )
	{
		this.subspaceInfoMap = subspaceInfoMap;
		this.dataSource = dataSource;
		//this.eservice = Executors.newCachedThreadPool();
	}
	
	@Override
	public void createTables()
	{
		Connection myConn  = null;
		Statement  stmt    = null;
		
		try
		{
			myConn = dataSource.getConnection();
			stmt   = myConn.createStatement();
			
			String tableName = "encryptionInfoStorage";
			String newTableCommand = "create table "+tableName+" ( "
				      + " nodeGUID Binary(20) NOT NULL , attrName VARCHAR("
				      + ContextServiceConfig.MAXIMUM_ATTRNAME_LENGTH+") NOT NULL , "
				      + " realIDEncryption Binary("+
					ContextServiceConfig.REAL_ID_ENCRYPTION_SIZE+") NOT NULL , "
				      		+ " subspaceId INTEGER NOT NULL , "
				      		+ " INDEX USING HASH(nodeGUID) , "
				      		+ " INDEX USING HASH(attrName) , "
				      		+ " INDEX USING HASH(realIDEncryption) , "
				      		+ " INDEX USING HASH(subspaceId) )";
			stmt.executeUpdate(newTableCommand);
			
			
			// On an update  of an attribute, each anonymized ID comes with a list of RealIDMappingInfo, this list consists 
			// for realID encrypted with a subset of ACL members of the updated attribute. Precisely, it is the 
			// intersection of the guid set of the anonymizedID and the ACL of the attribute. 
			// Each element of that list is stored as a separately in the corresponding attribute table.
//			Iterator<Integer> subapceIdIter 
//											= subspaceInfoMap.keySet().iterator();
//			
//			while( subapceIdIter.hasNext() )
//			{
//				int subspaceId = subapceIdIter.next();
//				// at least one replica and all replica have same default value for each attribute.
//				SubspaceInfo<NodeIDType> currSubspaceInfo 
//											= subspaceInfoMap.get(subspaceId).get(0);
//				
//				HashMap<String, AttributePartitionInfo> attrSubspaceMap 
//											= currSubspaceInfo.getAttributesOfSubspace();
//				
//				Iterator<String> attrIter = attrSubspaceMap.keySet().iterator();
//				
//				while( attrIter.hasNext() )
//				{
//					String newTableCommand = "";
//					
//					// FIXME: not sure whether to add the uniquness check, adding uniqueness
//					// check to db just adds more checks for inserts and increases update time.
//					// that property should be true in most cases but we don't need to assert that all time.
//					
//					// adding a subspace Id field, so that this table can be shared by multiple subspaces
//					// and on deletion a subsapce Id can be specified to delete only that rows.
//					String attrName = attrIter.next();
//					String tableName = attrName+"EncryptionInfoStorage";
//					newTableCommand = "create table "+tableName+" ( "
//						      + " nodeGUID Binary(20) NOT NULL , realIDEncryption Binary("+
//							ContextServiceConfig.REAL_ID_ENCRYPTION_SIZE+") NOT NULL , "
//						      		+ " subspaceId INTEGER NOT NULL , "
//						      		+ " INDEX USING HASH(nodeGUID) , INDEX USING HASH(realIDEncryption) , "
//						      		+ " INDEX USING HASH(subspaceId) )";
//					stmt.executeUpdate(newTableCommand);
//				}
//			}
			// creating table for storing privacy information at privacy subspace
//			String tableName = "privacyInfoStoragePrimarySubsapce";
//			String newTableCommand = "create table "+tableName+" ( "
//				      + " nodeGUID Binary(20) NOT NULL , attrName VARCHAR("
//					+ContextServiceConfig.MAXIMUM_ATTRNAME_LENGTH+") NOT NULL , "
//							+ " realIDEncryptionList TEXT NOT NULL , "
//				      		+ " INDEX USING HASH(nodeGUID) , "
//				      		+ " INDEX USING HASH(attrName) )";
//			
//			stmt.executeUpdate(newTableCommand);
		}
		catch( Exception mysqlEx )
		{
			mysqlEx.printStackTrace();
		} finally
		{
			try
			{
				if( stmt != null )
					stmt.close();
				if( myConn != null )
					myConn.close();
			} catch( SQLException sqex )
			{
				sqex.printStackTrace();
			}
		}
	}
	
	/**
	 * sample join query in privacy case
	 * SELECT HEX(attr0Alias.nodeGUID), HEX(attr0Alias.realIDEncryption) 
	 * FROM attr0Alias INNER JOIN (attr1Alias , attr2Alias) ON 
	 * (attr0Alias.nodeGUID = attr1Alias.nodeGUID AND 
	 * attr1Alias.nodeGUID = attr2Alias.nodeGUID AND 
	 * attr0Alias.realIDEncryption = attr1Alias.realIDEncryption AND 
	 * attr1Alias.realIDEncryption = attr2Alias.realIDEncryption) 
	 * WHERE attr0Alias.attrName='attr0' AND attr1Alias.attrName='attr1' 
	 * AND attr2Alias.attrName='attr2' AND 
	 * attr0Encryption.nodeGUID IN (SELECT nodeGUID FROM subspaceId0DataStorage);
	 */
	public String getMySQLQueryForFetchingRealIDMappingForQuery(String query, int subspaceId)
	{
		//TODO: move these commons functions to HyperMySQLDB
		QueryInfo<NodeIDType> qinfo = new QueryInfo<NodeIDType>(query);
		
		HashMap<String, ProcessingQueryComponent> pqComponents = qinfo.getProcessingQC();
		
		Vector<String> queryAttribtues = new Vector<String>();
		Iterator<String> attrIter = pqComponents.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			queryAttribtues.add(attrName);
		}
		
		if( queryAttribtues.size() <= 0 )
			assert(false);
		
		// sample join query
		// SELECT HEX(attr0Encryption.nodeGUID), HEX(attr0Encryption.realIDEncryption) 
		// FROM attr0Encryption INNER JOIN (attr1Encryption , attr2Encryption) ON 
		// (attr0Encryption.nodeGUID = attr1Encryption.nodeGUID AND attr1Encryption.nodeGUID = attr2Encryption.nodeGUID AND attr0Encryption.realIDEncryption = attr1Encryption.realIDEncryption AND attr1Encryption.realIDEncryption = attr2Encryption.realIDEncryption) WHERE attr0Encryption.nodeGUID IN (SELECT nodeGUID FROM subspaceId0DataStorage);
		
		// original table name, then aliases are used for self join the table.
		String originalTableName = "encryptionInfoStorage";
		// in one attribute case no need to join, considered in else
		if(queryAttribtues.size() >= 2)
		{
			// sample join query
			// SELECT HEX(attr0Encryption.nodeGUID), HEX(attr0Encryption.realIDEncryption) 
			// FROM attr0Encryption INNER JOIN (attr1Encryption , attr2Encryption) ON 
			// (attr0Encryption.nodeGUID = attr1Encryption.nodeGUID AND 
			// attr1Encryption.nodeGUID = attr2Encryption.nodeGUID AND 
			// attr0Encryption.realIDEncryption = attr1Encryption.realIDEncryption AND 
			// attr1Encryption.realIDEncryption = attr2Encryption.realIDEncryption) 
			// WHERE attr0Encryption.nodeGUID IN (SELECT nodeGUID FROM subspaceId0DataStorage);
			
			String firstAttr = queryAttribtues.get(0);
			String firstAttrAlias = firstAttr+"Alias";
			
			//String tableName = "subspaceId"+subspaceId+"DataStorage";
			String mysqlQuery = "SELECT "+firstAttrAlias+".nodeGUID as nodeGUID , "
					+ firstAttrAlias+".realIDEncryption as realIDEncryption "
							+ " FROM "+originalTableName+" "+firstAttrAlias+" INNER JOIN ( ";
			
			for( int i=1; i<queryAttribtues.size(); i++ )
			{
				String currAttrAlias = queryAttribtues.get(i)+"Alias";
				if(i != 1)
				{
					mysqlQuery = mysqlQuery +" , "+originalTableName+" "+currAttrAlias;
				}
				else
				{
					mysqlQuery = mysqlQuery +originalTableName+" "+currAttrAlias;
				}
			}
			mysqlQuery = mysqlQuery + " ) ON ( ";
			
			for(int i=0; i<(queryAttribtues.size()-1); i++)
			{
				String currAttrAlias = queryAttribtues.get(i)+"Alias";
				String nextAttrAlias = queryAttribtues.get(i+1)+"Alias";
				
				String currCondition = currAttrAlias+".nodeGUID = "+nextAttrAlias+".nodeGUID AND "+
							currAttrAlias+".realIDEncryption = "+nextAttrAlias+".realIDEncryption ";
				mysqlQuery = mysqlQuery + currCondition;
			}
			mysqlQuery = mysqlQuery + " ) WHERE ";
			
			for(int i=0; i<queryAttribtues.size(); i++)
			{
				String attrName = queryAttribtues.get(i);
				String currAttrAlias = attrName+"Alias";
				
				mysqlQuery = mysqlQuery + currAttrAlias+".attrName='"+attrName+"' AND "
						+currAttrAlias+".subspaceId="+subspaceId+" AND ";
			}
					
			mysqlQuery = mysqlQuery + firstAttrAlias+".nodeGUID IN ( ";
			
			return mysqlQuery;
		}
		else
		{
			String firstAttr = queryAttribtues.get(0);
			//String firstAttrTable = firstAttr+"EncryptionInfoStorage";
			
			String mysqlQuery = "SELECT "+originalTableName+".nodeGUID as nodeGUID , "
					+ originalTableName+".realIDEncryption as realIDEncryption "
							+ " FROM "+originalTableName+" WHERE "
							+ originalTableName+".attrName='"+firstAttr+"' AND "
							+ originalTableName+".subspaceId = "+subspaceId+" AND "
							+ originalTableName+".nodeGUID IN ( ";
			return mysqlQuery;
		}
	}
	
	public void bulkInsertPrivacyInformationBlocking( String ID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep , int insertsubspaceId )
	{
		ContextServiceLogger.getLogger().fine
								("STARTED bulkInsertPrivacyInformation called ");
		
		// just a way to get attributes.
		Iterator<Integer> subapceIdIter = subspaceInfoMap.keySet().iterator();
		
		String tableName = "encryptionInfoStorage";
		
		String insertQuery = "INSERT INTO "+tableName+" (nodeGUID , attrName , "
				+ "realIDEncryption , subspaceId) VALUES ";
		
		Connection myConn = null;
		Statement stmt	  = null;
		
		try
		{
			myConn = dataSource.getConnection();
			stmt = myConn.createStatement();
			
			boolean first = true;
			
			//FIXME: remove this out loop after the changed scheme is done and test
			// and giving performance gains.
			// Now we don't need to insert all attributes just the updated attributes 
			while( subapceIdIter.hasNext() )
			{
				int currsubspaceId = subapceIdIter.next();
				// at least one replica and all replica have same default value for each attribute.
				SubspaceInfo<NodeIDType> currSubspaceInfo 
											= subspaceInfoMap.get(currsubspaceId).get(0);
				
				HashMap<String, AttributePartitionInfo> attrSubspaceMap 
											= currSubspaceInfo.getAttributesOfSubspace();
				
				Iterator<String> attrIter = attrSubspaceMap.keySet().iterator();
				
				while( attrIter.hasNext() )
				{
					String currAttrName = attrIter.next();
					
					// just checking if this acl info for this ID and this attribute 
					// already exists, if it is already there then no need to insert.
					// on acl update, whole ID changes, so older ID acl info just gets 
					// deleted, it is never updated. There are only inserts and deletes of 
					// acl info, no updates.
					
					//FIXME: right now it cannot support updates,
					// will be fixed when we start supporting ACL changes.
					boolean ifExists = checkIfAlreadyExists
							(ID, insertsubspaceId, tableName, stmt);
					
					if(ifExists)
						continue;
					
					JSONArray realIDMappingArray = null;
					
					if( atrToValueRep.containsKey(currAttrName) )
					{
						AttrValueRepresentationJSON attrValRep = atrToValueRep.get( currAttrName );
					
						// array of hex String representation of encryption
						realIDMappingArray = attrValRep.getRealIDMappingInfo();
					}
					else
					{
						// check oldVal JSON
						// this is convention in table creation too.
						// check the primarGUIDStroage table in HyperspaceMySQLDB,
						// this is the column name and keys in oldValJSON is table column names.
//						String aclEntryKey = "ACL"+currAttrName;
//						
//						if( oldValJSON.has(aclEntryKey) )
//						{
//							try
//							{
//								String jsonArString = oldValJSON.getString(aclEntryKey);
//								if(jsonArString.length() > 0)
//								{
//									// catching here so other attrs not get affected.
//									realIDMappingArray = new JSONArray(jsonArString);
//								}
//							}
//							catch(JSONException jsonEx)
//							{
//								jsonEx.printStackTrace();
//							}
//						}
					}
					
					if(realIDMappingArray != null)
					{
						String valueStrings = 
							getARealIDMapingArrayInsertString( ID, currAttrName, 
							realIDMappingArray, insertsubspaceId );
						
						if(valueStrings.length() > 0)
						{
							if(first)
							{
								insertQuery = insertQuery +valueStrings; 
								first = false;
							}
							else
							{
								insertQuery = insertQuery +" , "+valueStrings;
							}
						}
					}
				}
			}
			
			// at least one item to insert
			if( !first )
			{
				long start = System.currentTimeMillis();
				int numInserted = stmt.executeUpdate(insertQuery);
				long end = System.currentTimeMillis();
				
				if( ContextServiceConfig.DEBUG_MODE )
				{
					System.out.println("TIME_DEBUG: bulkInsertPrivacyInformation time "
																				+(end-start) +
																				" numInserted "+numInserted);
				}
			}
			
			ContextServiceLogger.getLogger().fine("bulkInsertIntoSubspacePartitionInfo "
					+ "completed");
		}
		catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		finally
		{
			try
			{
				if( myConn != null )
				{
					myConn.close();
				}
				if( stmt != null )
				{
					stmt.close();
				}
			} catch( SQLException sqex )
			{
				sqex.printStackTrace();
			}
		}
	}
	
	public void deleteAnonymizedIDFromPrivacyInfoStorageBlocking( String nodeGUID, 
			int deleteSubspaceId )
	{
		Connection myConn = null;
		Statement stmt	  = null;
		
		try
		{
			myConn = dataSource.getConnection();
			stmt = myConn.createStatement();
			
			String tableName = "encryptionInfoStorage";
			
			String deleteCommand = "DELETE FROM "+tableName+" WHERE nodeGUID = X'"+nodeGUID
					+"' AND "+" subspaceId = "+deleteSubspaceId;
			
			long start = System.currentTimeMillis();
			int numDel = stmt.executeUpdate(deleteCommand);
			long end = System.currentTimeMillis();
			
			if(ContextServiceConfig.DEBUG_MODE)
			{
				System.out.println("TIME_DEBUG: deleteAnonymizedIDFromPrivacyInfoStorageBlocking "
						+(end-start)+" numDel "+numDel);
			}
		}
		catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		finally
		{
			try
			{
				if( myConn != null )
				{
					myConn.close();
				}
				if( stmt != null )
				{
					stmt.close();
				}
			} catch( SQLException sqex )
			{
				sqex.printStackTrace();
			}
		}
	}
	
	/**
	 * For a realIDMappingArray for an attribute it returns 
	 * the insert syntax for the mysql query for encryptionInfoStorage table
	 * @param IDString
	 * @param attrName
	 * @return
	 */
	private String getARealIDMapingArrayInsertString( String IDString, String attrName, 
			JSONArray realIDMappingArray, int subspaceId )
	{
//		String insertTableSQL = "INSERT INTO "+tableName 
//				+" ( nodeGUID , realIDEncryption , subspaceId ) VALUES ";
		// (nodeGUID, attrName, realIDEncryption , subspaceId)
		String insertTableSQL = "";
		
		if( realIDMappingArray != null )
		{
			for( int i=0; i<realIDMappingArray.length() ; i++ )
			{
				// catching JSON Exception here, so other insertions can proceed
				try
				{
					String hexStringRep = realIDMappingArray.getString(i);
	
					if(i != 0)
					{
						insertTableSQL = insertTableSQL + " , ";
					}
					insertTableSQL = insertTableSQL +"( X'"+IDString+"' , '"+attrName
							+"' , X'"+hexStringRep+"' , "+subspaceId +" ) ";
					
				} catch(JSONException jsoExcp)
				{
					jsoExcp.printStackTrace();
				}
			}
		}
		return insertTableSQL;
	}
	
	/**
	 * Checks if privacy info already exists, returns true
	 * otherwise returns false.
	 * If true returns then insert doesn't happen.
	 * @return
	 * @throws SQLException 
	 */
	private boolean checkIfAlreadyExists(String ID, int subspaceId, String attrName, 
			Statement stmt) throws SQLException
	{
		String tableName = "encryptionInfoStorage";
		String mysqlQuery = "SELECT COUNT(nodeGUID) as RowCount FROM "+tableName+
				" WHERE nodeGUID = X'"+ID+"' AND "
				+" subspaceId = "+subspaceId;
		
		long start = System.currentTimeMillis();
		ResultSet rs = stmt.executeQuery(mysqlQuery);
		long end = System.currentTimeMillis();
		
		if(ContextServiceConfig.DEBUG_MODE)
		{
			System.out.println("TIME_DEBUG: checkIfAlreadyExists time "
					+ (end-start));
		}
		
		while( rs.next() )
		{
			int rowCount = rs.getInt("RowCount");
			ContextServiceLogger.getLogger().fine("ID "+ID+" subspaceId "+subspaceId+" tableName "
					+tableName+" rowCount "+rowCount);
			if(rowCount >= 1)
			{
				rs.close();
				return true;
			}
			else
			{
				rs.close();
				return false;
			}
		}
		rs.close();
		return false;
	}
	
//	public void deleteAnonymizedIDFromPrivacyInfoStorageBlocking( String nodeGUID, 
//			int deleteSubspaceId )
//	{
//		ContextServiceLogger.getLogger().fine("deleteAnonymizedIDFromPrivacyInfoStorage called ");
//		
//		long start = System.currentTimeMillis();
//
//		// do it for each attribute separately
//		Vector<PrivacyUpdateInAttrTableThread<NodeIDType>> attrUpdates 
//					= new Vector<PrivacyUpdateInAttrTableThread<NodeIDType>>();
//
//		PrivacyUpdateStateStorage<NodeIDType> updateState 
//					= new PrivacyUpdateStateStorage<NodeIDType>(attrUpdates);
//
//		// just a way to get attributes.
//		Iterator<Integer> subapceIdIter = subspaceInfoMap.keySet().iterator();
//
//		while( subapceIdIter.hasNext() )
//		{
//			int currsubspaceId = subapceIdIter.next();
//			// at least one replica and all replica have same default value for each attribute.
//			SubspaceInfo<NodeIDType> currSubspaceInfo 
//				= subspaceInfoMap.get(currsubspaceId).get(0);
//
//			HashMap<String, AttributePartitionInfo> attrSubspaceMap 
//				= currSubspaceInfo.getAttributesOfSubspace();
//
//			Iterator<String> attrIter = attrSubspaceMap.keySet().iterator();
//
//			while( attrIter.hasNext() )
//			{
//				String currAttrName = attrIter.next();
//
//				String tableName = currAttrName+"EncryptionInfoStorage";
//
//				PrivacyUpdateInAttrTableThread<NodeIDType> attrUpdateThread 
//					= new PrivacyUpdateInAttrTableThread<NodeIDType>(
//							PrivacyUpdateInAttrTableThread.PERFORM_DELETION,  tableName , 
//							nodeGUID, null, deleteSubspaceId, dataSource, updateState);
//
//				attrUpdates.add(attrUpdateThread);
//			}
//		}
//		
//		ContextServiceLogger.getLogger().fine("attrUpdates size "+attrUpdates.size());
//
//
////		for(int i=0; i<attrUpdates.size(); i++)
////		{
////			PrivacyUpdateInAttrTableThread<NodeIDType> attrUpdateThread = attrUpdates.get(i);
////			ContextServiceLogger.getLogger().fine("Start attrUpdateThread delete "+attrUpdateThread.toString());
////			attrUpdateThread.run();
////			ContextServiceLogger.getLogger().fine("Finish attrUpdateThread delete "+attrUpdateThread.toString());
////		}
//		
//		for(int i=0; i<attrUpdates.size(); i++)
//		{
//			this.eservice.execute(attrUpdates.get(i));
//		}
//		
//		ContextServiceLogger.getLogger().fine("deleteAnonymizedIDFromPrivacyInfoStorage waiting updateState.waitForFinish()");
//		updateState.waitForFinish();
//		ContextServiceLogger.getLogger().fine("deleteAnonymizedIDFromPrivacyInfoStorage waiting updateState.waitForFinish() finished");
//		
//		long end = System.currentTimeMillis();
//
//		if( ContextServiceConfig.DEBUG_MODE )
//		{
//			System.out.println("TIME_DEBUG: deleteAnonymizedIDFromPrivacyInfoStorage time "
//									+(end-start) );
//		}
//	}
	
	
//	public void bulkInsertPrivacyInformationBlocking( String ID, 
//	HashMap<String, AttrValueRepresentationJSON> atrToValueRep , int insertsubspaceId,
//	JSONObject oldValJSON )
//{
//ContextServiceLogger.getLogger().fine
//						("STARTED bulkInsertPrivacyInformation called ");
//long start = System.currentTimeMillis();
//
//// do it for each attribute separately
//Vector<PrivacyUpdateInAttrTableThread<NodeIDType>> attrUpdates 
//					= new Vector<PrivacyUpdateInAttrTableThread<NodeIDType>>();
//
//PrivacyUpdateStateStorage<NodeIDType> updateState 
//					= new PrivacyUpdateStateStorage<NodeIDType>(attrUpdates);
//
//// just a way to get attributes.
//Iterator<Integer> subapceIdIter = subspaceInfoMap.keySet().iterator();
//
//while( subapceIdIter.hasNext() )
//{
//	int currsubspaceId = subapceIdIter.next();
//	// at least one replica and all replica have same default value for each attribute.
//	SubspaceInfo<NodeIDType> currSubspaceInfo 
//								= subspaceInfoMap.get(currsubspaceId).get(0);
//	
//	HashMap<String, AttributePartitionInfo> attrSubspaceMap 
//								= currSubspaceInfo.getAttributesOfSubspace();
//	
//	Iterator<String> attrIter = attrSubspaceMap.keySet().iterator();
//	
//	while( attrIter.hasNext() )
//	{
//		String currAttrName = attrIter.next();
//		
//		String tableName = currAttrName+"EncryptionInfoStorage";
//		
//		JSONArray realIDMappingArray = null;
//		
//		if( atrToValueRep.containsKey(currAttrName) )
//		{
//			AttrValueRepresentationJSON attrValRep = atrToValueRep.get( currAttrName );
//		
//			// array of hex String representation of encryption
//			realIDMappingArray = attrValRep.getRealIDMappingInfo();
//		}
//		else
//		{
//			// check oldVal JSON
//			// this is convention in table creation too.
//			// check the primarGUIDStroage table in HyperspaceMySQLDB,
//			// this is the column name and keys in oldValJSON is table column names.
//			String aclEntryKey = "ACL"+currAttrName;
//			
//			if( oldValJSON.has(aclEntryKey) )
//			{
//				try
//				{
//					String jsonArString = oldValJSON.getString(aclEntryKey);
//					if(jsonArString.length() > 0)
//					{
//						
//						// catching here so other attrs not get affected.
//						
//							realIDMappingArray = new JSONArray(jsonArString);
//					}
//				}
//				catch(JSONException jsonEx)
//				{
//					jsonEx.printStackTrace();
//				}
//			}
//		}
//		
//		if(realIDMappingArray != null)
//		{
//			PrivacyUpdateInAttrTableThread<NodeIDType> attrUpdateThread 
//				= new PrivacyUpdateInAttrTableThread<NodeIDType>(
//				PrivacyUpdateInAttrTableThread.PERFORM_INSERT,  tableName , ID, 
//				realIDMappingArray, insertsubspaceId, dataSource, updateState);
//			
//			attrUpdates.add(attrUpdateThread);
//		}
//	}
//}
////perform sequential for testing
////for(int i=0; i<attrUpdates.size(); i++)
////{
////	PrivacyUpdateInAttrTableThread<NodeIDType> attrUpdateThread = attrUpdates.get(i);
////	ContextServiceLogger.getLogger().fine("Start attrUpdateThread insert "+attrUpdateThread.toString());
////	attrUpdateThread.run();
////	ContextServiceLogger.getLogger().fine("Finish attrUpdateThread insert "+attrUpdateThread.toString());
////}
//	
//
//for(int i=0; i<attrUpdates.size(); i++)
//{
//	this.eservice.execute(attrUpdates.get(i));
//}
//updateState.waitForFinish();
//
//long end = System.currentTimeMillis();
//
//if( ContextServiceConfig.DEBUG_MODE )
//{
//	System.out.println("TIME_DEBUG: bulkInsertPrivacyInformation time "
//																+(end-start) );
//}
//
//ContextServiceLogger.getLogger().fine("bulkInsertIntoSubspacePartitionInfo "
//		+ "completed");
//}
	
	/**
	 * sample join query in privacy case
	 * SELECT HEX(attr0Encryption.nodeGUID), HEX(attr0Encryption.realIDEncryption) FROM attr0Encryption INNER JOIN (attr1Encryption , attr2Encryption) ON (attr0Encryption.nodeGUID = attr1Encryption.nodeGUID AND attr1Encryption.nodeGUID = attr2Encryption.nodeGUID AND attr0Encryption.realIDEncryption = attr1Encryption.realIDEncryption AND attr1Encryption.realIDEncryption = attr2Encryption.realIDEncryption) WHERE attr0Encryption.nodeGUID IN (SELECT nodeGUID FROM subspaceId0DataStorage);
	 */
//	public String getMySQLQueryForFetchingRealIDMappingForQuery(String query, int subspaceId)
//	{
//		//TODO: move these commons functions to HyperMySQLDB
//		QueryInfo<NodeIDType> qinfo = new QueryInfo<NodeIDType>(query);
//		
//		HashMap<String, ProcessingQueryComponent> pqComponents = qinfo.getProcessingQC();
//		
//		Vector<String> queryAttribtues = new Vector<String>();
//		Iterator<String> attrIter = pqComponents.keySet().iterator();
//		
//		while( attrIter.hasNext() )
//		{
//			String attrName = attrIter.next();
//			queryAttribtues.add(attrName);
//		}
//		
//		if( queryAttribtues.size() <= 0 )
//			assert(false);
//		
//		// sample join query
//		// SELECT HEX(attr0Encryption.nodeGUID), HEX(attr0Encryption.realIDEncryption) 
//		// FROM attr0Encryption INNER JOIN (attr1Encryption , attr2Encryption) ON 
//		// (attr0Encryption.nodeGUID = attr1Encryption.nodeGUID AND attr1Encryption.nodeGUID = attr2Encryption.nodeGUID AND attr0Encryption.realIDEncryption = attr1Encryption.realIDEncryption AND attr1Encryption.realIDEncryption = attr2Encryption.realIDEncryption) WHERE attr0Encryption.nodeGUID IN (SELECT nodeGUID FROM subspaceId0DataStorage);
//		
//		
//		// in one attribute case no need to join, considered in else
//		if(queryAttribtues.size() >= 2)
//		{
//			// sample join query
//			// SELECT HEX(attr0Encryption.nodeGUID), HEX(attr0Encryption.realIDEncryption) 
//			// FROM attr0Encryption INNER JOIN (attr1Encryption , attr2Encryption) ON 
//			// (attr0Encryption.nodeGUID = attr1Encryption.nodeGUID AND 
//			// attr1Encryption.nodeGUID = attr2Encryption.nodeGUID AND 
//			// attr0Encryption.realIDEncryption = attr1Encryption.realIDEncryption AND 
//			// attr1Encryption.realIDEncryption = attr2Encryption.realIDEncryption) 
//			// WHERE attr0Encryption.nodeGUID IN (SELECT nodeGUID FROM subspaceId0DataStorage);
//			
//			String firstAttr = queryAttribtues.get(0);
//			String firstAttrTable = firstAttr+"EncryptionInfoStorage";
//			
//			//String tableName = "subspaceId"+subspaceId+"DataStorage";
//			String mysqlQuery = "SELECT "+firstAttrTable+".nodeGUID as nodeGUID , "
//					+ firstAttrTable+".realIDEncryption as realIDEncryption "
//							+ " FROM "+firstAttrTable+" INNER JOIN ( ";
//			
//			for( int i=1; i<queryAttribtues.size(); i++ )
//			{
//				String currAttrTable = queryAttribtues.get(i)+"EncryptionInfoStorage";
//				if(i != 1)
//				{
//					mysqlQuery = mysqlQuery +" , "+currAttrTable;
//				}
//				else
//				{
//					mysqlQuery = mysqlQuery +currAttrTable;
//				}
//			}
//			mysqlQuery = mysqlQuery + " ) ON ( ";
//			
//			for(int i=0; i<(queryAttribtues.size()-1); i++)
//			{
//				String currAttrTable = queryAttribtues.get(i)+"EncryptionInfoStorage";
//				String nextAttrTable = queryAttribtues.get(i+1)+"EncryptionInfoStorage";
//				
//				String currCondition = currAttrTable+".nodeGUID = "+nextAttrTable+".nodeGUID AND "+
//							currAttrTable+".realIDEncryption = "+nextAttrTable+".realIDEncryption ";
//				mysqlQuery = mysqlQuery + currCondition;
//			}
//			mysqlQuery = mysqlQuery + " ) WHERE "+firstAttrTable+".subspaceId = "+subspaceId+" AND "
//			+firstAttrTable+".nodeGUID IN ( ";
//			
//			return mysqlQuery;
//		}
//		else
//		{
//			String firstAttr = queryAttribtues.get(0);
//			String firstAttrTable = firstAttr+"EncryptionInfoStorage";
//			
//			String mysqlQuery = "SELECT "+firstAttrTable+".nodeGUID as nodeGUID , "
//					+ firstAttrTable+".realIDEncryption as realIDEncryption "
//							+ " FROM "+firstAttrTable+" WHERE "+firstAttrTable+".subspaceId = "+subspaceId+" AND "
//					+firstAttrTable+".nodeGUID IN ( ";
//			return mysqlQuery;
//		}
//	}
}