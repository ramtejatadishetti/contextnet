package edu.umass.cs.contextservice.database.privacy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;
import java.util.concurrent.ExecutorService;

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
	//FIXME: need t fins out the exact size of realIDEncryption.
	
	// current encryption generated 128 bytes, if that changes then this has to change.
	public static final int REAL_ID_ENCRYPTION_SIZE			= 128;
	
	private final HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap;
	private final DataSource<NodeIDType> dataSource;
	private final ExecutorService execService;
	
	public PrivacyInformationStorage(
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap , 
			DataSource<NodeIDType> dataSource, ExecutorService execService )
	{
		this.subspaceInfoMap = subspaceInfoMap;
		this.dataSource = dataSource;
		this.execService = execService;
	}
	
	@Override
	public void createTables()
	{
		// On an update  of an attribute, each anonymized ID comes with a list of RealIDMappingInfo, this list consists 
		// for realID encrypted with a subset of ACL members of the updated attribute. Precisely, it is the 
		// intersection of the guid set of the anonymizedID and the ACL of the attribute. 
		// Each element of that list is stored as a separately in the corresponding attribute table.
		
		Iterator<Integer> subapceIdIter = subspaceInfoMap.keySet().iterator();
		
		while( subapceIdIter.hasNext() )
		{
			int subspaceId = subapceIdIter.next();
			// at least one replica and all replica have same default value for each attribute.
			SubspaceInfo<NodeIDType> currSubspaceInfo 
										= subspaceInfoMap.get(subspaceId).get(0);
			
			HashMap<String, AttributePartitionInfo> attrSubspaceMap 
										= currSubspaceInfo.getAttributesOfSubspace();
			
			Iterator<String> attrIter = attrSubspaceMap.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String newTableCommand = "";
				
				// FIXME: not sure whether to add the uniquness check, adding uniqueness
				// check to db just adds more checks for inserts and increases update time.
				// that property should be true in most cases but we don't need to assert that all time.
				
				// adding a subspace Id field, so that this table can be shared by multiple subspaces
				// and on deletion a subsapce Id can be specified to delete only that rows.
				String attrName = attrIter.next();
				String tableName = attrName+"EncryptionInfoStorage";
				newTableCommand = "create table "+tableName+" ( "
					      + " nodeGUID Binary(20) NOT NULL , realIDEncryption Binary("+REAL_ID_ENCRYPTION_SIZE+") NOT NULL , "
					      		+ " subspaceId INTEGER NOT NULL , "
					      		+ " INDEX USING HASH(nodeGUID) , INDEX USING HASH(realIDEncryption) , "
					      		+ " INDEX USING HASH(subspaceId) )";
				
				
				Connection myConn  = null;
				Statement  stmt    = null;
				
				try
				{
					myConn = dataSource.getConnection();
					stmt   = myConn.createStatement();
					
					stmt.executeUpdate(newTableCommand);
				}
				catch( SQLException mysqlEx )
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
		}
	}
	
	/**
	 * sample join query in privacy case
	 * SELECT HEX(attr0Encryption.nodeGUID), HEX(attr0Encryption.realIDEncryption) FROM attr0Encryption INNER JOIN (attr1Encryption , attr2Encryption) ON (attr0Encryption.nodeGUID = attr1Encryption.nodeGUID AND attr1Encryption.nodeGUID = attr2Encryption.nodeGUID AND attr0Encryption.realIDEncryption = attr1Encryption.realIDEncryption AND attr1Encryption.realIDEncryption = attr2Encryption.realIDEncryption) WHERE attr0Encryption.nodeGUID IN (SELECT nodeGUID FROM subspaceId0DataStorage);
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
			String firstAttrTable = firstAttr+"EncryptionInfoStorage";
			
			//String tableName = "subspaceId"+subspaceId+"DataStorage";
			String mysqlQuery = "SELECT "+firstAttrTable+".nodeGUID as nodeGUID , "
					+ firstAttrTable+".realIDEncryption as realIDEncryption "
							+ " FROM "+firstAttrTable+" INNER JOIN ( ";
			
			for( int i=1; i<queryAttribtues.size(); i++ )
			{
				String currAttrTable = queryAttribtues.get(i)+"EncryptionInfoStorage";
				if(i != 1)
				{
					mysqlQuery = mysqlQuery +" , "+currAttrTable;
				}
				else
				{
					mysqlQuery = mysqlQuery +currAttrTable;
				}
			}
			mysqlQuery = mysqlQuery + " ) ON ( ";
			
			for(int i=0; i<(queryAttribtues.size()-1); i++)
			{
				String currAttrTable = queryAttribtues.get(i)+"EncryptionInfoStorage";
				String nextAttrTable = queryAttribtues.get(i+1)+"EncryptionInfoStorage";
				
				String currCondition = currAttrTable+".nodeGUID = "+nextAttrTable+".nodeGUID AND "+
							currAttrTable+".realIDEncryption = "+nextAttrTable+".realIDEncryption ";
				mysqlQuery = mysqlQuery + currCondition;
			}
			mysqlQuery = mysqlQuery + " ) WHERE "+firstAttrTable+".subspaceId = "+subspaceId+" AND "
			+firstAttrTable+".nodeGUID IN ( ";
			
			return mysqlQuery;
		}
		else
		{
			String firstAttr = queryAttribtues.get(0);
			String firstAttrTable = firstAttr+"EncryptionInfoStorage";
			
			String mysqlQuery = "SELECT "+firstAttrTable+".nodeGUID as nodeGUID , "
					+ firstAttrTable+".realIDEncryption as realIDEncryption "
							+ " FROM "+firstAttrTable+" WHERE "+firstAttrTable+".subspaceId = "+subspaceId+" AND "
					+firstAttrTable+".nodeGUID IN ( ";
			return mysqlQuery;
		}
	}
	
	
	public void bulkInsertPrivacyInformation( String ID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep , int insertsubspaceId,
    		JSONObject oldValJSON )
	{
		ContextServiceLogger.getLogger().fine
								("STARTED bulkInsertPrivacyInformation called ");
		long start = System.currentTimeMillis();
		
		// do it for each attribute separately
		Vector<PrivacyUpdateInAttrTableThread<NodeIDType>> attrUpdates 
							= new Vector<PrivacyUpdateInAttrTableThread<NodeIDType>>();
		
		PrivacyUpdateStateStorage<NodeIDType> updateState 
							= new PrivacyUpdateStateStorage<NodeIDType>(attrUpdates);
		
		// just a way to get attributes.
		Iterator<Integer> subapceIdIter = subspaceInfoMap.keySet().iterator();
		
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
				
				String tableName = currAttrName+"EncryptionInfoStorage";
				
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
					String aclEntryKey = "ACL"+currAttrName;
					
					if( oldValJSON.has(aclEntryKey) )
					{
						try
						{
							String jsonArString = oldValJSON.getString(aclEntryKey);
							if(jsonArString.length() > 0)
							{
								
								// catching here so other attrs not get affected.
								
									realIDMappingArray = new JSONArray(jsonArString);
							}
						}
						catch(JSONException jsonEx)
						{
							jsonEx.printStackTrace();
						}
					}
				}
				
				if(realIDMappingArray != null)
				{
					PrivacyUpdateInAttrTableThread<NodeIDType> attrUpdateThread 
						= new PrivacyUpdateInAttrTableThread<NodeIDType>(
						PrivacyUpdateInAttrTableThread.PERFORM_INSERT,  tableName , ID, 
						realIDMappingArray, insertsubspaceId, dataSource, updateState);
					
					attrUpdates.add(attrUpdateThread);
				}
			}
		}
		//perform sequential for testing
		for(int i=0; i<attrUpdates.size(); i++)
		{
			PrivacyUpdateInAttrTableThread<NodeIDType> attrUpdateThread = attrUpdates.get(i);
			ContextServiceLogger.getLogger().fine("Start attrUpdateThread "+attrUpdateThread.toString());
			attrUpdateThread.run();
			ContextServiceLogger.getLogger().fine("Finish attrUpdateThread "+attrUpdateThread.toString());
		}
			
		
//		for(int i=0; i<attrUpdates.size(); i++)
//		{
//			this.execService.execute(attrUpdates.get(i));
//		}
		updateState.waitForFinish();
		
		long end = System.currentTimeMillis();
		
		if( ContextServiceConfig.DEBUG_MODE )
		{
			System.out.println("TIME_DEBUG: bulkInsertPrivacyInformation time "
																		+(end-start) );
		}
		
		ContextServiceLogger.getLogger().fine("bulkInsertIntoSubspacePartitionInfo "
				+ "completed");
	}
	
	public void deleteAnonymizedIDFromPrivacyInfoStorage( String nodeGUID, 
			int deleteSubspaceId )
	{	
		ContextServiceLogger.getLogger().fine("deleteAnonymizedIDFromPrivacyInfoStorage called ");
		
		long start = System.currentTimeMillis();

		// do it for each attribute separately
		Vector<PrivacyUpdateInAttrTableThread<NodeIDType>> attrUpdates 
					= new Vector<PrivacyUpdateInAttrTableThread<NodeIDType>>();

		PrivacyUpdateStateStorage<NodeIDType> updateState 
					= new PrivacyUpdateStateStorage<NodeIDType>(attrUpdates);

		// just a way to get attributes.
		Iterator<Integer> subapceIdIter = subspaceInfoMap.keySet().iterator();

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

				String tableName = currAttrName+"EncryptionInfoStorage";

				PrivacyUpdateInAttrTableThread<NodeIDType> attrUpdateThread 
					= new PrivacyUpdateInAttrTableThread<NodeIDType>(
							PrivacyUpdateInAttrTableThread.PERFORM_DELETION,  tableName , 
							nodeGUID, null, deleteSubspaceId, dataSource, updateState);

				attrUpdates.add(attrUpdateThread);
			}
		}
		
		ContextServiceLogger.getLogger().fine("attrUpdates size "+attrUpdates.size());


		for(int i=0; i<attrUpdates.size(); i++)
		{
			this.execService.execute(attrUpdates.get(i));
		}
		ContextServiceLogger.getLogger().fine("deleteAnonymizedIDFromPrivacyInfoStorage waiting updateState.waitForFinish()");
		updateState.waitForFinish();
		ContextServiceLogger.getLogger().fine("deleteAnonymizedIDFromPrivacyInfoStorage waiting updateState.waitForFinish() finished");
		
		long end = System.currentTimeMillis();

		if( ContextServiceConfig.DEBUG_MODE )
		{
			System.out.println("TIME_DEBUG: deleteAnonymizedIDFromPrivacyInfoStorage time "
									+(end-start) );
		}
	}
}