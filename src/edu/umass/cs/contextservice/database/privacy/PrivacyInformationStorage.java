package edu.umass.cs.contextservice.database.privacy;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.DataSource;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.utils.DelayProfiler;

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
	
	public static final int REAL_ID_ENCRYPTION_SIZE			= 100;
	
	private final HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap;
	private final DataSource<NodeIDType> dataSource;
	
	
	public PrivacyInformationStorage( 
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap , 
			DataSource<NodeIDType> dataSource )
	{
		this.subspaceInfoMap = subspaceInfoMap;
		this.dataSource = dataSource;
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
				newTableCommand = "create table ( "+tableName+" "
					      + " nodeGUID Binary(20) , realIDEncryption Binary("+REAL_ID_ENCRYPTION_SIZE+") , "
					      		+ " subspaceId INTEGER , "
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
			mysqlQuery = mysqlQuery + " ) WHERE subspaceId = "+subspaceId+" AND "
			+firstAttrTable+".nodeGUID IN ( ";
			
			return mysqlQuery;
		}
		else
		{
			String firstAttr = queryAttribtues.get(0);
			String firstAttrTable = firstAttr+"EncryptionInfoStorage";
			
			String mysqlQuery = "SELECT "+firstAttrTable+".nodeGUID as nodeGUID , "
					+ firstAttrTable+".realIDEncryption as realIDEncryption "
							+ " FROM "+firstAttrTable+" WHERE subspaceId = "+subspaceId+" AND "
					+firstAttrTable+".nodeGUID IN ( ";
			return mysqlQuery;
		}
	}
	
	/**
	 * Inserts multiple attributes and their associated realIDEncryption lists in
	 * a bulk/batched insert so that we don't have to do multiple mysql inserts.
	 * @param ID can be anonymized or GUID
	 * @param atrToValueRep attrValue map 
	 * @param respNodeIdList
	 */
	public void bulkInsertPrivacyInformation( String ID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep , int subspaceId)
	{
		ContextServiceLogger.getLogger().fine
								("bulkInsertPrivacyInformation called ");
		
		long t0 							= System.currentTimeMillis();
		Connection myConn   				= null;
		PreparedStatement prepStmt      	= null;
		
		// do it for each attribute separately
		Iterator<String> attrIter = atrToValueRep.keySet().iterator();
		
		try
		{
			myConn = this.dataSource.getConnection();
			
			while( attrIter.hasNext() )
			{
				String currAttrName = attrIter.next();
				
				String tableName = currAttrName+"EncryptionInfoStorage";
				
				String insertTableSQL = " INSERT INTO "+tableName 
						+" ( nodeGUID , realIDEncryption , subspaceId ) VALUES ( ? , ? , ? )";
					
				prepStmt = myConn.prepareStatement(insertTableSQL);
				
				AttrValueRepresentationJSON attrValRep = atrToValueRep.get( currAttrName );
				
				// array of hex String representation of encryption
				JSONArray realIDMappingArray = attrValRep.getRealIDMappingInfo();
				
				if( realIDMappingArray != null )
				{
					for( int i=0; i<realIDMappingArray.length() ; i++ )
					{
						// catching JSON Exception here, so other insertions can proceed
						try
						{
							String hexStringRep = realIDMappingArray.getString(i);
							byte[] encryptionBytes = Utils.hexStringToByteArray(hexStringRep);
							byte[] IDBytes = Utils.hexStringToByteArray(ID);
		
							prepStmt.setBytes(1, IDBytes);
							prepStmt.setBytes(2, encryptionBytes);
							prepStmt.setInt(3, subspaceId);
							
							prepStmt.addBatch();
						} catch(JSONException jsoExcp)
						{
							jsoExcp.printStackTrace();
						}
					}
					
					prepStmt.executeBatch();
				}
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
				if( prepStmt != null )
				{
					prepStmt.close();
				}
			} catch(SQLException sqex)
			{
				sqex.printStackTrace();
			}
		}
		
		ContextServiceLogger.getLogger().fine("bulkInsertIntoSubspacePartitionInfo "
				+ "completed");
		
		if( ContextServiceConfig.DELAY_PROFILER_ON )
		{
			DelayProfiler.updateDelay("bulkInsertPrivacyInformation ", t0);
		}
	}
	
	
	public void deleteAnonymizedIDFromPrivacyInfoStorage(String nodeGUID, 
			int deleteSubspaceId)
	{
		long t0 = System.currentTimeMillis();
		
		// delete from all attribute tables.
		// as one subspace can contain all attributes.
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
				String currAttrName = attrIter.next();
				String tableName = currAttrName+"EncryptionInfoStorage";
				
				
				String deleteCommand = "DELETE FROM "+tableName+" WHERE nodeGUID = X'"+nodeGUID+"' AND "
						+" subspaceId = "+deleteSubspaceId;
				Connection myConn 	= null;
				Statement stmt 		= null;
				
				try
				{
					myConn = this.dataSource.getConnection();
					stmt = myConn.createStatement();
					stmt.executeUpdate(deleteCommand);
				} catch(SQLException sqex)
				{
					sqex.printStackTrace();
				}
				finally
				{
					try
					{
						if(myConn != null)
						{
							myConn.close();
						}
						if(	stmt != null )
						{
							stmt.close();
						}
					} catch(SQLException sqex)
					{
						sqex.printStackTrace();
					}
				}
				
			}
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON)
		{
			DelayProfiler.updateDelay("deleteAnonymizedIDFromPrivacyInfoStorage", t0);
		}
	}
	
}