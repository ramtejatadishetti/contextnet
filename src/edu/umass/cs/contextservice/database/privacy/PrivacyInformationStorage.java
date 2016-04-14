package edu.umass.cs.contextservice.database.privacy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import edu.umass.cs.contextservice.database.DataSource;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;

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
		// On an update  of an attribte, each anonymized ID comes with a list of RealIDMappingInfo, this list consists 
		// for realID encrypted with a subset of ACL members of the updated attribute. Precisely, it is the 
		// intersection of the guid set of the anonymizedID and the ACL of the attribtue. 
		// Each element of that list is stored as a separately in the corresponding attribtue table.
		
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
				
				String attrName = attrIter.next();
				String tableName = attrName+"EncryptionInfoStorage";
				newTableCommand = "create table "+tableName+" "
					      + " nodeGUID Binary(20) , realIDEncryption Binary("+REAL_ID_ENCRYPTION_SIZE+") , "
					      		+ " INDEX USING HASH(nodeGUID) , INDEX USING HASH(realIDEncryption)";
				
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
					} catch(SQLException sqex)
					{
						sqex.printStackTrace();
					}
				}
			}
		}
	}
	
}