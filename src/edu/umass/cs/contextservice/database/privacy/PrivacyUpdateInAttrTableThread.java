package edu.umass.cs.contextservice.database.privacy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.JSONArray;
import org.json.JSONException;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.DataSource;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;

/**
 * Performs privacy update in an attribute table.
 * Usually privacy updates involves updating privacy 
 * table corresponding to each attribute.
 * So this thread is used to update corresponding to 
 * multiple attributes in parallel.
 * @author adipc
 *
 */
public class PrivacyUpdateInAttrTableThread<NodeIDType> implements Runnable
{
	public static final int PERFORM_INSERT				= 1;
	public static final int PERFORM_DELETION			= 2;
	
	private final int operation;
	private final String tableName;
	private final String ID;
	private final JSONArray realIDMappingArray;
	private final int subspaceId;
	private final DataSource<NodeIDType> dataSource;
	private final PrivacyUpdateStateStorage<NodeIDType> updateState;
	
	public PrivacyUpdateInAttrTableThread(int operation,  String tableName , String ID, 
			JSONArray realIDMappingArray, int subspaceId, DataSource<NodeIDType> dataSource,
			PrivacyUpdateStateStorage<NodeIDType> updateState )
	{
		this.operation = operation;
		this.tableName = tableName;
		this.ID = ID;
		this.realIDMappingArray = realIDMappingArray;
		this.subspaceId = subspaceId;
		this.dataSource = dataSource;
		this.updateState = updateState;
	}
	
	
	@Override
	public void run()
	{
		if( operation == PERFORM_INSERT )
		{
			performInsert();
		}
		else if( operation == PERFORM_DELETION )
		{
			performDeletion();
		}
		updateState.indicateFinished();
	}
	
	public String toString()
	{
		String str = "";
		if(realIDMappingArray != null)
			str = "operation: "+this.operation+" tableName: "+this.tableName+
				" ID: "+ this.ID+" subspaceId: "+subspaceId+" realIDMappingArray: "+realIDMappingArray.length();
		else
			str = "operation: "+this.operation+" tableName: "+this.tableName+
			" ID: "+ this.ID+" subspaceId: "+subspaceId+" realIDMappingArray: NULL";
		return str;
	}
	
	
	private void performInsert()
	{
		Connection myConn = null;
		Statement stmt	  = null;
		try
		{
			myConn = dataSource.getConnection();
			stmt = myConn.createStatement();
			boolean ifExists = checkIfAlreadyExists(ID, subspaceId, tableName, stmt);
			
			// just checking if this acl info for this ID anf this attribute 
			// already exists, if it is already there then no need to insert.
			// on acl update, whole ID changes, sol older ID acl info just gets 
			// deleted, it is never updated. There are only inserts and deletes of 
			// acl info, no updates.
			if( ifExists )
				return;
			
			String insertTableSQL = "INSERT INTO "+tableName 
				+" ( nodeGUID , realIDEncryption , subspaceId ) VALUES ";
		
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
						insertTableSQL = insertTableSQL +"( X'"+ID+"' , X'"+hexStringRep
								+"' , "+subspaceId +" ) ";
						
					} catch(JSONException jsoExcp)
					{
						jsoExcp.printStackTrace();
					}
					
					//insertTableSQL = insertTableSQL+;
					long start = System.currentTimeMillis();
					stmt.executeUpdate(insertTableSQL);
					long end = System.currentTimeMillis();
					
					if(ContextServiceConfig.DEBUG_MODE)
					{
						System.out.println("TIME_DEBUG: bulkInsertPrivacyInformation performInsert "
								+ "time "
								+ (end-start)+" batch length "+realIDMappingArray.length());
					}
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
	
	private void performDeletion()
	{
		String deleteCommand = "DELETE FROM "+tableName+" WHERE nodeGUID = X'"+ID+"' AND "
				+" subspaceId = "+subspaceId;
		
		Connection myConn = null;
		Statement stmt	  = null;
		
		try
		{
			myConn = this.dataSource.getConnection();
			
			//insertTableSQL = insertTableSQL+;
			stmt = myConn.createStatement();
			long start = System.currentTimeMillis();
			stmt.executeUpdate(deleteCommand);
			long end = System.currentTimeMillis();
			
			if(ContextServiceConfig.DEBUG_MODE)
			{
				System.out.println("TIME_DEBUG: deleteAnonymizedIDFromPrivacyInfoStorage performDeletion "
						+(end-start));
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
	 * Checks if privacy info already exists, returns true
	 * otherwise returns false.
	 * If true returns then insert doesn't happen.
	 * @return
	 * @throws SQLException 
	 */
	private boolean checkIfAlreadyExists(String ID, int subspaceId, String tableName, 
			Statement stmt) throws SQLException
	{		
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
}