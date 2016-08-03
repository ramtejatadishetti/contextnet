package edu.umass.cs.contextservice.database.mysqlpool;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.mysqlpool.callbacks.OverlappingRegionSubspaceCallback;
import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;


public class MySQLQueryExecutor implements Runnable
{
	private final MySQLRequestStorage requestToExecute;
	//private final DataSource<NodeIDType> dataSource;
	private final Connection myConn;
	
	public MySQLQueryExecutor( MySQLRequestStorage requestToExecute, 
			Connection myConn )
	{
		this.requestToExecute = requestToExecute;
		//this.dataSource = dataSource;
		// this connection is not opened in this thread, so that we can measure 
		// mysql queue/dequeue rate, but agreement is that this connection will 
		// be closed in this thread, to prevent mysql connection leaks.
		this.myConn = myConn;
	}
	
	@Override
	public void run()
	{
		switch( requestToExecute.getRequestType() )
		{
			case OVERLAPPING_REGION_SUBSPACE:
			{
				executeOverlappingRegionSubspace();
				break;
			}
			case DELETE_EXPIRED_SEARCH_QUERIES:
			{
				break;
			}
			case DELETE_GUID_FROM_SUBSPACE_REGION:
			{
				break;
			}
			case CHECK_AND_INSERT_SEARCH_QUERY_IN_PRIMARY_TRIGGER_SUBSPACE:
			{
				break;
			}
			case GET_TRIGGER_DATAINFO:
			{
				break;
			}
			case GUID_GET_PRIMARY_SUBSPACE:
			{
				break;
			}
			case INSERT_SUBSPACE_PARTITION_INFO:
			{
				break;
			}
			case INSERT_SUBSPACE_TRIGGER_DATA_INFO:
			{
				break;
			}
			case INSERT_TRIGGER_PARITION_INFO:
			{
				break;
			}
			case OVERLAPPING_PARTITIONS_IN_TRIGGERS:
			{
				break;
			}
			case SEARCH_QUERY_IN_SUBSPACE_REGION:
			{
				executeSearchQueryInSubspaceRegion();
				break;
			}
			case STORE_GUID_PRIMARY_SUBSPACE:
			{
				break;
			}
			default:
			{
				assert(false);
				break;
			}
		}
	}
	
	private void executeOverlappingRegionSubspace()
	{
		OverlappingRegionSubspaceCallback dbCallBack 
				= (OverlappingRegionSubspaceCallback) 
						requestToExecute.getDatabaseCallBack();
		
		Statement stmt 				= null;
		
		assert(requestToExecute.getQueryToExecute().size() == 1);
		
		String selectTableSQL 		= requestToExecute.getQueryToExecute().get(0);
		
		HashMap<Integer, OverlappingInfoClass> answerList 
					= dbCallBack.getAnswerObject();
		
		try
		{
//			myConn = this.dataSource.getConnection();
			stmt = myConn.createStatement();
			ContextServiceLogger.getLogger().fine("selectTableSQL "+selectTableSQL);
			long start = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(selectTableSQL);
			long end = System.currentTimeMillis();
			
			if(ContextServiceConfig.DEBUG_MODE)
			{
				System.out.println("TIME_DEBUG: getOverlappingRegionsInSubspace time "+(end-start));
			}
			
		    while( rs.next() )
		    {
		    	//Retrieve by column name
		    	int respNodeID  	 = rs.getInt("respNodeID");
		    	//int hashCode		 = rs.getInt("hashCode");
		    	OverlappingInfoClass overlapObj = new OverlappingInfoClass();
		    	
		    	//overlapObj.hashCode = hashCode;
		    	overlapObj.respNodeId = respNodeID;
		    	overlapObj.replyArray = null;
		    	
		    	answerList.put(respNodeID, overlapObj);
		    	//MetadataTableInfo<Integer> metaobj = new MetadataTableInfo<Integer>(nodeID, partitionNum);
		    	//answerList.add( metaobj );
		    }
		    rs.close();
		} catch( SQLException sqlex )
		{
			sqlex.printStackTrace();
		}
		finally
		{
			try
			{
				if( stmt != null )
					stmt.close();
				
				if( myConn != null )
					myConn.close();
			}
			catch( SQLException sqlex )
			{
				sqlex.printStackTrace();
			}
		}
		dbCallBack.requestCompletion();
	}
	
	
	private void executeSearchQueryInSubspaceRegion()
	{
		
	}
	
	private void executeInsertSubspacePartitionInfo()
	{
		
	}
	
	private void executeGUIDGetPrimarySubspace()
	{
		
	}
	
	private void executeInsertSubspaceTriggerDataInfo()
	{
		
	}
	
	private void executeGetTriggerDataInfo()
	{
		
	}
	
	private void executeDeleteExpiredSearchQueries()
	{
		
	}
	
	private void executeStoreGUIDPrimarySubspace()
	{
		
	}
	
	private void executeDeleteGUIDFromSubspaceRegion()
	{
		
	}
	
	private void executeCheckAndInsertSearchQueryInPrimaryTriggerSubspace()
	{
		Statement stmt  = null;
		boolean found   = false;
		String selectQuery 			= requestToExecute.getQueryToExecute().get(0);
		String insertTableSQL       = requestToExecute.getQueryToExecute().get(1);
		
		try
		{
			stmt 		 	= myConn.createStatement();
			ResultSet rs 	= stmt.executeQuery(selectQuery);	
			
			while( rs.next() )
			{
				found = true;
			}
			rs.close();
			
			if( !found )
			{	
				stmt.executeUpdate(insertTableSQL);
			}
		} catch (SQLException e)
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if (stmt != null)
					stmt.close();
				if (myConn != null)
					myConn.close();
			}
			catch(SQLException e)
			{
				e.printStackTrace();
			}
		}
		
	}
}