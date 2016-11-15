package edu.umass.cs.contextservice.capacitymeasurement;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.JSONArray;

import edu.umass.cs.contextservice.utils.Utils;

public class SearchTask implements Runnable
{
	private final String searchQuery;
	private final AbstractRequestSendingClass requestSendingTask;
	private final DataSource dataSource;
	
	public SearchTask( String searchQuery,
			AbstractRequestSendingClass requestSendingTask, DataSource dataSource )
	{
		this.searchQuery = searchQuery;
		this.requestSendingTask = requestSendingTask;
		this.dataSource = dataSource;
	}
	
	@Override
	public void run()
	{
		try
		{
			long start = System.currentTimeMillis();
			JSONArray resultArray = getValueInfoObjectRecord();
			long end = System.currentTimeMillis();
			int replySize = resultArray.length();
			requestSendingTask.incrementSearchNumRecvd(replySize, end-start);
		} catch(Exception ex)
		{
			ex.printStackTrace();
		}
		catch(Error ex)
		{
			ex.printStackTrace();
		}
	}
	
	public JSONArray getValueInfoObjectRecord()
	{
		JSONArray jsoArray = new JSONArray();
		Connection myConn = null;
		Statement stmt = null;
		
		try
		{
//			String selectTableSQL = "SELECT nodeGUID from "+tableName+" WHERE "
//			+ "( value1 >= "+queryMin1 +" AND value1 < "+queryMax1+" AND "
//					+ " value2 >= "+queryMin2 +" AND value2 < "+queryMax2+" )";
			
			myConn = dataSource.getConnection();
			stmt = myConn.createStatement();
			
			ResultSet rs = stmt.executeQuery(searchQuery);
			
			while( rs.next() )
			{
				//Retrieve by column name
				//double value  	 = rs.getDouble("value");
				byte[] nodeGUIDBytes  = rs.getBytes("nodeGUID");
				String nodeGUIDString = Utils.byteArrayToHex(nodeGUIDBytes);
			
				jsoArray.put(nodeGUIDString);
			}
			
			rs.close();
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		finally
		{
			 try 
			 {
				 if(stmt != null)
					 stmt.close();
				 
				 if(myConn != null)
					 myConn.close();
			 } catch (SQLException e) 
			 {
				 e.printStackTrace();
			 }
		}
		return jsoArray;
	}
}