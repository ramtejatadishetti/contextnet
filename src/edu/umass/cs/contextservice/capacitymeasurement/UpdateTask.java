package edu.umass.cs.contextservice.capacitymeasurement;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;

/**
 * Class implements the task used for 
 * update info in GNS, which is blocking so this 
 * class's object is passed in executor service
 * @author adipc
 */
public class UpdateTask implements Runnable
{
	private final String guid;
	private final JSONObject updateJSON;
	private final AbstractRequestSendingClass requestSendingTask;
	private final DataSource dataSource;
	
	public UpdateTask(String guid, JSONObject updateJSON,
			AbstractRequestSendingClass requestSendingTask, DataSource dataSource)
	{
		this.guid = guid;
		this.updateJSON = updateJSON;
		this.requestSendingTask = requestSendingTask;
		this.dataSource = dataSource;
	}
	
	@Override
	public void run()
	{
		try 
		{
			long start = System.currentTimeMillis();
			putValueObjectRecord();
			long end = System.currentTimeMillis();
			requestSendingTask.incrementUpdateNumRecvd(guid, end-start);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		}
		
	}
	
	public void putValueObjectRecord() throws SQLException
	{
		Connection myConn = null;
		Statement statement = null;

		try 
		{
			Iterator<String> attrIter = updateJSON.keys();
			
			String updateTableSQL = "UPDATE "+ MySQLCapacityMeasurement.DATE_TABLE_NAME+
					" SET ";
					//+ "value1="+value1+", value2="+value2+" where nodeGUID='"+guid+"'";
			boolean first = true;
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				
				try
				{
					String value = updateJSON.getString(attrName);
					
					if( first )
					{
						updateTableSQL = updateTableSQL + attrName+" = "+value;
						first = false;
					}
					else
					{
						updateTableSQL = updateTableSQL +" , "+attrName+" = "+value;
					}
				}
				catch(JSONException jsoExcp)
				{
					jsoExcp.printStackTrace();
				}
			}
			updateTableSQL = updateTableSQL +" where nodeGUID=X'"+guid+"'";
			

			myConn = dataSource.getConnection();
			statement = (Statement) myConn.createStatement();
			
			statement.executeUpdate(updateTableSQL);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		} finally
		{
			try 
			{
				if(statement != null)
					statement.close();
				
				if(myConn != null)
					myConn.close();
				
			} catch (SQLException e) 
			{
				e.printStackTrace();
			}
		}
	}
}