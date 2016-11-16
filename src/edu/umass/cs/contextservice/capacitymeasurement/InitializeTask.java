package edu.umass.cs.contextservice.capacitymeasurement;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;

public class InitializeTask implements Runnable
{
	private final String guid;
	private final JSONObject attrValJSON;
	private final AbstractRequestSendingClass requestSendingTask;
	private final DataSource dataSource;
	
	public InitializeTask(String guid, JSONObject attrValJSON,
			AbstractRequestSendingClass requestSendingTask, DataSource dataSource)
	{
		this.guid = guid;
		this.attrValJSON = attrValJSON;
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
	
	// JSON iterator warning 
	@SuppressWarnings("unchecked")
	public void putValueObjectRecord() throws SQLException
	{
		Connection myConn = null;
		Statement statement = null;
		
		try
		{
			myConn = dataSource.getConnection();
			
			String insertTableSQL = "INSERT INTO "
					+ MySQLCapacityMeasurement.DATE_TABLE_NAME
					+ " ( nodeGUID ";
			
			
			Iterator<String> attrIter = attrValJSON.keys();
			
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				insertTableSQL = insertTableSQL + " , "+attrName;
			}
			insertTableSQL = insertTableSQL + " ) VALUES ( X'"+guid+"' ";
			
			
			attrIter = attrValJSON.keys();
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				try 
				{
					String value = attrValJSON.getString(attrName);
					insertTableSQL = insertTableSQL + " , "+value;
				} catch (JSONException e) 
				{
					e.printStackTrace();
				}
			}
			insertTableSQL = insertTableSQL + " ) ";
			
			statement = (Statement) myConn.createStatement();
			statement.executeUpdate(insertTableSQL);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		}
		finally
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