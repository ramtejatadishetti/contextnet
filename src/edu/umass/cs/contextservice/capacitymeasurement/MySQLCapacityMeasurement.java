package edu.umass.cs.contextservice.capacitymeasurement;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class MySQLCapacityMeasurement
{
	public static final int RUN_UPDATE							= 1;
	public static final int RUN_SEARCH							= 2;
	
	// 100 seconds, experiment runs for 100 seconds
	public static final int EXPERIMENT_TIME						= 100000;
	
	// after sending all the requests it waits for 100 seconds 
	public static final int WAIT_TIME							= 100000;
	
	// 1% loss tolerance
	public static final double INSERT_LOSS_TOLERANCE			= 0.5;
			
	// 1% loss tolerance
	public static final double UPD_LOSS_TOLERANCE				= 0.5;
		
	// 1% loss tolerance
	public static final double SEARCH_LOSS_TOLERANCE			= 0.5;
	
	public static final String GUID_PREFIX						= "guidPrefix";
	
	public static final String DATE_TABLE_NAME 					= "testTable";
	
	
	public static final int ATTR_MAX							= 1500;
	public static final int ATTR_MIN							= 1;
	public static final int ATTR_DEFAULT						= 0;
	
	public static final int NUM_ATTRS_IN_QUERY					= 4;
	
	private DataSource dsInst;
	
	//private final int nodeId;
	
	private final int numGuids;
	private final int numAttrs;
	
	
	private final double requestsps;
	
	private final int poolSize;
	
	private final double predicateLength;
	
	private final ExecutorService	 taskES;
	
	public MySQLCapacityMeasurement(int nodeId, int numGuids, int numAttrs, 
			int requestType, int requestsps, int poolSize, 
			double predicateLength)
	{
		//this.nodeId = nodeId;
		this.numGuids = numGuids;
		this.numAttrs = numAttrs;
		//this.requestType = requestType;
		this.requestsps = requestsps;
		this.poolSize = poolSize;
		this.predicateLength = predicateLength;
		
		taskES = Executors.newFixedThreadPool(poolSize);
		
		try
		{
			dsInst = new DataSource(nodeId, poolSize);
			createTable();
		} catch (IOException e)
		{
			e.printStackTrace();
		} catch (SQLException e)
		{
			e.printStackTrace();
		} catch (PropertyVetoException e)
		{
			e.printStackTrace();
		}
	}
	
	public int getNumGuids()
	{
		return this.numGuids;
	}
	
	public int getNumAttrs()
	{
		return this.numAttrs;
	}
	
	public ExecutorService getExecService()
	{
		return this.taskES;
	}
	
	public double getRequestsPs()
	{
		return this.requestsps;
	}
	
	public double getPredicateLength()
	{
		return this.predicateLength;
	}
	
	public int getPoolSize()
	{
		return this.poolSize;
	}
	
	public DataSource getDataSource()
	{
		return dsInst;
	}
	
	
	private void createTable()
	{
		Connection myConn 	= null;
		Statement stmt 		= null;
		
		try
		{
			myConn = dsInst.getConnection();
			stmt = myConn.createStatement();
			
			String newTableCommand = "drop table "+DATE_TABLE_NAME;
			
			try
			{
				stmt.executeUpdate(newTableCommand);
			}
			catch(Exception ex)
			{
				System.out.println("Table delete exception");
			}
			
			// char 45 for GUID because, GUID is 40 char in length, 5 just additional
			newTableCommand 
				= "create table "+DATE_TABLE_NAME+" ( nodeGUID Binary(20) PRIMARY KEY ";
			
			
			for( int i=0; i<numAttrs; i++ )
			{
				String attrName = "attr"+i;
				
				newTableCommand = newTableCommand +" , "+ attrName+" DOUBLE NOT NULL , "
						+ "INDEX USING BTREE ("+attrName+")";
			}
			
			newTableCommand = newTableCommand +" ) ";
			
			stmt.executeUpdate(newTableCommand);
			
		}
		catch ( SQLException e )
		{
			e.printStackTrace();
			
			assert(false);
		}
		finally
		{
			try
			{
				if(stmt != null)
					stmt.close();
				
				if(myConn != null)
					myConn.close();
			}
			catch (SQLException e)
			{
				e.printStackTrace();
			}
		}
	}
	
	
	public static String getSHA1(String stringToHash)
	{
		MessageDigest md = null;
		try
		{
			md = MessageDigest.getInstance("SHA-256");
		} 
		catch (NoSuchAlgorithmException e)
		{
			e.printStackTrace();
		}
		
		md.update(stringToHash.getBytes());
		
		byte byteData[] = md.digest();
       
		//convert the byte to hex format method 1
        StringBuffer sb = new StringBuffer();
        
        for (int i = 0; i < byteData.length; i++) 
        {
        	sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
        }
        String returnGUID = sb.toString();
        return returnGUID.substring(0, 40);
	}
	
	
	public static void main( String[] args )
	{
		int nodeId 			      = Integer.parseInt(args[0]);
		int numGuids 		      = Integer.parseInt(args[1]);
		int numAttrs 		      = Integer.parseInt(args[2]);
		
		int requestType           = Integer.parseInt(args[3]);
		int requestsps            = Integer.parseInt(args[4]);
		int poolSize  		   	  = Integer.parseInt(args[5]);
		double predicateLength    = Double.parseDouble(args[6]);
		
		
		MySQLCapacityMeasurement mysqlBech = new 
				MySQLCapacityMeasurement(nodeId, numGuids, numAttrs, 
				requestType, requestsps, poolSize, 
				predicateLength);
		
		long start = System.currentTimeMillis();
		InitializeClass initClass = null;
		new Thread(initClass).start();
		initClass.waitForThreadFinish();
		
		System.out.println(numGuids+" records inserted in "
									+(System.currentTimeMillis()-start));
		
		try
		{
			Thread.sleep(10000);
		}
		catch (InterruptedException e)
		{
			e.printStackTrace();
		}
		
		
		AbstractRequestSendingClass requestTypeObj = null;
		switch( requestType )
		{
			case RUN_UPDATE:
			{
				//requestTypeObj = new UpdateClass(mysqlBech);
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				double avgUpdTime = ((UpdateClass)requestTypeObj).getAvgUpdateTime();
				System.out.println("Avg update time "+avgUpdTime);
				break;
			}
			case RUN_SEARCH:
			{
				//requestTypeObj = new SearchClass(mysqlBech);
				new Thread(requestTypeObj).start();
				requestTypeObj.waitForThreadFinish();
				double avgReplySize = ((SearchClass)requestTypeObj).getAvgResultSize();
				double avgReplyTime = ((SearchClass)requestTypeObj).getAvgTime();
				System.out.println("Average result size "
						+avgReplySize + " avg time "+avgReplyTime);
				break;
			}
			default:
				assert(false);
		}
		System.exit(0);
	}
}