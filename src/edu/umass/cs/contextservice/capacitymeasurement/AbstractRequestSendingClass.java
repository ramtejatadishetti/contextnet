package edu.umass.cs.contextservice.capacitymeasurement;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public abstract class AbstractRequestSendingClass implements Runnable
{
	protected long expStartTime;
	protected final Timer waitTimer;
	protected final Object waitLock = new Object();
	protected long numSent;
	protected long numRecvd;
	protected boolean threadFinished;
	protected final Object threadFinishLock = new Object();
	
	// 1% loss tolerance
	private final double lossTolerance;
	
	protected final ExecutorService	 taskES;
	protected DataSource dsInst;
	protected final int numAttrs;
	
	
	public AbstractRequestSendingClass(int nodeId, int poolSize, double lossTolerance, 
			int numAttrs )
	{
		threadFinished = false;
		this.lossTolerance = lossTolerance;
		numSent = 0;
		numRecvd = 0;
		waitTimer = new Timer();
		
		taskES = Executors.newFixedThreadPool(poolSize);
		this.numAttrs = numAttrs;
		
		try
		{	
			dsInst = new DataSource(nodeId, poolSize);
			dropAndCreateTable();
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
	
	
	private void dropAndCreateTable()
	{
		Connection myConn 	= null;
		Statement stmt 		= null;
		
		try
		{
			myConn = dsInst.getConnection();
			stmt = myConn.createStatement();
			
			String newTableCommand = "drop table "+MySQLCapacityMeasurement.DATE_TABLE_NAME;
			
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
				= "create table "+MySQLCapacityMeasurement.DATE_TABLE_NAME
										+" ( nodeGUID Binary(20) PRIMARY KEY ";
			
			
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
	
	
	protected void startExpTime()
	{
		expStartTime = System.currentTimeMillis();
	}
	
	protected void waitForFinish()
	{
		waitTimer.schedule(new WaitTimerTask(), MySQLCapacityMeasurement.WAIT_TIME);
		
		while( !checkForCompletionWithLossTolerance(numSent, numRecvd) )
		{
			synchronized(waitLock)
			{
				try
				{
					waitLock.wait();
				} catch (InterruptedException e)
				{
					e.printStackTrace();
				}
			}
		}
		//stopThis();	
		waitTimer.cancel();
		
		threadFinished = true;
		synchronized( threadFinishLock )
		{
			threadFinishLock.notify();
		}
		//System.exit(0);
	}
	
	public void waitForThreadFinish()
	{
		while( !threadFinished )
		{
			synchronized( threadFinishLock )
			{
				try 
				{
					threadFinishLock.wait();
				} catch (InterruptedException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	public class WaitTimerTask extends TimerTask
	{			
		@Override
		public void run()
		{
			// print the remaining update and query times
			// and finish the process, cancel the timer and exit JVM.
			//stopThis();
			
			double endTimeReplyRecvd = System.currentTimeMillis();
			double sysThrput= (numRecvd * 1000.0)/(endTimeReplyRecvd - expStartTime);
			
			System.out.println(this.getClass().getName()+" Result:TimeOutThroughput "+sysThrput);
			
			waitTimer.cancel();
			// just terminate the JVM
			//System.exit(0);
			threadFinished = true;
			synchronized( threadFinishLock )
			{
				threadFinishLock.notify();
			}
		}
	}
	
	protected boolean checkForCompletionWithLossTolerance(double numSent, double numRecvd)
	{
		boolean completion = false;
		
		double withinLoss = (lossTolerance * numSent)/100.0;
		if( (numSent - numRecvd) <= withinLoss )
		{
			completion = true;
		}
		return completion;
	}
	
	public abstract void incrementUpdateNumRecvd(String userGUID, long timeTaken);
	
	public abstract void incrementSearchNumRecvd(int resultSize, long timeTaken);
	
	/**
	 * Takes request sending rate as input parameter and returns 
	 * goodput
	 * @param requestSendingRate
	 * @return
	 */
	public abstract double startSendingRequests(double requestSendingRate);
}