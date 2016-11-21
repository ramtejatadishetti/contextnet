package edu.umass.cs.contextservice.database.mysqlpool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.datasource.MySQLDataSource;

public class MySQLRequestDispatcher implements Runnable
{
	private final ExecutorService eService;
	private final ConcurrentLinkedQueue<MySQLRequestStorage> mysqlRequestsQueue;
	private final Object queueWaitLock;
	private final MySQLDataSource datasource;
	
	public MySQLRequestDispatcher
		( ConcurrentLinkedQueue<MySQLRequestStorage> mysqlRequestsQueue, 
				Object queueWaitLock, MySQLDataSource datasource )
	{
		this.mysqlRequestsQueue = mysqlRequestsQueue;
		this.queueWaitLock = queueWaitLock;
		// so that we have 1 connections per thread.
		eService = Executors.newFixedThreadPool
					(ContextServiceConfig.MYSQL_MAX_CONNECTIONS);
		this.datasource = datasource;
	}
	
	
	@Override
	public void run()
	{
		while(true)
		{
			synchronized( queueWaitLock )
			{
				// not checking the size of the queue here, as size is not
				// constant time operation here.
				if( mysqlRequestsQueue.peek() == null )
				{
					try
					{
						queueWaitLock.wait();
					}
					catch (InterruptedException e)
					{
						e.printStackTrace();
					}
				}
			}
			
			// not checking the size of the queue here, as size is not
			// constant time operation here.
			while( mysqlRequestsQueue.peek() != null )
			{
				MySQLRequestStorage currReq = mysqlRequestsQueue.poll();
				
				if( currReq != null )
				{
					// creating connection here so that we can measure 
					// queue and dequeue rates.
					Connection mysqlConn = null;
					try 
					{
						mysqlConn = datasource.getConnection();
					}
					catch (SQLException e)
					{
						e.printStackTrace();
					}
					finally
					{
						if( mysqlConn != null )
						{
							try
							{
								mysqlConn.close();
							} catch (SQLException e) 
							{
								e.printStackTrace();
							}
						}
					}
					
					MySQLQueryExecutor mysqlExec 
								= new MySQLQueryExecutor(currReq, mysqlConn);
					
					this.eService.execute(mysqlExec);			
				}
			}
			
		}
	}
	
}