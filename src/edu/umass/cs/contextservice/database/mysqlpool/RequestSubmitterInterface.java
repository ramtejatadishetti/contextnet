package edu.umass.cs.contextservice.database.mysqlpool;

import edu.umass.cs.contextservice.database.mysqlpool.callbacks.DatabaseCallBack;

public interface RequestSubmitterInterface
{
	public void submitRequest
		( String queryToExecute, DatabaseRequestTypes requestType, 
					DatabaseCallBack databaseCallBack );
}