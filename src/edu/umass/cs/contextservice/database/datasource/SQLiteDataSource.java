package edu.umass.cs.contextservice.database.datasource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.umass.cs.contextservice.config.ContextServiceConfig;


public class SQLiteDataSource extends AbstractDataSource
{	
    private ComboPooledDataSource updatePool;
    
    private ComboPooledDataSource searchPool;
    
    public SQLiteDataSource(int myNodeID) throws PropertyVetoException
    {
    	updatePool = new ComboPooledDataSource();
    	updatePool.setDriverClass("org.sqlite.JDBC"); //loads the jdbc driver
        
    	updatePool.setJdbcUrl("jdbc:sqlite:file:contextdb"+myNodeID+"?mode=memory&cache=shared");
    	
        // multiple concurrent updates are not allowed.
    	updatePool.setMaxPoolSize(1);
    	updatePool.setAutoCommitOnClose(true);
    	
    	
    	searchPool = new ComboPooledDataSource();
    	searchPool.setDriverClass("org.sqlite.JDBC"); //loads the jdbc driver
        
    	searchPool.setJdbcUrl("jdbc:sqlite:file:contextdb"+myNodeID+"?mode=memory&cache=shared");
    	

    	searchPool.setMaxPoolSize(ContextServiceConfig.MYSQL_MAX_CONNECTIONS);
    	searchPool.setAutoCommitOnClose(true);
    	
    	Connection conn;
		
    	try {
			conn = searchPool.getConnection();
			Statement stmt = conn.createStatement();
	    	stmt.execute("pragma journal_mode=wal");
	    	
	    	stmt.close();
	    	conn.close();
		} catch (SQLException e) 
		{
			e.printStackTrace();
		}
    }

    public Connection getConnection(DB_REQUEST_TYPE dbReqType) throws SQLException 
    {
    	//return searchPool.getConnection();
    	
    	if(dbReqType == DB_REQUEST_TYPE.UPDATE)
    	{
    		return updatePool.getConnection();
    	}
    	else if(dbReqType == DB_REQUEST_TYPE.SELECT)
    	{
    		return searchPool.getConnection();
    	}
    	assert(false);
        return null;
    }
}