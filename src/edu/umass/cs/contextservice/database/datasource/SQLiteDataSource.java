package edu.umass.cs.contextservice.database.datasource;
import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.SQLException;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.umass.cs.contextservice.config.ContextServiceConfig;


public class SQLiteDataSource extends AbstractDataSource
{	
    private ComboPooledDataSource cpds;
    
    public SQLiteDataSource(int myNodeID) throws PropertyVetoException
    {
    	cpds = new ComboPooledDataSource();
    	cpds.setDriverClass("org.sqlite.JDBC"); //loads the jdbc driver
        
    	cpds.setJdbcUrl("jdbc:sqlite:file:contextdb"+myNodeID+"?mode=memory&cache=shared");
    	

    	cpds.setMaxPoolSize(ContextServiceConfig.MYSQL_MAX_CONNECTIONS);
    	cpds.setAutoCommitOnClose(true);
    }

    public Connection getConnection() throws SQLException 
    {
    	long start = System.currentTimeMillis();
    	Connection conn = this.cpds.getConnection();
    	if(ContextServiceConfig.DEBUG_MODE)
    	{
    		System.out.println("TIME_DEBUG getConnection "
    							+(System.currentTimeMillis()-start));	
    	}
        return conn;
    }
}