package edu.umass.cs.contextservice.capacitymeasurement;

import java.beans.PropertyVetoException;
import java.io.IOException;
import java.sql.Connection;

import java.sql.SQLException;


import com.mchange.v2.c3p0.ComboPooledDataSource;

public class DataSource
{
    private ComboPooledDataSource cpds;
    
    private final int portNum;
    
    private final String dirName;
    
    private final String dbName;
    
    public DataSource(int nodeId, int poolSize) throws IOException, 
    										SQLException, PropertyVetoException
    {
    	portNum = 6000+nodeId;
    	//int portNum = 3306;
    	dirName = "mysqlDir-serv"+nodeId;
    	
    	dbName = "contextDB"+nodeId;
    	//dropDB();
    	//createDB();
    	
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver"); //loads the jdbc driver
        cpds.setJdbcUrl("jdbc:mysql://localhost:"+portNum+"/"+dbName+"?socket=/home/"+dirName+"/thesock");
        //cpds.setJdbcUrl("jdbc:mysql://localhost:"+portNum+"/contextDB0");
        cpds.setUser("root");
        cpds.setPassword("aditya");
        
        // the settings below are optional -- c3p0 can work with defaults
        //cpds.setMinPoolSize(5);
        //cpds.setAcquireIncrement(5);
        // 151 is default but on d710 machines it is set to 214
        cpds.setMaxPoolSize(poolSize);
        cpds.setAutoCommitOnClose(false);
        //cpds.setMaxStatements(180);
    }
    
    
    public Connection getConnection() throws SQLException 
    {
    	return this.cpds.getConnection();
    }
    
//    private void createDB()
//    {
//    	Connection conn = null;
//    	Statement stmt = null;
//    	try
//    	{
//    		//STEP 2: Register JDBC driver
//    		Class.forName("com.mysql.jdbc.Driver");
//    		//int portNum = 6000;
//        	//String dirName = "mysqlDir-serv0";
//        	
//    		//STEP 3: Open a connection
//    		String jdbcURL = "jdbc:mysql://localhost:"
//    						+portNum+"?socket=/home/"+dirName+"/thesock";
//    		
//    	    //ContextServiceLogger.getLogger().fine("Connecting to database...");
//    	    conn = DriverManager.getConnection(jdbcURL, "root", "aditya");
//    	    
//		    //STEP 4: Execute a query
//		    //ContextServiceLogger.getLogger().fine("Creating database...");
//		    stmt = conn.createStatement();
//		    String sql = "CREATE DATABASE testDB";
//		    stmt.executeUpdate(sql);
//		}
//    	catch(SQLException se)
//    	{
//    		se.printStackTrace();
//    	}catch(Exception e)
//    	{
//    		e.printStackTrace();
//    	}
//    	finally
//    	{
//    		//finally block used to close resources
//    	    try
//    	    {
//    	    	if(stmt!=null)
//    	    		stmt.close();
//    	    }
//    	    catch(SQLException se2)
//    	    {
//    	    }// nothing we can do
//    	    try
//    	    {
//    	    	if(conn!=null)
//    	    		conn.close();
//    	    }
//    	    catch(SQLException se)
//    	    {
//    	    	se.printStackTrace();
//    	    }//end finally try
//    	}//end try
//    }
//    
//    private void dropDB()
//    {
//    	Connection conn = null;
//    	Statement stmt = null;
//    	try
//    	{
//    		Class.forName("com.mysql.jdbc.Driver");
////    		int portNum = 6000;
////        	String dirName = "mysqlDir-serv0";
//        	
//    		String jdbcURL = "jdbc:mysql://localhost:"
//    							+portNum+"?socket=/home/"+dirName+"/thesock";
//    		
//    	    conn = DriverManager.getConnection(jdbcURL, "root", "aditya");
//    	    
//		    stmt = conn.createStatement();
//		    String sql = "drop DATABASE testDB";
//		    stmt.executeUpdate(sql);
//		}
//    	catch(SQLException se)
//    	{
//    		se.printStackTrace();
//    	}catch(Exception e)
//    	{
//    		e.printStackTrace();
//    	}finally
//    	{
//    		//finally block used to close resources
//    	    try
//    	    {
//    	    	if(stmt!=null)
//    	    		stmt.close();
//    	    }
//    	    catch(SQLException se2)
//    	    {
//    	    }// nothing we can do
//    	    try
//    	    {
//    	    	if(conn!=null)
//    	    		conn.close();
//    	    }
//    	    catch(SQLException se)
//    	    {
//    	    	se.printStackTrace();
//    	    }//end finally try
//    	}//end try
//    }
}