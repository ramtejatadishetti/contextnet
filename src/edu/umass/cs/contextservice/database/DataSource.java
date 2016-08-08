package edu.umass.cs.contextservice.database;
import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;

public class DataSource<NodeIDType>
{
	private final HashMap<NodeIDType, SQLNodeInfo> sqlNodeInfoMap;
	
    private ComboPooledDataSource cpds;

    public DataSource(NodeIDType myNodeID) throws IOException, SQLException, PropertyVetoException 
    {
    	this.sqlNodeInfoMap = new HashMap<NodeIDType, SQLNodeInfo>();
    	readDBNodeSetup();
    	
    	int portNum = sqlNodeInfoMap.get(myNodeID).portNum;
    	String dbName = sqlNodeInfoMap.get(myNodeID).databaseName;
    	String username = sqlNodeInfoMap.get(myNodeID).username;
    	String password = sqlNodeInfoMap.get(myNodeID).password;
    	String arguments = sqlNodeInfoMap.get(myNodeID).arguments;
    	
    	dropDB(sqlNodeInfoMap.get(myNodeID));
    	createDB(sqlNodeInfoMap.get(myNodeID));
    	
    	//"mysqlDir-compute-0-13";
    	
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver"); //loads the jdbc driver
        if(arguments.length() > 0)
        {
        	cpds.setJdbcUrl("jdbc:mysql://localhost:"+portNum+"/"+dbName+"?"+arguments);
        }
        else
        {
        	cpds.setJdbcUrl("jdbc:mysql://localhost:"+portNum+"/"+dbName);
        }
        
        
        cpds.setUser(username);
        cpds.setPassword(password);

        //the settings below are optional -- c3p0 can work with defaults
        //cpds.setMinPoolSize(5);
        //cpds.setAcquireIncrement(5);
        // NOTE: max pool size of DB affects the performance alot
        // definitely set it, don't leave it for default. default gives very bad
        // update performance. 
        // TODO: need to find its optimal value.
        // on d710 cluster 150 gives the best performance, after that performance remains same.
        // should be at least same as the hyperspace hashing pool size.
        // actually default mysql server max connection is 151. So this should be
        // set in conjuction with that. and also the hyperpsace hashing thread pool
        // size should be set greater than that. These things affect system performance a lot.
        cpds.setMaxPoolSize(ContextServiceConfig.MYSQL_MAX_CONNECTIONS);
        cpds.setAutoCommitOnClose(false);
        //cpds.setMaxStatements(180);
        ContextServiceLogger.getLogger().fine("HyperspaceMySQLDB datasource "
        		+ "max pool size "+cpds.getMaxPoolSize());
    }
    
    private void createDB(SQLNodeInfo sqlInfo)
    {
    	Connection conn = null;
    	Statement stmt = null;
    	try
    	{
    		//STEP 2: Register JDBC driver
    		Class.forName("com.mysql.jdbc.Driver");
    		
    		//STEP 3: Open a connection
    		String jdbcURL = "";
    		if(sqlInfo.arguments.length() > 0)
    		{
    			jdbcURL = "jdbc:mysql://localhost:"+sqlInfo.portNum+"?"+sqlInfo.arguments;
    		}
    		else
    		{
    			jdbcURL = "jdbc:mysql://localhost:"+sqlInfo.portNum;
    		}
    		
    	    //ContextServiceLogger.getLogger().fine("Connecting to database...");
    	    conn = DriverManager.getConnection(jdbcURL, sqlInfo.username, sqlInfo.password);

		    //STEP 4: Execute a query
		    //ContextServiceLogger.getLogger().fine("Creating database...");
		    stmt = conn.createStatement();
		    //String sql = "drop DATABASE "+sqlInfo.databaseName;
		    //stmt.executeUpdate(sql);
		    
		    String sql = "CREATE DATABASE "+sqlInfo.databaseName;
		    stmt.executeUpdate(sql);
		    //ContextServiceLogger.getLogger().fine("Database created successfully...");
    	   }catch(SQLException se){
    	      se.printStackTrace();
    	   }catch(Exception e)
    	{
    	      e.printStackTrace();
    	   }finally{
    	      //finally block used to close resources
    	      try{
    	         if(stmt!=null)
    	            stmt.close();
    	      }catch(SQLException se2){
    	      }// nothing we can do
    	      try{
    	          if(conn!=null)
    	             conn.close();
    	       }catch(SQLException se){
    	          se.printStackTrace();
    	       }//end finally try
    	    }//end try
    }
    
    private void dropDB(SQLNodeInfo sqlInfo)
    {
    	Connection conn = null;
    	Statement stmt = null;
    	try{
    		//STEP 2: Register JDBC driver
    		Class.forName("com.mysql.jdbc.Driver");
    		
    		//STEP 3: Open a connection
    		String jdbcURL = "";
    		if(sqlInfo.arguments.length() > 0)
    		{
    			jdbcURL = "jdbc:mysql://localhost:"+sqlInfo.portNum+"?"+sqlInfo.arguments;
    		}
    		else
    		{
    			jdbcURL = "jdbc:mysql://localhost:"+sqlInfo.portNum;
    		}
    		
    	    //ContextServiceLogger.getLogger().fine("Connecting to database...");
    	    conn = DriverManager.getConnection(jdbcURL, sqlInfo.username, sqlInfo.password);

		    //STEP 4: Execute a query
		    //ContextServiceLogger.getLogger().fine("Creating database...");
		    stmt = conn.createStatement();
		    String sql = "drop DATABASE "+sqlInfo.databaseName;
		    stmt.executeUpdate(sql);
		    
    	   }catch(SQLException se)
    	   {
    		  ContextServiceLogger.getLogger().info("Couldn't drop the database "+sqlInfo.databaseName);
    	      //se.printStackTrace();
    	   }catch(Exception e)
    	{
    	      e.printStackTrace();
    	   }finally{
    	      //finally block used to close resources
    	      try{
    	         if(stmt!=null)
    	            stmt.close();
    	      }catch(SQLException se2){
    	      }// nothing we can do
    	      try{
    	          if(conn!=null)
    	             conn.close();
    	       }catch(SQLException se){
    	          se.printStackTrace();
    	       }//end finally try
    	    }//end try
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
    
    @SuppressWarnings("unchecked")
	private void readDBNodeSetup() throws NumberFormatException, IOException
	{
		BufferedReader reader = new BufferedReader(new FileReader(
				ContextServiceConfig.configFileDirectory+"/"+ContextServiceConfig.dbConfigFileName));
		String line = null;
		
		while ( (line = reader.readLine()) != null )
		{
			// ignore comments
			if(line.startsWith("#"))
				continue;
			
			String [] parsed = line.split(" ");
			Integer readNodeId = Integer.parseInt(parsed[0]);
			int readPort = Integer.parseInt(parsed[1]);
			String dbName = parsed[2];
			String username = parsed[3];
			String password = parsed[4];
			String arguments = "";
			
			if(parsed.length == 6)
			{
				arguments = parsed[5];
			}
			
			SQLNodeInfo sqlNodeInfo = new SQLNodeInfo();
			
			sqlNodeInfo.portNum = readPort;
			sqlNodeInfo.databaseName = dbName;
			sqlNodeInfo.username = username;
			sqlNodeInfo.password = password;
			sqlNodeInfo.arguments = arguments;
			
					
			this.sqlNodeInfoMap.put((NodeIDType)readNodeId, sqlNodeInfo);
			
			//csNodeConfig.add(readNodeId, new InetSocketAddress(readIPAddress, readPort));
		}
		reader.close();
	}
	
	private class SQLNodeInfo
	{
		public int portNum;
		public String databaseName;
		public String username;
		public String password;
		public String arguments;	
	}
}