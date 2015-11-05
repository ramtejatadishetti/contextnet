package edu.umass.cs.contextservice.database;
import java.beans.PropertyVetoException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import edu.umass.cs.contextservice.examples.basic.CSTestConfig;

public class DataSource<NodeIDType>
{
	//private final Connection dbConnection;
	private final NodeIDType myNodeID;
	private final HashMap<NodeIDType, SQLNodeInfo> sqlNodeInfoMap;
	
    //private static DataSource     datasource;
    private ComboPooledDataSource cpds;

    public DataSource(NodeIDType myNodeID) throws IOException, SQLException, PropertyVetoException 
    {
    	this.myNodeID = myNodeID;
    	this.sqlNodeInfoMap = new HashMap<NodeIDType, SQLNodeInfo>();
    	readDBNodeSetup();
    	
    	int portNum = sqlNodeInfoMap.get(myNodeID).portNum;
    	String dirName = sqlNodeInfoMap.get(myNodeID).directoryName;
    	//"mysqlDir-compute-0-13";
    	
        cpds = new ComboPooledDataSource();
        cpds.setDriverClass("com.mysql.jdbc.Driver"); //loads the jdbc driver
        cpds.setJdbcUrl("jdbc:mysql://localhost:"+portNum+"/contextDB?socket=/proj/MobilityFirst/ayadavDir/"+dirName+"/thesock");
        cpds.setUser("root");
        cpds.setPassword("aditya");

        //the settings below are optional -- c3p0 can work with defaults
        //cpds.setMinPoolSize(5);
        //cpds.setAcquireIncrement(5);
        cpds.setMaxPoolSize(150);
        //cpds.setMaxStatements(180);
    }

    /*public static DataSource getInstance() throws IOException, SQLException, PropertyVetoException 
    {
        if (datasource == null)
        {
            datasource = new DataSource();
            return datasource;
        } else 
        {
            return datasource;
        }
    	this;
    }*/

    public Connection getConnection() throws SQLException 
    {
        return this.cpds.getConnection();
    }
    
    @SuppressWarnings("unchecked")
	private void readDBNodeSetup() throws NumberFormatException, IOException
	{
		BufferedReader reader = new BufferedReader(new FileReader("dbNodeSetup.txt"));
		String line = null;
		
		while ( (line = reader.readLine()) != null )
		{
			String [] parsed = line.split(" ");
			Integer readNodeId = Integer.parseInt(parsed[0])+CSTestConfig.startNodeID;
			String dirName = parsed[1];
			int readPort = Integer.parseInt(parsed[2]);
			
			SQLNodeInfo sqlNodeInfo = new SQLNodeInfo();
			
			sqlNodeInfo.directoryName = dirName;
			sqlNodeInfo.portNum = readPort;
					
			this.sqlNodeInfoMap.put((NodeIDType)readNodeId, sqlNodeInfo);
			
			//csNodeConfig.add(readNodeId, new InetSocketAddress(readIPAddress, readPort));
		}
		reader.close();
	}
	
	private class SQLNodeInfo
	{
		public String directoryName;
		public int portNum;
	}
}