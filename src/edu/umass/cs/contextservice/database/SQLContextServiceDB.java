package edu.umass.cs.contextservice.database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.records.MetadataTableInfo;
import edu.umass.cs.contextservice.database.records.ValueTableInfo;
import edu.umass.cs.contextservice.examples.basic.CSTestConfig;

public class SQLContextServiceDB<NodeIDType>
{
	private final Connection dbConnection;
	private final NodeIDType myNodeID;
	private final HashMap<NodeIDType, SQLNodeInfo> sqlNodeInfoMap;
	
	public SQLContextServiceDB(NodeIDType myNodeID) throws Exception
	{
		this.myNodeID = myNodeID;
		readDBNodeSetup();
		
		dbConnection = getConnection();
		
		sqlNodeInfoMap = new HashMap<NodeIDType, SQLNodeInfo>();
		
		// create necessary tables
		createTables();
	}
	
	/**
	 * creates tables needed for the database.
	 * @throws SQLException
	 */
	private void createTables() throws SQLException
	{
		Statement stmt = (Statement) dbConnection.createStatement();
		
		for( int i=0; i<ContextServiceConfig.NUM_ATTRIBUTES; i++ )
		{
			String attName   = ContextServiceConfig.CONTEXT_ATTR_PREFIX+i;
			String tableName = attName+ContextServiceConfig.MetadataTableSuffix;
			
			String newTableCommand = "create table "+tableName+" ( "
				      + "   lowerRange DOUBLE NOT NULL, upperRange DOUBLE NOT NULL, nodeID INT NOT NULL, "
				      + "   partitionNum INT AUTO_INCREMENT, INDEX USING BTREE (lowerRange), "
				      + "   INDEX USING BTREE (upperRange) )";
			
			stmt.executeUpdate(newTableCommand);
			
			tableName = attName+ContextServiceConfig.ValueTableSuffix;
			
			// char 45 for GUID because, GUID is 40 char in length, 5 just additional
			newTableCommand = "create table "+tableName+" ( "
				      + "   value DOUBLE NOT NULL, nodeGUID CHAR(45) PRIMARY KEY, versionNum INT NOT NULL,"
				      + " INDEX USING BTREE (value) )";
			
			stmt.executeUpdate(newTableCommand);
			
			for( int j=0;j<ContextServiceConfig.NUM_RANGE_PARTITIONS;j++ )
			{
				tableName       = "groupGUIDTable"+j;
				newTableCommand = "create table "+tableName+" ( "
				      + "   groupGUID PRIMARY KEY, groupQuery )";
				
				stmt.executeUpdate(newTableCommand);
			}
		}
		
		// creating full GUID storage table.
		String tableName = "fullObjectTable";
		
		String newTableCommand = "create table "+tableName+" ( "
			      + "   nodeGUID CHAR(45) PRIMARY KEY";
		
		//	      + ", upperRange DOUBLE NOT NULL, nodeID INT NOT NULL, "
		//	      + "   partitionNum INT AUTO_INCREMENT, INDEX USING BTREE (lowerRange, upperRange) )";
		for(int i=0; i<ContextServiceConfig.NUM_ATTRIBUTES; i++)
		{
			newTableCommand += newTableCommand + ", contextATT"+i+" DOUBLE";
		}
		newTableCommand += newTableCommand +" )";
		stmt.executeUpdate(newTableCommand);
		
		stmt.close();
	}
	
	private Connection getConnection() throws Exception
	{
		int portNum 	= this.sqlNodeInfoMap.get(this.myNodeID).portNum;
		String dirName  = this.sqlNodeInfoMap.get(this.myNodeID).directoryName;
		
		//String driver = "org.gjt.mm.mysql.Driver";
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://localhost:"+portNum+"/contextDB?socket=/home/ayadav/"+dirName+"/thesock";
		String username = "root";
		String password = "aditya";
		Class.forName(driver);
		Connection conn = (Connection) DriverManager.getConnection(url, username, password);
		return conn;
	}
	
	public List<MetadataTableInfo<Integer>> 
		getAttributeMetaObjectRecord(String attrName, double queryMin, double queryMax)
	{
		List<MetadataTableInfo<Integer>> answerList 
						= new LinkedList<MetadataTableInfo<Integer>>();
		
		try
		{
			// three cases to check, documentation
			// trying to find if there is an overlap in the ranges, 
			// the range specified by user and the range in database.
			// overlap is there if queryMin lies between the range in database
			// or queryMax lies between the range in database.
			// So, we specify two or conditions.
			// for right side value, it can't be equal to rangestart, 
			// but it can be equal to rangeEnd, although even then it doesn't include
			// rangeEnd.
			// or the range lies in between the queryMin and queryMax
			
			String tableName = attrName+ContextServiceConfig.MetadataTableSuffix;
			String selectTableSQL = "SELECT nodeID, partitionNum from "+tableName+" WHERE "
					+ "( lowerRange <= "+queryMin +" AND upperRange > "+queryMin+" ) OR "
					+ "( lowerRange < "+queryMax +" AND upperRange >= "+queryMax+" ) OR "
					+ "( lowerRange >= "+queryMin +" AND upperRange < "+queryMax+" ) ";
			
			Statement stmt = (Statement) dbConnection.createStatement();
			
			ResultSet rs = stmt.executeQuery(selectTableSQL);
			
		    while( rs.next() )
		    {
		    	//Retrieve by column name
		    	int nodeID  	 = rs.getInt("nodeID");
		    	int partitionNum = rs.getInt("partitionNum");
		    	
		    	MetadataTableInfo<Integer> metaobj = new MetadataTableInfo<Integer>(nodeID, partitionNum);
		    	answerList.add(metaobj);
		    }
		    
		    rs.close();
		    stmt.close();
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		return answerList;
	}
	
	public JSONArray getValueInfoObjectRecord
							(String attrName, double queryMin, double queryMax)
	{
		//List<ValueTableInfo> answerList 
		//	= new LinkedList<ValueTableInfo>();
		
		JSONArray jsoArray = new JSONArray();
		
		try
		{
			String tableName = attrName+ContextServiceConfig.ValueTableSuffix;
			String selectTableSQL = "SELECT value, nodeGUID from "+tableName+" WHERE "
					+ "( value >= "+queryMin +" AND value < "+queryMax+" )";

			Statement stmt = (Statement) dbConnection.createStatement();
			
			ResultSet rs = stmt.executeQuery(selectTableSQL);
			
			while( rs.next() )
			{
				//Retrieve by column name
				//double value  	 = rs.getDouble("value");
				String nodeGUID = rs.getString("nodeGUID");
			
				//ValueTableInfo valobj = new ValueTableInfo(value, nodeGUID);
				//answerList.add(valobj);
				jsoArray.put(nodeGUID);
			}
		
			rs.close();
			stmt.close();
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		return jsoArray;
	}
	
	public void putAttributeMetaObjectRecord
							(String attrName, double lowerRange, double upperRange, NodeIDType nodeID)
	{
		Statement statement = null;

		String tableName = attrName+ContextServiceConfig.MetadataTableSuffix;
		String insertTableSQL = "INSERT INTO "+tableName 
				+"(lowerRange, upperRange, nodeID) " + "VALUES"
				+ "("+lowerRange+","+upperRange+","+nodeID +")";

		try 
		{
			statement = (Statement) dbConnection.createStatement();

			// execute insert SQL stetement
			statement.executeUpdate(insertTableSQL);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		} finally
		{
			if (statement != null) 
			{
				try 
				{
					statement.close();
				} catch (SQLException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	public void putValueObjectRecord(String attrName, double value, String nodeGUID, 
			long versionNum)
	{
		Statement statement = null;

		String tableName = attrName+ContextServiceConfig.ValueTableSuffix;
		String insertTableSQL = "INSERT INTO "+tableName 
				+"(value, nodeGUID, versionNum) " + "VALUES"
				+ "("+value+",'"+nodeGUID+"',"+versionNum +")";

		try 
		{
			statement = (Statement) dbConnection.createStatement();

			// execute insert SQL stetement
			statement.executeUpdate(insertTableSQL);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		} finally
		{
			if (statement != null) 
			{
				try 
				{
					statement.close();
				} catch (SQLException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	// updates valueInfoObjectRecord
	public void updateValueInfoObjectRecord(String attrName, 
				ValueTableInfo.Operations operType, String nodeGUID, double value, long versionNum)
	{
		Statement statement = null;

		String tableName = attrName+ContextServiceConfig.ValueTableSuffix;
		
		String sqlQuery = null;
		switch(operType)
		{
			case REMOVE:
			{
				sqlQuery = "DELETE FROM "+tableName +
		                   " WHERE nodeGUID = '"+nodeGUID+"'";
				break;
			}
			case UPDATE:
			{
				sqlQuery = "UPDATE "+tableName
						+ " SET value = "+value+" , versionNum = "+versionNum
						+ " WHERE nodeGUID = '"+nodeGUID+"'";
				break;
			}
		}

		try 
		{
			statement = (Statement) dbConnection.createStatement();

			// execute insert SQL stetement
			statement.executeUpdate(sqlQuery);
		} catch (SQLException e) 
		{
			e.printStackTrace();
		} finally
		{
			if (statement != null) 
			{
				try 
				{
					statement.close();
				} catch (SQLException e) 
				{
					e.printStackTrace();
				}
			}
		}
	}
	

	@SuppressWarnings("unchecked")
	private void readDBNodeSetup() throws NumberFormatException, IOException
	{	
		BufferedReader reader = new BufferedReader(new FileReader("dbNodeSetup.txt"));
		String line = null;
		
		while ((line = reader.readLine()) != null)
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