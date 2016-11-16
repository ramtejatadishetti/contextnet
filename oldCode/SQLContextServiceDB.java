package edu.umass.cs.contextservice.database;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.Statement;

import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.records.MetadataTableInfo;
import edu.umass.cs.contextservice.database.records.ValueTableInfo;
import edu.umass.cs.contextservice.examples.basic.CSTestConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ValueUpdateMsgToMetadataNode;
import edu.umass.cs.contextservice.queryparsing.QueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryParser;

public class SQLContextServiceDB<Integer>
{
	//private final Connection dbConnection;
	private final Integer myNodeID;
	private final HashMap<Integer, SQLNodeInfo> sqlNodeInfoMap;
	
	public static final int NUM_PARALLEL_CLIENTS				= 100;
	
	private Connection[] dbConnectionArray						= new Connection[NUM_PARALLEL_CLIENTS];
	
	private final ConcurrentLinkedQueue<Connection> freedbConnQueue;
	
	private final Object dbConnFreeMonitor						= new Object();
	
	//private final Object storeFullObjMonitor					= new Object();
	
	public SQLContextServiceDB(Integer myNodeID) throws Exception
	{
		this.myNodeID = myNodeID;
		sqlNodeInfoMap = new HashMap<Integer, SQLNodeInfo>();
		readDBNodeSetup();
		
		freedbConnQueue = new ConcurrentLinkedQueue<Connection>();
		
		for(int i=0;i<NUM_PARALLEL_CLIENTS;i++)
		{
			dbConnectionArray[i] = getConnection();
			freedbConnQueue.add(dbConnectionArray[i]);
		}
		
		// create necessary tables
		createTables();
	}
	
	private Connection getAFreeConnection()
	{
		Connection dbConFree = null;
		
		while( dbConFree == null )
		{
			dbConFree = freedbConnQueue.poll();
			
			if( dbConFree == null )
			{
				synchronized(dbConnFreeMonitor)
				{
					try
					{
						dbConnFreeMonitor.wait();
					} catch (InterruptedException e)
					{
						e.printStackTrace();
					}
				}
			}
		}
		return dbConFree;
	}
	
	private void returnAFreeConn(Connection retConn)
	{
		synchronized(dbConnFreeMonitor)
		{
			freedbConnQueue.add(retConn);
			dbConnFreeMonitor.notifyAll();
		}
	}
	
	/**
	 * creates tables needed for the database.
	 * @throws SQLException
	 */
	private void createTables() throws SQLException
	{
		/*Connection myConn = getAFreeConnection();
		Statement stmt = (Statement) myConn.createStatement();
		
		for( int i=0; i<ContextServiceConfig.NUM_ATTRIBUTES; i++ )
		{
			String attName   = ContextServiceConfig.CONTEXT_ATTR_PREFIX+i;
			String tableName = attName+ContextServiceConfig.MetadataTableSuffix;
			
			String newTableCommand = "create table "+tableName+" ( "
				      + "   lowerRange DOUBLE NOT NULL, upperRange DOUBLE NOT NULL, nodeID INT NOT NULL, "
				      + "   partitionNum INT NOT NULL AUTO_INCREMENT , INDEX USING BTREE (lowerRange), "
			 	      + "   INDEX USING BTREE (upperRange), PRIMARY KEY (partitionNum) )";
			
			stmt.executeUpdate(newTableCommand);
			
			tableName = attName+ContextServiceConfig.ValueTableSuffix;
			
			// char 45 for GUID because, GUID is 40 char in length, 5 just additional
			newTableCommand = "create table "+tableName+" ( "
				      + "   value DOUBLE NOT NULL, nodeGUID CHAR(100) PRIMARY KEY, versionNum INT NOT NULL,"
				      + " INDEX USING BTREE (value) )";
			
			stmt.executeUpdate(newTableCommand);
			
			for( int j=0;j<ContextServiceConfig.NUM_RANGE_PARTITIONS;j++ )
			{
				tableName       = attName+"GroupGUIDTable"+j;
				newTableCommand = "create table "+tableName+" ( "
				      + "   groupGUID CHAR(100) PRIMARY KEY, groupQuery VARCHAR(100) )";
				
				stmt.executeUpdate(newTableCommand);
			}
		}
		
		// creating full GUID storage table.
		String tableName = "fullObjectTable";
		
		String newTableCommand = "create table "+tableName+" ( "
			      + "   nodeGUID CHAR(100) PRIMARY KEY";
		
		//	      + ", upperRange DOUBLE NOT NULL, nodeID INT NOT NULL, "
		//	      + "   partitionNum INT AUTO_INCREMENT, INDEX USING BTREE (lowerRange, upperRange) )";
		for(int i=0; i<ContextServiceConfig.NUM_ATTRIBUTES; i++)
		{
			newTableCommand = newTableCommand + ", contextATT"+i+" DOUBLE DEFAULT "+AttributeTypes.NOT_SET+" , INDEX USING BTREE(contextATT"+i+")";
		}
		newTableCommand = newTableCommand +" )";
		stmt.executeUpdate(newTableCommand);*/
		
		
		// groupGUID storage table
		/*tableName       = ContextServiceConfig.groupGUIDTable;
		newTableCommand = "create table "+tableName+" ( "
			      + " nodeGUID CHAR(100) NOT NULL, groupGUID CHAR(100) NOT NULL, groupQuery VARCHAR(100) NOT NULL, "
			      + " INDEX USING HASH(nodeGUID), INDEX USING HASH(groupGUID) )";
		stmt.executeUpdate(newTableCommand);*/
		
//		stmt.close();
//		this.returnAFreeConn(myConn);
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
		//ContextServiceLogger.getLogger().fine("getAttributeMetaObjectRecord attrName "+attrName+" queryMin "
		//				+queryMin+" queryMax "+queryMax);
		
		Connection myConn = this.getAFreeConnection();
		//ContextServiceLogger.getLogger().fine("getAttributeMetaObjectRecord gotTheConnection ");
		
		
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
			
			
			Statement stmt = (Statement) myConn.createStatement();
			
			ResultSet rs = stmt.executeQuery(selectTableSQL);
			
		    while( rs.next() )
		    {
		    	//Retrieve by column name
		    	int nodeID  	 = rs.getInt("nodeID");
		    	int partitionNum = rs.getInt("partitionNum");
		    	
		    	MetadataTableInfo<Integer> metaobj = new MetadataTableInfo<Integer>(nodeID, partitionNum);
		    	answerList.add(metaobj);
		    }
		    
		    //ContextServiceLogger.getLogger().fine("getAttributeMetaObjectRecord queryExecuted");
		    
		    rs.close();
		    stmt.close();
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		this.returnAFreeConn(myConn);
		//ContextServiceLogger.getLogger().fine("getAttributeMetaObjectRecord greturnedConnection "+answerList.size()+" "+answerList);
		return answerList;
	}
	
	public JSONArray getValueInfoObjectRecord
							(String attrName, double queryMin, double queryMax)
	{
		Connection myConn  = this.getAFreeConnection();
		JSONArray jsoArray = new JSONArray();
		
		try
		{
			String tableName = attrName+ContextServiceConfig.ValueTableSuffix;
			
			String selectTableSQL = "SELECT value, nodeGUID from "+tableName+" WHERE "
					+ "( value >= "+queryMin +" AND value < "+queryMax+" )";
			
			Statement stmt = (Statement) myConn.createStatement();
			
			ResultSet rs = stmt.executeQuery(selectTableSQL);
			
			while( rs.next() )
			{
				//Retrieve by column name
				//double value  	 = rs.getDouble("value");
				String nodeGUID = rs.getString("nodeGUID");
				
				jsoArray.put(nodeGUID);
			}
		
			rs.close();
			stmt.close();
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		this.returnAFreeConn(myConn);
		return jsoArray;
	}
	
	
	/*public JSONArray getValueInfoObjectRecordCreateTable
		(String attrName, double queryMin, double queryMax)
	{
		Connection myConn  = this.getAFreeConnection();
		JSONArray jsoArray = new JSONArray();

		try
		{
			String tableName = attrName+ContextServiceConfig.ValueTableSuffix;
			String selectTableSQL = "SELECT value, nodeGUID from "+tableName+" WHERE "
					+ "( value >= "+queryMin +" AND value < "+queryMax+" ) LIMIT 10";
			
			Statement stmt = (Statement) myConn.createStatement();
			
			ResultSet rs = stmt.executeQuery(selectTableSQL);
			
			while( rs.next() )
			{
				//Retrieve by column name
				//double value  	 = rs.getDouble("value");
				String nodeGUID = rs.getString("nodeGUID");

				jsoArray.put(nodeGUID);
			}
			
			rs.close();
			stmt.close();
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		this.returnAFreeConn(myConn);
		return jsoArray;
	}*/
	
	
	public void putAttributeMetaObjectRecord
							(String attrName, double lowerRange, double upperRange, Integer nodeID)
	{
		Connection myConn = this.getAFreeConnection();
		Statement statement = null;

		String tableName = attrName+ContextServiceConfig.MetadataTableSuffix;
		String insertTableSQL = "INSERT INTO "+tableName 
				+" (lowerRange, upperRange, nodeID) " + "VALUES"
				+ "("+lowerRange+","+upperRange+","+nodeID +")";
		
		try 
		{
			statement = (Statement) myConn.createStatement();

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
		this.returnAFreeConn(myConn);
	}
	
	
	public void putValueObjectRecord(String attrName, double value, String nodeGUID, 
			long versionNum)
	{
		Connection myConn = this.getAFreeConnection();
		Statement statement = null;

		String tableName = attrName+ContextServiceConfig.ValueTableSuffix;
		String insertTableSQL = "INSERT INTO "+tableName 
				+" (value, nodeGUID, versionNum) " + "VALUES"
				+ "("+value+",'"+nodeGUID+"',"+versionNum +")";

		try 
		{
			statement = (Statement) myConn.createStatement();

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
		this.returnAFreeConn(myConn);
	}
	
	// updates valueInfoObjectRecord
	public void updateValueInfoObjectRecord(String attrName, 
				ValueTableInfo.Operations operType, String nodeGUID, double value, long versionNum)
	{
		Connection myConn = this.getAFreeConnection();
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
			statement = (Statement) myConn.createStatement();

			// execute insert SQL stetement
			int numRows = statement.executeUpdate(sqlQuery);
			ContextServiceLogger.getLogger().fine("Num rows affected "+numRows);
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
		this.returnAFreeConn(myConn);
	}
	
	// store in fulldataObjTable
	@SuppressWarnings("unchecked")
	public JSONObject storeInFullDataObjTable(String nodeGUID, JSONObject attrValuePairs) throws JSONException
	{
		Connection myConn = this.getAFreeConnection();
		Statement statement = null;

		String tableName = "fullObjectTable";
		
		//String sqlQuery = "UPDATE "+tableName
		//		+ " SET "+attrName+" = "+value + " WHERE nodeGUID = '"+nodeGUID+"'";
		//INSERT INTO table (a,b,c) VALUES (1,2,3)
		// ON DUPLICATE KEY UPDATE c=c+1;
		
		String selectQuery = "SELECT ";
		//+attrName+" FROM "+tableName+" WHERE nodeGUID = '"+nodeGUID+"'";
		
		String updateSqlQuery = "UPDATE "+tableName
				+ " SET ";
				//+ "value = "+value+" , versionNum = "+versionNum
				//+ " WHERE nodeGUID = '"+nodeGUID+"'";
		
		String insertQuery = "INSERT INTO "+tableName+ " (";
		//String sqlQuery = "INSERT INTO "+tableName 
		//				+" ("+attrName+", nodeGUID) " + "VALUES"
		//		+ "("+value+",'"+nodeGUID+"' ) ON DUPLICATE KEY UPDATE "+attrName+" = "+value;
		
		JSONObject oldValueJSON = new JSONObject();
		
		@SuppressWarnings("unchecked")
		Iterator<String> jsoObjKeysIter = attrValuePairs.keys();
		int i=0;
		while( jsoObjKeysIter.hasNext() )
		{
			String attrName = jsoObjKeysIter.next();
			double newVal = attrValuePairs.getDouble(attrName);
			
			//oldValueJSON.put(attrName, AttributeTypes.NOT_SET);
			
			if(i == 0)
			{
				selectQuery = selectQuery + attrName;
				updateSqlQuery = updateSqlQuery + attrName +" = "+newVal;
				insertQuery = insertQuery + attrName;
			}
			else
			{
				selectQuery = selectQuery + ", "+attrName+" ";
				updateSqlQuery = updateSqlQuery +" , "+ attrName +" = "+newVal;
				insertQuery = insertQuery +", "+attrName;
			}
			i++;
		}
		
		selectQuery = selectQuery + " FROM "+tableName+" WHERE nodeGUID = '"+nodeGUID+"'";
		updateSqlQuery = updateSqlQuery + " WHERE nodeGUID = '"+nodeGUID+"'";
		insertQuery = insertQuery + ", nodeGUID) " + "VALUES"+ "(";
				//+ ",'"+nodeGUID+"' )
		//double oldValue = Double.MIN_VALUE;
		boolean foundARow = false;
		
		try
		{
			statement = (Statement) myConn.createStatement();
			ResultSet rs = statement.executeQuery(selectQuery);
			
			while( rs.next() )
			{
				//Retrieve by column name
				//double value  	 = rs.getDouble("value");
				//oldValue = rs.getDouble(attrName);
				
				jsoObjKeysIter = attrValuePairs.keys();
				while( jsoObjKeysIter.hasNext() )
				{
					String attrName = jsoObjKeysIter.next();
					double oldValueForAttr = rs.getDouble(attrName);
					
					oldValueJSON.put(attrName, oldValueForAttr);
				}
				foundARow = true;
			}
			rs.close();
			
			// then update
			if(foundARow)
			{
				statement.executeUpdate(updateSqlQuery);
			}
			else
			{// otherwise insert and update on duplication
				try
				{
					i = 0;
					//try insert, if fails then update
					jsoObjKeysIter = attrValuePairs.keys();
					while( jsoObjKeysIter.hasNext() )
					{
						String attrName = jsoObjKeysIter.next();
						double newValue = attrValuePairs.getDouble(attrName);
						
						if(i == 0)
						{
							insertQuery = insertQuery + newValue;
						}
						else
						{
							insertQuery = insertQuery +", "+newValue;
						}
						i++;
					}
					insertQuery = insertQuery +", '"+nodeGUID+"')";
					statement.executeUpdate(insertQuery);
				}catch(SQLException e)
				{
					ContextServiceLogger.getLogger().fine("Insert failed, duplicate key, doing update");
					statement.executeUpdate(updateSqlQuery);
				}
			}
			// execute insert SQL stetement
			//statement.executeUpdate(sqlQuery);
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
		
		this.returnAFreeConn(myConn);
		return oldValueJSON;
	}
	
	public JSONArray checkGUIDsForQuery(String query, JSONArray guidsToCheck)
	{
		Vector<QueryComponent> qComponents = QueryParser.parseQueryNew(query);
		String mysqlQuery = "SELECT nodeGUID from fullObjectTable WHERE ( ";
		
		
		for(int i=0; i<guidsToCheck.length(); i++)
		{
			try
			{
				if(i == (guidsToCheck.length()-1) )
				{
					mysqlQuery = mysqlQuery +"nodeGUID = '"+guidsToCheck.getString(i)+"' ) ";
				}
				else
				{
					mysqlQuery = mysqlQuery +"nodeGUID = '"+guidsToCheck.getString(i)+"' OR ";
				}
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		mysqlQuery = mysqlQuery + " AND ( ";
		
		/*for(int i=0;i<qComponents.size();i++)
		{
			QueryComponent qc = qComponents.get(i);
			
			if(i == (qComponents.size()-1) )
			{
				mysqlQuery = mysqlQuery + " ( "+qc.getAttributeName() +" >= "+qc.getLeftValue() +" AND " 
						+qc.getAttributeName() +" <= "+qc.getRightValue()+" ) )";
			}
			else
			{
				mysqlQuery = mysqlQuery + " ( "+qc.getAttributeName() +" >= "+qc.getLeftValue() +" AND " 
						+qc.getAttributeName() +" <= "+qc.getRightValue()+" ) AND ";
			}
		}*/
		
		
		Connection myConn = this.getAFreeConnection();
		JSONArray jsoArray = new JSONArray();
		
		try
		{
			Statement stmt = (Statement) myConn.createStatement();
			
			ResultSet rs = stmt.executeQuery(mysqlQuery);
			
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
		this.returnAFreeConn(myConn);
		return jsoArray;
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
					
			this.sqlNodeInfoMap.put((Integer)readNodeId, sqlNodeInfo);
			
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