package edu.umass.cs.contextservice.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.hash.Hashing;

import edu.umass.cs.contextservice.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.hyperspace.storage.DomainPartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.processing.QueryComponent;
import edu.umass.cs.contextservice.processing.QueryParser;
import edu.umass.cs.utils.DelayProfiler;

public class HyperspaceMySQLDB<NodeIDType>
{
	//private final Connection dbConnection;
	private final NodeIDType myNodeID;
	//private final HashMap<NodeIDType, SQLNodeInfo> sqlNodeInfoMap;
	private final DataSource<NodeIDType> mysqlDataSource;
	
	private final HashMap<Integer, SubspaceInfo<NodeIDType>> subspaceInfo;
	
	//public static final int NUM_PARALLEL_CLIENTS					= 100;
	//private Connection[] dbConnectionArray						= new Connection[NUM_PARALLEL_CLIENTS];
	//private final ConcurrentLinkedQueue<Connection> freedbConnQueue;
	//private final Object dbConnFreeMonitor						= new Object();
	//private final Object storeFullObjMonitor						= new Object();
	
	public HyperspaceMySQLDB(NodeIDType myNodeID, HashMap<Integer, SubspaceInfo<NodeIDType>> subspaceInfo)
			throws Exception
	{
		this.myNodeID = myNodeID;
		//sqlNodeInfoMap = new HashMap<NodeIDType, SQLNodeInfo>();
		//readDBNodeSetup();
		this.mysqlDataSource = new DataSource<NodeIDType>(myNodeID);
		this.subspaceInfo = subspaceInfo;
		
		/*freedbConnQueue = new ConcurrentLinkedQueue<Connection>();	
		for(int i=0;i<NUM_PARALLEL_CLIENTS;i++)
		{
			dbConnectionArray[i] = getConnection();
			freedbConnQueue.add(dbConnectionArray[i]);
		}*/
		// create necessary tables
		createTables();
	}
	
	/**
	 * creates tables needed for the database.
	 * @throws SQLException
	 */
	private void createTables()
	{
		Connection myConn = null;
		Statement stmt    = null;
		
		try
		{
			myConn = this.mysqlDataSource.getConnection();
			stmt =  myConn.createStatement();
			
			for(int i=0;i<subspaceInfo.size();i++)
			{
				SubspaceInfo<NodeIDType> subInfo = subspaceInfo.get(i);
				int subspaceNum = subInfo.getSubspaceNum();
				HashMap<String, Boolean> subspaceAttributes = subInfo.getAttributesOfSubspace();
				
				// partition info storage info
				String tableName = "subspace"+subspaceNum+"PartitionInfo";
				
				String newTableCommand = "create table "+tableName+" ( hashCode INTEGER PRIMARY KEY , "
					      + "   respNodeID INTEGER ";
				
				//	      + ", upperRange DOUBLE NOT NULL, nodeID INT NOT NULL, "
				//	      + "   partitionNum INT AUTO_INCREMENT, INDEX USING BTREE (lowerRange, upperRange) )";
				//FIXME: which indexing scheme is better, indexing two attribute once or creating a index over all 
				// attributes
				Iterator<String> attrIter = subspaceAttributes.keySet().iterator();
				while( attrIter.hasNext() )
				{
					String attrName = attrIter.next();
					
					// lower range of this attribute in this subspace
					String lowerAttrName = "lower"+attrName;
					String upperAttrName = "upper"+attrName;
					
					newTableCommand = newTableCommand + " , "+lowerAttrName+" DOUBLE , "+upperAttrName+" DOUBLE , "
							+ "INDEX USING BTREE("+lowerAttrName+" , "+upperAttrName+")";
				}
				
				newTableCommand = newTableCommand +" )";
				stmt.executeUpdate(newTableCommand);
				
				//FIXME: which indexing scheme is better, indexing two attribute once or creating a index over all 
				// attributes
				// datastorage table of each subspace
				tableName = "subspace"+subspaceNum+"DataStorage";
				
				newTableCommand = "create table "+tableName+" ( "
					      + "   nodeGUID CHAR(100) PRIMARY KEY";
				
				//	      + ", upperRange DOUBLE NOT NULL, nodeID INT NOT NULL, "
				//	      + "   partitionNum INT AUTO_INCREMENT, INDEX USING BTREE (lowerRange, upperRange) )";
				for(int k=0; k<ContextServiceConfig.NUM_ATTRIBUTES; k++)
				{
					newTableCommand = newTableCommand + ", contextATT"+k+" DOUBLE DEFAULT "+AttributeTypes.NOT_SET
							+" , INDEX USING BTREE(contextATT"+k+")";
				}
				newTableCommand = newTableCommand +" )";
				stmt.executeUpdate(newTableCommand);
			}
			
			String tableName = "primarySubspaceDataStorage";
			
			String newTableCommand = "create table "+tableName+" ( "
				      + "   nodeGUID CHAR(100) PRIMARY KEY";
			
			//	      + ", upperRange DOUBLE NOT NULL, nodeID INT NOT NULL, "
			//	      + "   partitionNum INT AUTO_INCREMENT, INDEX USING BTREE (lowerRange, upperRange) )";
			for(int k=0; k<ContextServiceConfig.NUM_ATTRIBUTES; k++)
			{
				newTableCommand = newTableCommand + ", contextATT"+k+" DOUBLE DEFAULT "
									+AttributeTypes.NOT_SET+" , INDEX USING BTREE(contextATT"+k+")";
			}
			newTableCommand = newTableCommand +" )";
			stmt.executeUpdate(newTableCommand);
		} catch(SQLException mysqlEx)
		{
			mysqlEx.printStackTrace();
		} finally
		{
			try
			{
				if( stmt != null )
					stmt.close();
				if( myConn != null )
					myConn.close();
			} catch(SQLException sqex)
			{
				sqex.printStackTrace();
			}
		}
	}
	
	/**
	 * Returns a list of regions/nodes that overlap with a query in a given subspace.
	 * @param subspaceNum
	 * @param qcomponents, takes matching attributes as input
	 * @return
	 */
	public HashMap<Integer, JSONArray> 
		getOverlappingRegionsInSubspace(int subspaceNum, Vector<QueryComponent> matchingQueryComponents)
	{
		long t0 = System.currentTimeMillis();
		HashMap<Integer, JSONArray> answerList 
						= new HashMap<Integer, JSONArray>();
		
		String tableName = "subspace"+subspaceNum+"PartitionInfo";
		
		String selectTableSQL = "SELECT respNodeID from "+tableName+" WHERE ";
		
		for( int i=0; i<matchingQueryComponents.size(); i++ )
		{
			QueryComponent qcomponent = matchingQueryComponents.get(i);
			String attrName = qcomponent.getAttributeName();
			
			String lowerAttr = "lower"+attrName;
			String upperAttr = "upper"+attrName;
			double queryMin  = qcomponent.getLeftValue();
			double queryMax  = qcomponent.getRightValue();
			
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
			
			selectTableSQL = selectTableSQL +" ( "
					+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
					+ "( "+lowerAttr+" < "+queryMax +" AND "+upperAttr+" >= "+queryMax+" ) OR "
					+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" < "+queryMax+" ) "+" ) ";
			if( i != (matchingQueryComponents.size()-1) )
			{
				selectTableSQL = selectTableSQL + " AND ";
			}
		}
		
		Statement stmt 		= null;
		Connection myConn 	= null;
		try
		{
			myConn = this.mysqlDataSource.getConnection();
			stmt = myConn.createStatement();
			System.out.println("selectTableSQL "+selectTableSQL);
			ResultSet rs = stmt.executeQuery(selectTableSQL);
		    while( rs.next() )
		    {
		    	//Retrieve by column name
		    	int respNodeID  	 = rs.getInt("respNodeID");
		    	answerList.put(respNodeID, null);
		    	//MetadataTableInfo<Integer> metaobj = new MetadataTableInfo<Integer>(nodeID, partitionNum);
		    	//answerList.add( metaobj );
		    }
		    rs.close();
		} catch( SQLException sqlex )
		{
			sqlex.printStackTrace();
		}
		finally
		{
			try
			{
				if( stmt != null )
					stmt.close();
				
				if( myConn != null )
					myConn.close();
			}
			catch(SQLException sqlex)
			{
				sqlex.printStackTrace();
			}
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON)
		{
			DelayProfiler.updateDelay("getOverlappingRegionsInSubspace", t0);
		}
		return answerList;
	}
	
	public JSONArray processSearchQueryInSubspaceRegion(int subspaceNum, String query)
	{
		long t0 = System.currentTimeMillis();
		
		Vector<QueryComponent> qComponents = QueryParser.parseQuery(query);
		
		String tableName = "subspace"+subspaceNum+"DataStorage";
		
		String mysqlQuery = "SELECT nodeGUID from "+tableName+" WHERE ( ";
		
		/*for(int i=0; i<guidsToCheck.length(); i++)
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
		mysqlQuery = mysqlQuery + " AND ( ";*/
		
		for(int i=0;i<qComponents.size();i++)
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
		}
		
		Connection myConn  = null;
		Statement stmt     = null;
		JSONArray jsoArray = new JSONArray();
		
		try
		{
			myConn = this.mysqlDataSource.getConnection();
			stmt   = myConn.createStatement();
			
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
		finally
		{
			try
			{
				if( stmt != null )
					stmt.close();
				if( myConn != null )
					myConn.close();
			} catch(SQLException sqlex)
			{
				sqlex.printStackTrace();
			}
		}
		if(ContextServiceConfig.DELAY_PROFILER_ON)
		{
			DelayProfiler.updateDelay("processSearchQueryInSubspaceRegion", t0);
		}
		return jsoArray;
	}
	
	/**
	 * inserts a subspace region denoted by subspace vector, integer denotes partition num in partition
	 * info 
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoSubspacePartitionInfo(int subspaceNum, List<Integer> subspaceVector, NodeIDType respNodeId)
	{
		long t0 = System.currentTimeMillis();
		Connection myConn   = null;
		Statement stmt      = null;
		
		String tableName = "subspace"+subspaceNum+"PartitionInfo";
		
		SubspaceInfo<NodeIDType> currSubInfo = subspaceInfo.get(subspaceNum);
		Vector<DomainPartitionInfo> domainPartInfo = currSubInfo.getDomainPartitionInfo();
		//Vector<String> attrSubspaceInfo = currSubInfo.getAttributesOfSubspace();
		HashMap<String, Boolean> attrSubspaceInfo = currSubInfo.getAttributesOfSubspace();
		
		
		//int numNodes = currSubInfo.getNodesOfSubspace().size();
		
		//int mapIndex = Hashing.consistentHash(subspaceVector.hashCode(), numNodes);
		
		//NodeIDType respNodeId = currSubInfo.getNodesOfSubspace().get(mapIndex);
		
		
		String insertTableSQL = "INSERT INTO "+tableName 
				+" ( hashCode, respNodeID ";
				//+ "nodeID) " + "VALUES"
				//+ "("+lowerRange+","+upperRange+","+nodeID +")";
		
		Iterator<String> attrIter = attrSubspaceInfo.keySet().iterator();
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			
			String lowerAtt = "lower"+attrName;
			String upperAtt = "upper"+attrName;
			
			insertTableSQL = insertTableSQL + ", "+lowerAtt+" , "+upperAtt;
		}
		
		insertTableSQL = insertTableSQL + " ) VALUES ( "+subspaceVector.hashCode() + 
				" , "+respNodeId;
		
		for( int i=0; i<subspaceVector.size(); i++ )
		{
			DomainPartitionInfo currPartitionInfo = domainPartInfo.get(subspaceVector.get(i));
			insertTableSQL = insertTableSQL + " , "+currPartitionInfo.getLowerbound()+" , "+ 
					currPartitionInfo.getUpperbound();
		}
		insertTableSQL = insertTableSQL + " ) ";
		
		try
		{
			myConn = this.mysqlDataSource.getConnection();
			stmt = myConn.createStatement();

			// execute insert SQL stetement
			stmt.executeUpdate(insertTableSQL);
			
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		}
		finally
		{
			try
			{
				if( myConn != null )
				{
					myConn.close();
				}
				if( stmt != null )
				{
					stmt.close();
				}
			} catch(SQLException sqex)
			{
				sqex.printStackTrace();
			}
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON)
		{
			DelayProfiler.updateDelay("insertIntoSubspacePartitionInfo", t0);
		}
	}
	
	/**
	 * stores GUID in a subspace. The decision to store a guid on this node
	 * in this subspace is not made in this fucntion.
	 * @param subspaceNum
	 * @param nodeGUID
	 * @param attrValuePairs
	 * @return
	 * @throws JSONException
	 */
	@SuppressWarnings("unchecked")
	public JSONObject storeGUIDInSubspace(String tableName, String nodeGUID, JSONObject attrValuePairs) throws JSONException
	{
		long t0 = System.currentTimeMillis();
		Connection myConn 	= null;
		Statement stmt 		= null;
		
		//String tableName 	= "subspace"+subspaceNum+"DataStorage";

		//String tableName 	= "fullObjectTable";
		
		//String sqlQuery = "UPDATE "+tableName
		//		+ " SET "+attrName+" = "+value + " WHERE nodeGUID = '"+nodeGUID+"'";
		//INSERT INTO table (a,b,c) VALUES (1,2,3)
		// ON DUPLICATE KEY UPDATE c=c+1;
		
		String selectQuery 		= "SELECT * ";
		//+attrName+" FROM "+tableName+" WHERE nodeGUID = '"+nodeGUID+"'";
		
		String updateSqlQuery 	= "UPDATE "+tableName
				+ " SET ";
				//+ "value = "+value+" , versionNum = "+versionNum
				//+ " WHERE nodeGUID = '"+nodeGUID+"'";
		
		String insertQuery 		= "INSERT INTO "+tableName+ " (";
		//String sqlQuery = "INSERT INTO "+tableName 
		//				+" ("+attrName+", nodeGUID) " + "VALUES"
		//		+ "("+value+",'"+nodeGUID+"' ) ON DUPLICATE KEY UPDATE "+attrName+" = "+value;
		
		JSONObject oldValueJSON = new JSONObject();
		
		
		Iterator<String> jsoObjKeysIter = attrValuePairs.keys();
		int i=0;
		while( jsoObjKeysIter.hasNext() )
		{
			String attrName = jsoObjKeysIter.next();
			double newVal = attrValuePairs.getDouble(attrName);
			
			oldValueJSON.put(attrName, AttributeTypes.NOT_SET);
			
			if(i == 0)
			{
				//selectQuery = selectQuery + attrName;
				updateSqlQuery = updateSqlQuery + attrName +" = "+newVal;
				insertQuery = insertQuery + attrName;
			}
			else
			{
				//selectQuery = selectQuery + ", "+attrName+" ";
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
			myConn = this.mysqlDataSource.getConnection();
			stmt = myConn.createStatement();
			ResultSet rs = stmt.executeQuery(selectQuery);
			
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
				stmt.executeUpdate(updateSqlQuery);
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
					stmt.executeUpdate(insertQuery);
				}catch(SQLException e)
				{
					System.out.println("Insert failed, duplicate key, doing update");
					stmt.executeUpdate(updateSqlQuery);
				}
			}
			// execute insert SQL stetement
			//statement.executeUpdate(sqlQuery);
		} catch (SQLException e)
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if (stmt != null)
					stmt.close();
				if (myConn != null)
					myConn.close();
			}
			catch(SQLException e)
			{
				e.printStackTrace();
			}
		}
		
		//attributes which are not set should be set to default value
		// for subspace hashing
		if( oldValueJSON.length() != ContextServiceConfig.NUM_ATTRIBUTES )
		{
			for(int j=0; j<ContextServiceConfig.NUM_ATTRIBUTES; j++)
			{
				if( !oldValueJSON.has(ContextServiceConfig.CONTEXT_ATTR_PREFIX+j) )
				{
					oldValueJSON.put(ContextServiceConfig.CONTEXT_ATTR_PREFIX+j, AttributeTypes.NOT_SET);
				}
			}
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON)
		{
			DelayProfiler.updateDelay("storeGUIDInSubspace", t0);
		}
		
		return oldValueJSON;
	}
	
	public void deleteGUIDFromSubspaceRegion(String tableName, String nodeGUID)
	{
		long t0 = System.currentTimeMillis();
		String deleteCommand = "DELETE FROM "+tableName+" WHERE nodeGUID='"+nodeGUID+"'";
		Connection myConn 	= null;
		Statement stmt 		= null;
		
		try
		{
			myConn = this.mysqlDataSource.getConnection();
			stmt = myConn.createStatement();
			stmt.executeUpdate(deleteCommand);
		} catch(SQLException sqex)
		{
			sqex.printStackTrace();
		}
		finally
		{
			try
			{
				if(myConn != null)
				{
					myConn.close();
				}
				if(	stmt != null )
				{
					stmt.close();
				}
			} catch(SQLException sqex)
			{
				sqex.printStackTrace();
			}
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON)
		{
			DelayProfiler.updateDelay("deleteGUIDFromSubspaceRegion", t0);
		}
	}
	
	public JSONObject getGUIDRecordFromPrimarySubspace(String tableName, String GUID)
	{
		long t0 = System.currentTimeMillis();
		Connection myConn 	= null;
		Statement stmt 		= null;
		
		String selectQuery 		= "SELECT * ";
		
		JSONObject oldValueJSON = new JSONObject();
		
		selectQuery = selectQuery + " FROM "+tableName+" WHERE nodeGUID = '"+GUID+"'";
		
		try
		{
			myConn = this.mysqlDataSource.getConnection();
			stmt = myConn.createStatement();
			ResultSet rs = stmt.executeQuery(selectQuery);
			
			while( rs.next() )
			{
				for(int j=0; j<ContextServiceConfig.NUM_ATTRIBUTES; j++)
				{
					String attrName = ContextServiceConfig.CONTEXT_ATTR_PREFIX+j;
					double oldValueForAttr = rs.getDouble(attrName);
					
					try 
					{
						oldValueJSON.put(attrName, oldValueForAttr);
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}
				}
			}
			System.out.println("NodeId "+this.myNodeID+" getGUIDRecordFromPrimarySubspace guid "+GUID+" oldValueJSON size "+oldValueJSON.length()+"oldValueJSON "+oldValueJSON);
			rs.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		} finally
		{
			try
			{
				if (stmt != null)
					stmt.close();
				if (myConn != null)
					myConn.close();
			}
			catch(SQLException e)
			{
				e.printStackTrace();
			}
		}
		
		if(ContextServiceConfig.DELAY_PROFILER_ON)
		{
			DelayProfiler.updateDelay("getGUIDRecordFromPrimarySubspace", t0);
		}
		
		return oldValueJSON;
	}
}