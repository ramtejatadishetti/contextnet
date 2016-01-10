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

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes.DomainPartitionInfo;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.utils.DelayProfiler;

public class HyperspaceMySQLDB<NodeIDType>
{
	public static final int UPDATE_REC 								= 1;
	public static final int INSERT_REC 								= 2;
	
	// maximum query length of 1000bytes
	public static final int MAX_QUERY_LENGTH						= 1000;
	
	
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
	
	//TriggerInfo table columns
	//groupGUID VARCHAR(100) , userIP VARCHAR(20) , "+ "userPort INTEGER
	public static final String userQuery = "userQuery";
	public static final String groupGUID = "groupGUID";
	public static final String userIP = "userIP";
	public static final String userPort = "userPort";
	
	
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
	 * Creates tables needed for the database.
	 * @throws SQLException
	 */
	private void createTables()
	{
		Connection myConn  = null;
		Statement  stmt    = null;
		
		try
		{
			myConn = this.mysqlDataSource.getConnection();
			stmt   =  myConn.createStatement();
			
			for( int i=0;i<subspaceInfo.size();i++ )
			{
				SubspaceInfo<NodeIDType> subInfo = subspaceInfo.get(i);
				int subspaceNum = subInfo.getSubspaceNum();
				HashMap<String, AttributePartitionInfo> subspaceAttributes = subInfo.getAttributesOfSubspace();
				
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
					String attrDataType = subspaceAttributes.get(attrName).getAttrMetaInfo().getDataType();
					String mySQLDataType = AttributeTypes.mySQLDataType.get(attrDataType);
					// lower range of this attribute in this subspace
					String lowerAttrName = "lower"+attrName;
					String upperAttrName = "upper"+attrName;
					
					newTableCommand = newTableCommand + " , "+lowerAttrName+" "+mySQLDataType+" , "+upperAttrName+" "+mySQLDataType+" , "
							+ "INDEX USING BTREE("+lowerAttrName+" , "+upperAttrName+")";
				}
				
				newTableCommand = newTableCommand +" )";
				//ContextServiceLogger.getLogger().fine("newTableCommand "+newTableCommand);
				stmt.executeUpdate(newTableCommand);
				
				//FIXME: which indexing scheme is better, indexing two attribute once or creating a index over all 
				// attributes
				// datastorage table of each subspace
				tableName = "subspace"+subspaceNum+"DataStorage";
				
				newTableCommand = "create table "+tableName+" ( "
					      + "   nodeGUID CHAR(100) PRIMARY KEY";
				
				//	      + ", upperRange DOUBLE NOT NULL, nodeID INT NOT NULL, "
				//	      + "   partitionNum INT AUTO_INCREMENT, INDEX USING BTREE (lowerRange, upperRange) )";
				attrIter = AttributeTypes.attributeMap.keySet().iterator();
				while(attrIter.hasNext())
				{
					String attrName = attrIter.next();
					AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
					String dataType = attrMetaInfo.getDataType();
					String defaultVal = attrMetaInfo.getDefaultValue();
					String mySQLDataType = AttributeTypes.mySQLDataType.get(dataType);
					newTableCommand = newTableCommand + ", "+attrName+" "+mySQLDataType+" DEFAULT "+AttributeTypes.convertStringToDataTypeForMySQL(defaultVal, dataType)
							+" , INDEX USING BTREE("+attrName+")";
				}
				newTableCommand = newTableCommand +" )";
				stmt.executeUpdate(newTableCommand);
				
				
				if( ContextServiceConfig.TRIGGER_ENABLED )
				{
					// creating trigger guid storage
					tableName = "subspace"+subspaceNum+"TriggerInfo";
					
					newTableCommand = "create table "+tableName+" ( hashCode INTEGER , " + 
					"  userQuery VARCHAR("+HyperspaceMySQLDB.MAX_QUERY_LENGTH+") , groupGUID VARCHAR(100) , userIP VARCHAR(20) , "
							+ "userPort INTEGER , INDEX USING  HASH(hashCode) )";
					
					stmt.executeUpdate(newTableCommand);
				}
			}
			
			String tableName = "primarySubspaceDataStorage";
			
			String newTableCommand = "create table "+tableName+" ( "
				      + "   nodeGUID CHAR(100) PRIMARY KEY";
			
			//	      + ", upperRange DOUBLE NOT NULL, nodeID INT NOT NULL, "
			//	      + "   partitionNum INT AUTO_INCREMENT, INDEX USING BTREE (lowerRange, upperRange) )";
//			for(int k=0; k<ContextServiceConfig.NUM_ATTRIBUTES; k++)
//			{
//				newTableCommand = newTableCommand + ", contextATT"+k+" DOUBLE DEFAULT "
//									+AttributeTypes.NOT_SET+" , INDEX USING BTREE(contextATT"+k+")";
//			}
			
			Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
			while(attrIter.hasNext())
			{
				String attrName = attrIter.next();
				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
				String dataType = attrMetaInfo.getDataType();
				String defaultVal = attrMetaInfo.getDefaultValue();
				String mySQLDataType = AttributeTypes.mySQLDataType.get(dataType);
				newTableCommand = newTableCommand + ", "+attrName+" "+mySQLDataType+" DEFAULT "+AttributeTypes.convertStringToDataTypeForMySQL(defaultVal, dataType)
						+" , INDEX USING BTREE("+attrName+")";
			}
			
			newTableCommand = newTableCommand +" )";
			stmt.executeUpdate(newTableCommand);
		} catch( SQLException mysqlEx )
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
	public HashMap<Integer, OverlappingInfoClass> 
		getOverlappingRegionsInSubspace(int subspaceNum, Vector<ProcessingQueryComponent> matchingQueryComponents)
	{
		long t0 = System.currentTimeMillis();
		HashMap<Integer, OverlappingInfoClass> answerList 
						= new HashMap<Integer, OverlappingInfoClass>();
		
		String tableName = "subspace"+subspaceNum+"PartitionInfo";
		
		String selectTableSQL = "SELECT hashCode, respNodeID from "+tableName+" WHERE ";
		
		for( int i=0; i<matchingQueryComponents.size(); i++ )
		{
			ProcessingQueryComponent qcomponent = matchingQueryComponents.get(i);
			String attrName = qcomponent.getAttributeName();
			
			String lowerAttr = "lower"+attrName;
			String upperAttr = "upper"+attrName;
			
			AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
			String dataType = attrMetaInfo.getDataType();
			
			
			String queryMin  =  AttributeTypes.convertStringToDataTypeForMySQL(qcomponent.getLowerBound(), dataType) + "";
			String queryMax  =  AttributeTypes.convertStringToDataTypeForMySQL(qcomponent.getUpperBound(), dataType) + "";
			
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
			ContextServiceLogger.getLogger().fine("selectTableSQL "+selectTableSQL);
			ResultSet rs = stmt.executeQuery(selectTableSQL);
		    while( rs.next() )
		    {
		    	//Retrieve by column name
		    	int respNodeID  	 = rs.getInt("respNodeID");
		    	int hashCode		 = rs.getInt("hashCode");
		    	OverlappingInfoClass overlapObj = new OverlappingInfoClass();
		    	
		    	overlapObj.hashCode = hashCode;
		    	overlapObj.respNodeId = respNodeID;
		    	overlapObj.replyArray = null;
		    	
		    	answerList.put(respNodeID, overlapObj);
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
	
	/**
	 * check if query has functions.
	 * If there are then sometimes 
	 * we need to do extra processing
	 * like for geoJSON, which initially 
	 * is processed by bounding rectangle
	 * @return
	 */
	private boolean ifQueryHasFunctions(Vector<QueryComponent> qcomponents)
	{
		boolean isFun = false;
		for(int i=0;i<qcomponents.size();i++)
		{
			QueryComponent qc = qcomponents.get(i);
			if(qc.getComponentType() == QueryComponent.FUNCTION_PREDICATE)
			{
				isFun = true;
				break;
			}
		}
		return isFun;
	}
	
	public JSONArray processSearchQueryInSubspaceRegion(int subspaceNum, String query)
	{
		long t0 = System.currentTimeMillis();
		
		QueryInfo<NodeIDType> qinfo = new QueryInfo<NodeIDType>(query);
		
		HashMap<String, ProcessingQueryComponent> pqComponents = qinfo.getProcessingQC();
		Vector<QueryComponent> qcomponents = qinfo.getQueryComponents();
		
		boolean isFun = ifQueryHasFunctions(qcomponents);
		
		String tableName = "subspace"+subspaceNum+"DataStorage";
		String mysqlQuery = "";
		
		if(isFun)
		{
			// get all fields as function might need to check them
			// for post processing
			mysqlQuery = "SELECT * from "+tableName+" WHERE ( ";
		}
		else
		{
			mysqlQuery = "SELECT nodeGUID from "+tableName+" WHERE ( ";	
		}
		
		
		Iterator<String> attrIter = pqComponents.keySet().iterator();
		int counter = 0;
		try{
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			ProcessingQueryComponent pqc = pqComponents.get(attrName);
			 
			AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
			
			assert(attrMetaInfo != null);
			
			String dataType = attrMetaInfo.getDataType();
			
			ContextServiceLogger.getLogger().fine("attrName "+attrName+" dataType "+dataType+
					" pqc.getLowerBound() "+pqc.getLowerBound()+" pqc.getUpperBound() "+pqc.getUpperBound()+" pqComponents "+pqComponents.size());
			
			String queryMin  = AttributeTypes.convertStringToDataTypeForMySQL(pqc.getLowerBound(), dataType)+"";
			String queryMax  = AttributeTypes.convertStringToDataTypeForMySQL(pqc.getUpperBound(), dataType)+"";
			
			
			if(counter == (pqComponents.size()-1) )
			{
				// it is assumed that the strings in query(pqc.getLowerBound() or pqc.getUpperBound()) 
				// will have single or double quotes in them so we don't need to them separately in mysql query
				mysqlQuery = mysqlQuery + " ( "+pqc.getAttributeName() +" >= "+queryMin +" AND " 
						+pqc.getAttributeName() +" <= "+queryMax+" ) )";
			}
			else
			{
				mysqlQuery = mysqlQuery + " ( "+pqc.getAttributeName() +" >= "+queryMin +" AND " 
						+pqc.getAttributeName() +" <= "+queryMax+" ) AND ";
			}
			counter++;
			ContextServiceLogger.getLogger().fine(mysqlQuery);
		}
		} catch(Exception | Error ex)
		{
			ex.printStackTrace();
		}
		
		Connection myConn  = null;
		Statement stmt     = null;
		JSONArray jsoArray = new JSONArray();
		
		try
		{
			myConn = this.mysqlDataSource.getConnection();
			stmt   = myConn.createStatement();
			ContextServiceLogger.getLogger().fine("processSearchQueryInSubspaceRegion: "+mysqlQuery);
			
			ResultSet rs = stmt.executeQuery(mysqlQuery);
			
			while( rs.next() )
			{
				//Retrieve by column name
				//double value  	 = rs.getDouble("value");
				if(isFun)
				{
					String nodeGUID = rs.getString("nodeGUID");
					
					boolean satisfies = true;
					// checks against all such functions
					for(int i=0; i<qcomponents.size(); i++)
					{
						QueryComponent qc = qcomponents.get(i);
						
						if( qc.getComponentType() == QueryComponent.FUNCTION_PREDICATE )
						{
							satisfies = qc.getFunction().checkDBRecordAgaistFunction(rs);
							if(!satisfies)
							{
								break;
							}
						}
					}
					if(satisfies)
					{
						jsoArray.put(nodeGUID);
					}
				}
				else
				{
					String nodeGUID = rs.getString("nodeGUID");
					
					//ValueTableInfo valobj = new ValueTableInfo(value, nodeGUID);
					//answerList.add(valobj);
					jsoArray.put(nodeGUID);
				}
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
		long t0 			= System.currentTimeMillis();
		Connection myConn   = null;
		Statement stmt      = null;
		
		String tableName = "subspace"+subspaceNum+"PartitionInfo";
		
		SubspaceInfo<NodeIDType> currSubInfo = subspaceInfo.get(subspaceNum);
		//Vector<AttributePartitionInfo> domainPartInfo = currSubInfo.getDomainPartitionInfo();
		//Vector<String> attrSubspaceInfo = currSubInfo.getAttributesOfSubspace();
		HashMap<String, AttributePartitionInfo> attrSubspaceInfo = currSubInfo.getAttributesOfSubspace();
		
		// subspace vector denotes parition num for each attribute 
		// in this subspace and attrSubspaceInfo.size denotes total 
		// number of attributes. The size of both should be same
		// as both denote number of attributes in this subspace.
		if(attrSubspaceInfo.size() != subspaceVector.size())
		{
			assert(false);
		}
		
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
		
		attrIter = attrSubspaceInfo.keySet().iterator();
		int counter =0;
		while(attrIter.hasNext())
		{
			String attrName = attrIter.next();
			AttributePartitionInfo attrPartInfo = attrSubspaceInfo.get(attrName);
			int partitionNum = subspaceVector.get(counter);
			DomainPartitionInfo domainPartInfo = attrPartInfo.getDomainPartitionInfo().get(partitionNum);
			// if it is a String then single quotes needs to be added
			
			AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
			String dataType = attrMetaInfo.getDataType();
			
			String lowerBound  = AttributeTypes.convertStringToDataTypeForMySQL(domainPartInfo.lowerbound, dataType)+"";
			String upperBound  = AttributeTypes.convertStringToDataTypeForMySQL(domainPartInfo.upperbound, dataType)+"";
			
			insertTableSQL = insertTableSQL + " , "+lowerBound+" , "+ 
					upperBound;
			
			counter++;
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
		
		if( ContextServiceConfig.DELAY_PROFILER_ON )
		{
			DelayProfiler.updateDelay("insertIntoSubspacePartitionInfo", t0);
		}
	}
	
	public JSONObject getGUIDStoredInPrimarySubspace( String guid )
	{
		long t0 = System.currentTimeMillis();
		Connection myConn 	= null;
		Statement stmt 		= null;
		
		String selectQuery 		= "SELECT * ";
		String tableName 		= "primarySubspaceDataStorage";
		
		JSONObject oldValueJSON = new JSONObject();
		
		selectQuery = selectQuery + " FROM "+tableName+" WHERE nodeGUID = '"+guid+"'";
		
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
				Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
				
				while( attrIter.hasNext() )
				{
					String attrName = attrIter.next();
					String oldValueForAttr = rs.getString(attrName);
					try
					{
						oldValueJSON.put(attrName, oldValueForAttr);
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}
					
				}
			}
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
			DelayProfiler.updateDelay("getGUIDStoredInPrimarySubspace", t0);
		}
		return oldValueJSON;
	}
	
	/**
	 * inserts trigger info on a query into the table
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoSubspaceTriggerInfo( int subspaceNum, int hashCode, String userQuery, 
			String groupGUID, String userIP, int userPort )
	{
		long t0 			= System.currentTimeMillis();
		Connection myConn   = null;
		Statement stmt      = null;
		
		// String tableName = "subspace"+subspaceNum+"PartitionInfo";
		
		String tableName = "subspace"+subspaceNum+"TriggerInfo";
		
		//newTableCommand = "create table "+tableName+" ( hashCode INTEGER , " + 
		//		"  userQuery VARCHAR("+HyperspaceMySQLDB.MAX_QUERY_LENGTH+") , groupGUID CHAR(100) , INDEX USING  HASH(hashCode) )";
		
		//newTableCommand = "create table "+tableName+" ( hashCode INTEGER , " + 
		//		"  userQuery VARCHAR("+HyperspaceMySQLDB.MAX_QUERY_LENGTH+") , groupGUID VARCHAR(100) , userIP VARCHAR(20) , "
		//				+ "userPort INTEGER , INDEX USING  HASH(hashCode) )";
		
		String insertTableSQL = "INSERT INTO "+tableName 
				+" ( hashCode, userQuery, groupGUID, userIP, userPort ) VALUES ("+hashCode+", '"+userQuery+"', '"+groupGUID+"',"
						+ " '"+userIP+"', "+userPort+" ) ";
				//+ "nodeID) " + "VALUES"
				//+ "("+lowerRange+","+upperRange+","+nodeID +")";
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
		
		if( ContextServiceConfig.DELAY_PROFILER_ON )
		{
			DelayProfiler.updateDelay("insertIntoSubspaceTriggerInfo", t0);
		}
	}
	
	/**
	 * returns a JSONArray of JSONObjects denoting each row in the table
	 * @param subspaceNum
	 * @param hashCode
	 * @return
	 */
	public JSONArray getTriggerInfo(int subspaceNum, int hashCode)
	{
		long t0 = System.currentTimeMillis();
		
		String tableName 			= "subspace"+subspaceNum+"TriggerInfo";
		
		Connection myConn 			= null;
		Statement stmt 				= null;
		
		String selectQuery 			= "SELECT * ";
		
		//JSONArray hashCodeGroups 	= new JSONArray();
		
		selectQuery = selectQuery + " FROM "+tableName+" WHERE hashCode = "+hashCode;
		
		JSONArray tableRows = new JSONArray();
		
		try
		{
			myConn 	     = this.mysqlDataSource.getConnection();
			stmt   		 = myConn.createStatement();
			ResultSet rs = stmt.executeQuery(selectQuery);
			
			while( rs.next() )
			{
				JSONObject tableRow = new JSONObject();
				tableRow.put( "userQuery", rs.getString("userQuery") );
				tableRow.put( "groupGUID", rs.getString("groupGUID") );
				tableRow.put( "userIP", rs.getString("userIP") );
				tableRow.put( "userPort", rs.getInt("userPort") );
				tableRows.put(tableRow);
			}
			//ContextServiceLogger.getLogger().fine("NodeId "+this.myNodeID+" getGUIDRecordFromPrimarySubspace guid "
			//		+ ""+GUID+" oldValueJSON size "+oldValueJSON.length()+"oldValueJSON "+oldValueJSON);
			rs.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		} catch (JSONException e) {
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
		
		if( ContextServiceConfig.DELAY_PROFILER_ON )
		{
			DelayProfiler.updateDelay("getTriggerInfo", t0);
		}
		return tableRows;
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
    public void storeGUIDInSubspace(String tableName, String nodeGUID, JSONObject attrValuePairs, int updateOrInsert) throws JSONException
    {
    	ContextServiceLogger.getLogger().fine("storeGUIDInSubspace "+tableName+" nodeGUID "+nodeGUID+" attrValuePairs "
    			+attrValuePairs+" updateOrInsert "+updateOrInsert);
    	
        long t0 = System.currentTimeMillis();
        Connection myConn      = null;
        Statement stmt         = null;
       
        String updateSqlQuery     = "UPDATE "+tableName
                + " SET ";
       
        // delayed insert performs better than just insert
        String insertQuery         = "INSERT INTO "+tableName+ " (";
        
        //JSONObject oldValueJSON = new JSONObject();
        try 
        {
        	Iterator<String> jsoObjKeysIter = attrValuePairs.keys();
        	int i=0;
	        while( jsoObjKeysIter.hasNext() )
	        {
	            String attrName = jsoObjKeysIter.next();            
	            String newVal   = attrValuePairs.getString(attrName);
	            
	            AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
				assert(attrMetaInfo != null);
	            String dataType = attrMetaInfo.getDataType();
				
				newVal = AttributeTypes.convertStringToDataTypeForMySQL
						(newVal, dataType)+"";
	
				
	            //oldValueJSON.put(attrName, AttributeTypes.NOT_SET);
	           
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
       
	        //selectQuery = selectQuery + " FROM "+tableName+" WHERE nodeGUID = '"+nodeGUID+"'";
	        updateSqlQuery = updateSqlQuery + " WHERE nodeGUID = '"+nodeGUID+"'";
	        insertQuery = insertQuery + ", nodeGUID) " + "VALUES"+ "(";
                //+ ",'"+nodeGUID+"' )
	        //double oldValue = Double.MIN_VALUE;
       
        
            i = 0;
            //try insert, if fails then update
            jsoObjKeysIter = attrValuePairs.keys();
            while( jsoObjKeysIter.hasNext() )
            {
                String attrName = jsoObjKeysIter.next();
                
                String newValue = attrValuePairs.getString(attrName);
                
                AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
    			
                String dataType = attrMetaInfo.getDataType();
    			
                newValue = AttributeTypes.convertStringToDataTypeForMySQL
    					(newValue, dataType)+"";
               
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
            
            myConn = this.mysqlDataSource.getConnection();
            stmt = myConn.createStatement();   
            if(updateOrInsert == UPDATE_REC)
            {
            	// if update fails then insert
            	try
                {
            		//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE "+updateSqlQuery);
                	int rowCount = stmt.executeUpdate(updateSqlQuery);
                	//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE rowCount "+rowCount);
                	// update failed try insert
                	if(rowCount == 0)
                	{
                		//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE EXCP "+insertQuery);
                    	rowCount = stmt.executeUpdate(insertQuery);
                    	//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE EXCP rowCount "+rowCount);
                	}
                } catch(SQLException sqlEx)
                {
                	try
                	{
	                	//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE EXCP "+insertQuery);
	                	int rowCount = stmt.executeUpdate(insertQuery);
	                	//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE EXCP rowCount "+rowCount);
                	}
                	// insert failed because of another insert, which caused primary key violation
                	catch(SQLException sqlEx2)
                	{
                		int rowCount = stmt.executeUpdate(updateSqlQuery);
                	}
                }
            }
            else if(updateOrInsert == INSERT_REC)
            {
            	try
                {
            		ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING INSERT "+insertQuery);
            		int rowCount = stmt.executeUpdate(insertQuery);
            		ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING INSERT rowCount "+rowCount+" insertQuery "+insertQuery);
            		// duplicate insert always gives exception so no need to check rowCount and do update
            		// it happends in exception code
                } catch(SQLException sqlEx)
                {
                	//ContextServiceLogger.getLogger().fine("EXECUTING INSERT "+updateSqlQuery);
                	int rowCount = stmt.executeUpdate(updateSqlQuery);
                	//ContextServiceLogger.getLogger().fine("EXECUTING INSERT rowCount "+rowCount);
                }
            }
            // execute insert SQL stetement
            //statement.executeUpdate(sqlQuery);
        } catch (Exception  | Error ex)
        {
            ex.printStackTrace();
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
            DelayProfiler.updateDelay("storeGUIDInSubspace", t0);
        }
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
	
	public JSONObject getGUIDRecordFromPrimarySubspace(String GUID)
	{
		long t0 = System.currentTimeMillis();
		
		String tableName 			= "primarySubspaceDataStorage";
		
		Connection myConn 			= null;
		Statement stmt 				= null;
		
		String selectQuery 			= "SELECT * ";
		
		JSONObject oldValueJSON 	= new JSONObject();
		
		selectQuery = selectQuery + " FROM "+tableName+" WHERE nodeGUID = '"+GUID+"'";
		
		try
		{
			myConn = this.mysqlDataSource.getConnection();
			stmt = myConn.createStatement();
			ResultSet rs = stmt.executeQuery(selectQuery);
			
			while( rs.next() )
			{
				Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
				
				while( attrIter.hasNext() )
				{
					String attrName = attrIter.next();
					String oldValueForAttr = rs.getString(attrName);
					
					try 
					{
						oldValueJSON.put(attrName, oldValueForAttr);
					} catch (JSONException e)
					{
						e.printStackTrace();
					}
				}
			}
			ContextServiceLogger.getLogger().fine("NodeId "+this.myNodeID+" getGUIDRecordFromPrimarySubspace guid "+GUID+" oldValueJSON size "+oldValueJSON.length()+"oldValueJSON "+oldValueJSON);
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
	
	/**
	 * stores GUID in a subspace. The decision to store a guid on this node
	 * in this subspace is not made in this fucntion.
	 * @param subspaceNum
	 * @param nodeGUID
	 * @param attrValuePairs
	 * @return
	 * @throws JSONException
	 */
	/*@SuppressWarnings("unchecked")
	public boolean storeGUIDInSubspace(String tableName, String nodeGUID, JSONObject attrValuePairs) 
			throws JSONException
	{
		boolean success = false;
		long t0 = System.currentTimeMillis();
		Connection myConn 		= null;
		Statement stmt 			= null;
		
		String insertQuery 		= "REPLACE INTO "+tableName+ " (";
		
		Iterator<String> jsoObjKeysIter = attrValuePairs.keys();
		int i=0;
		while( jsoObjKeysIter.hasNext() )
		{
			String attrName = jsoObjKeysIter.next();
			//double newVal = attrValuePairs.getDouble(attrName);
			
			if(i == 0)
			{
				insertQuery = insertQuery + attrName;
			}
			else
			{
				insertQuery = insertQuery +", "+attrName;
			}
			i++;
		}
		
		insertQuery = insertQuery + ", nodeGUID) " + "VALUES"+ "(";
		
		try
		{
			myConn = this.mysqlDataSource.getConnection();
			stmt = myConn.createStatement();
				
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
			success = true;
		} catch (SQLException e)
		{
			success = false;
			e.printStackTrace();
		} finally
		{
			try
			{
				if ( stmt != null )
					stmt.close();
				if ( myConn != null )
					myConn.close();
			}
			catch(SQLException e)
			{
				e.printStackTrace();
			}
		}	
		if( ContextServiceConfig.DELAY_PROFILER_ON )
		{
			DelayProfiler.updateDelay("storeGUIDInSubspace", t0);
		}	
		return success;
	}*/
}