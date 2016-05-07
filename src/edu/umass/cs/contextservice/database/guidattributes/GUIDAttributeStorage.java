package edu.umass.cs.contextservice.database.guidattributes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
import edu.umass.cs.contextservice.database.DataSource;
import edu.umass.cs.contextservice.database.HyperspaceMySQLDB;
import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.dataformat.AttrValueRepresentationJSON;
import edu.umass.cs.contextservice.messages.dataformat.SearchReplyGUIDRepresentationJSON;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.utils.DelayProfiler;

/**
 * This class implements GUIDAttributeStorageInterface.
 * This class has moethids for table creation, updates and searches of 
 * context service.
 * @author adipc
 *
 */
public class GUIDAttributeStorage<NodeIDType> implements GUIDAttributeStorageInterface<NodeIDType>
{
	private final NodeIDType myNodeID;
	private final HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap;
	private final DataSource<NodeIDType> dataSource;
	
	public GUIDAttributeStorage( NodeIDType myNodeID, 
			HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap , 
			DataSource<NodeIDType> dataSource )
	{
		this.myNodeID = myNodeID;
		this.subspaceInfoMap = subspaceInfoMap;
		this.dataSource = dataSource;
	}
	
	
	@Override
	public void createTables() 
	{
		Connection myConn  = null;
		Statement  stmt    = null;
		
		try
		{
			myConn = dataSource.getConnection();
			stmt   =  myConn.createStatement();
			Iterator<Integer> subspaceIter = this.subspaceInfoMap.keySet().iterator();
			
			while( subspaceIter.hasNext() )
			{
				int subspaceId = subspaceIter.next();
				Vector<SubspaceInfo<NodeIDType>> replicasOfSubspace 
										= subspaceInfoMap.get(subspaceId);
				
				for(int i = 0; i<replicasOfSubspace.size(); i++)
				{
					SubspaceInfo<NodeIDType> subInfo = replicasOfSubspace.get(i);
					
					int replicaNum = subInfo.getReplicaNum();
					
					HashMap<String, AttributePartitionInfo> subspaceAttributes = subInfo.getAttributesOfSubspace();
					
					// partition info storage info
					String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"PartitionInfo";
					
					String newTableCommand = "create table "+tableName+" ( hashCode INTEGER PRIMARY KEY , "
						      + "   respNodeID INTEGER ";
					
					//	      + ", upperRange DOUBLE NOT NULL, nodeID INT NOT NULL, "
					//	      + "   partitionNum INT AUTO_INCREMENT, INDEX USING BTREE (lowerRange, upperRange) )";
					//TODO: which indexing scheme is better, indexing two attribute once or creating a index over all 
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
					
					
					// partition info table is created for every node,
					// whether or not it is in replica, but data storage table
					// are only created on data storing nodes.
					// similar for trigger storage
					if( !subInfo.checkIfSubspaceHasMyID(myNodeID) )
					{
						continue;
					}
					
					//TODO: which indexing scheme is better, indexing two attribute once or creating a index over all 
					// attributes
					// datastorage table of each subspace
					tableName = "subspaceId"+subspaceId+"DataStorage";
					
					newTableCommand = "create table "+tableName+" ( "
						      + "   nodeGUID Binary(20) PRIMARY KEY";
					
					newTableCommand = getDataStorageString(newTableCommand);
					
					newTableCommand = newTableCommand +" )";
					stmt.executeUpdate(newTableCommand);
				}
			}
			
			String tableName = "primarySubspaceDataStorage";
			String newTableCommand = "create table "+tableName+" ( "
				      + " nodeGUID Binary(20) PRIMARY KEY";
			
			newTableCommand = getDataStorageString(newTableCommand);
			newTableCommand	= getPrivacyStorageString(newTableCommand);
			
			// row format dynamic because we want TEXT columns to be stored completely off the row, 
			// only pointer should be stored in the row, otherwise default is storing 700 bytes for each TEXT in row.
			newTableCommand = newTableCommand +" ) ROW_FORMAT=DYNAMIC ";
			stmt.executeUpdate(newTableCommand);
			
		} catch( SQLException mysqlEx )
		{
			mysqlEx.printStackTrace();
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
			catch( SQLException sqex )
			{
				sqex.printStackTrace();
			}
		}
	}
	
	/**
	 * Returns the search query, it doesn't execute. This
	 * is done so that this can be executed as a nested query in privacy case.
	 * HyperspaceMySQLDB calling this function has more info.
	 * @param subspaceId
	 * @param query
	 * @param resultArray
	 * @return
	 */
	public String getMySQLQueryForProcessSearchQueryInSubspaceRegion
										(int subspaceId, String query)
	{
		QueryInfo<NodeIDType> qinfo = new QueryInfo<NodeIDType>(query);
		
		HashMap<String, ProcessingQueryComponent> pqComponents = qinfo.getProcessingQC();
		Vector<QueryComponent> qcomponents = qinfo.getQueryComponents();
		
		boolean isFun = ifQueryHasFunctions(qcomponents);
		
		String tableName = "subspaceId"+subspaceId+"DataStorage";
		String mysqlQuery = "";
		
		if( isFun )
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
		try
		{
			while(attrIter.hasNext())
			{
				String attrName = attrIter.next();
				ProcessingQueryComponent pqc = pqComponents.get(attrName);
				 
				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
				
				assert(attrMetaInfo != null);
				
				String dataType = attrMetaInfo.getDataType();
				
				ContextServiceLogger.getLogger().fine("attrName "+attrName+" dataType "+dataType+
						" pqc.getLowerBound() "+pqc.getLowerBound()+" pqc.getUpperBound() "+pqc.getUpperBound()+
						" pqComponents "+pqComponents.size());
				
				// normal case of lower value being lesser than the upper value
				if(AttributeTypes.compareTwoValues(pqc.getLowerBound(), pqc.getUpperBound(), dataType))
				{
					String queryMin  = AttributeTypes.convertStringToDataTypeForMySQL(pqc.getLowerBound(), dataType)+"";
					String queryMax  = AttributeTypes.convertStringToDataTypeForMySQL(pqc.getUpperBound(), dataType)+"";
					
					if( counter == (pqComponents.size()-1) )
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
				}
				else
				{
					if(counter == (pqComponents.size()-1) )
					{
						String queryMin  = AttributeTypes.convertStringToDataTypeForMySQL(attrMetaInfo.getMinValue(), dataType)+"";
						String queryMax  = AttributeTypes.convertStringToDataTypeForMySQL(pqc.getUpperBound(), dataType)+"";
						
						mysqlQuery = mysqlQuery + " ( "
								+" ( "+pqc.getAttributeName() +" >= "+queryMin +" AND " 
								+pqc.getAttributeName() +" <= "+queryMax+" ) OR ";
								
						queryMin  = AttributeTypes.convertStringToDataTypeForMySQL(pqc.getLowerBound(), dataType)+"";
						queryMax  = AttributeTypes.convertStringToDataTypeForMySQL(attrMetaInfo.getMaxValue(), dataType)+"";
						
						mysqlQuery = mysqlQuery +" ( "+pqc.getAttributeName() +" >= "+queryMin +" AND " 
								+pqc.getAttributeName() +" <= "+queryMax+" ) ) )";
					}
					else
					{
						String queryMin  = AttributeTypes.convertStringToDataTypeForMySQL(attrMetaInfo.getMinValue(), dataType)+"";
						String queryMax  = AttributeTypes.convertStringToDataTypeForMySQL(pqc.getUpperBound(), dataType)+"";
						
						mysqlQuery = mysqlQuery + " ( "
								+" ( "+pqc.getAttributeName() +" >= "+queryMin +" AND " 
								+pqc.getAttributeName() +" <= "+queryMax+" ) OR ";
								
						queryMin  = AttributeTypes.convertStringToDataTypeForMySQL(pqc.getLowerBound(), dataType)+"";
						queryMax  = AttributeTypes.convertStringToDataTypeForMySQL(attrMetaInfo.getMaxValue(), dataType)+"";
						
						mysqlQuery = mysqlQuery +" ( "+pqc.getAttributeName() +" >= "+queryMin +" AND " 
								+pqc.getAttributeName() +" <= "+queryMax+" ) ) AND ";
					}
				}
				
				counter++;
				//ContextServiceLogger.getLogger().fine(mysqlQuery);
			}
			return mysqlQuery;
		} catch(Exception | Error ex)
		{
			ex.printStackTrace();
		}
		return null;
	}
	
	public int processSearchQueryInSubspaceRegion(int subspaceId, String query, 
			JSONArray resultArray)
	{
		long t0 = System.currentTimeMillis();
		
		String mysqlQuery = getMySQLQueryForProcessSearchQueryInSubspaceRegion
				(subspaceId, query);
		
		//should not be expensive operation to be performed twice.
		QueryInfo<NodeIDType> qinfo = new QueryInfo<NodeIDType>(query);
		
		Vector<QueryComponent> qcomponents = qinfo.getQueryComponents();
		
		boolean isFun = ifQueryHasFunctions(qcomponents);
		
		assert(mysqlQuery != null);
		
		Connection myConn  = null;
		Statement stmt     = null;
		//JSONArray jsoArray = new JSONArray();
		int resultSize = 0;
		try
		{
			myConn = this.dataSource.getConnection();
			// for row by row fetching, oterwise default is fetching whole result
			// set in memory. http://dev.mysql.com/doc/connector-j/en/connector-j-reference-implementation-notes.html
			stmt   = myConn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, 
					java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(ContextServiceConfig.MYSQL_CURSOR_FETCH_SIZE);
			
			ContextServiceLogger.getLogger().fine("processSearchQueryInSubspaceRegion: "+mysqlQuery);
			
			ResultSet rs = stmt.executeQuery(mysqlQuery);
			while( rs.next() )
			{
				//Retrieve by column name
				//double value  	 = rs.getDouble("value");
				if(isFun)
				{
					//String nodeGUID = rs.getString("nodeGUID");
					byte[] nodeGUIDBytes = rs.getBytes("nodeGUID");
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
						if(ContextServiceConfig.sendFullReplies)
						{
							String nodeGUID = Utils.bytArrayToHex(nodeGUIDBytes);
							resultArray.put(nodeGUID);
							resultSize++;
						}
						else
						{
							resultSize++;
						}
					}
				}
				else
				{
					//String nodeGUID = rs.getString("nodeGUID");
					byte[] nodeGUIDBytes = rs.getBytes("nodeGUID");
					// it is actually a JSONArray in hexformat byte array representation.
					// reverse conversion is byte array to String and then string to JSONArray.
					//byte[] realIDEncryptedArray = rs.getBytes(ACLattr);
					//ValueTableInfo valobj = new ValueTableInfo(value, nodeGUID);
					//answerList.add(valobj);
					if(ContextServiceConfig.sendFullReplies)
					{
						String nodeGUID = Utils.bytArrayToHex(nodeGUIDBytes);
						
						SearchReplyGUIDRepresentationJSON searchReplyRep 
						= new SearchReplyGUIDRepresentationJSON(nodeGUID);
						resultArray.put(searchReplyRep.toJSONObject());
						
						//TODO: this conversion may be removed and byte[] sent
						//FIXME: fix this string encoding.
//						if(realIDEncryptedArray != null)
//						{
//							String jsonArrayString = new String(realIDEncryptedArray);
//							JSONArray jsonArr = new JSONArray(jsonArrayString);						
//							
//							SearchReplyGUIDRepresentationJSON searchReplyRep 
//								= new SearchReplyGUIDRepresentationJSON(nodeGUID, jsonArr);
//							resultArray.put(searchReplyRep.toJSONObject());
//						}
//						else
//						{
//							SearchReplyGUIDRepresentationJSON searchReplyRep 
//							= new SearchReplyGUIDRepresentationJSON(nodeGUID);
//							resultArray.put(searchReplyRep.toJSONObject());
//						}
						
						resultSize++;
					}
					else
					{
						resultSize++;
					}
				}
			}
			
			rs.close();
			stmt.close();
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		} catch (JSONException e) 
		{
			e.printStackTrace();
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
		return resultSize;
	}
	
	
	public JSONObject getGUIDStoredInPrimarySubspace( String guid )
	{
		long t0 = System.currentTimeMillis();
		Connection myConn 	= null;
		Statement stmt 		= null;
		
		String selectQuery 		= "SELECT * ";
		String tableName 		= "primarySubspaceDataStorage";
		
		JSONObject oldValueJSON = new JSONObject();
		
		selectQuery = selectQuery + " FROM "+tableName+" WHERE nodeGUID = X'"+guid+"'";
		
		try
		{
			myConn = this.dataSource.getConnection();
			stmt = myConn.createStatement();
			long start = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(selectQuery);
			
			while( rs.next() )
			{
				ResultSetMetaData rsmd = rs.getMetaData();
				
				int columnCount = rsmd.getColumnCount();
				
				// The column count starts from 1
				for (int i = 1; i <= columnCount; i++ ) 
				{
					String colName = rsmd.getColumnName(i);
					String colVal = rs.getString(colName);
					try
					{
						oldValueJSON.put(colName, colVal);
					} catch (JSONException e) 
					{
						e.printStackTrace();
					}
				}
			}
			rs.close();	
			long end = System.currentTimeMillis();
			if(ContextServiceConfig.DEBUG_MODE)
			{
				System.out.println("TIME_DEBUG: getGUIDStoredInPrimarySubspace "+(end-start));
			}
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
	 * Inserts a subspace region denoted by subspace vector, 
	 * integer denotes partition num in partition info 
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoSubspacePartitionInfo(int subspaceId, int replicaNum,
			List<Integer> subspaceVector, NodeIDType respNodeId)
	{
		long t0 			= System.currentTimeMillis();
		Connection myConn   = null;
		Statement stmt      = null;
		
		String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"PartitionInfo";
		
		SubspaceInfo<NodeIDType> currSubInfo = subspaceInfoMap.
				get(subspaceId).get(replicaNum);
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
		
		String insertTableSQL = "SET unique_checks=0; INSERT INTO "+tableName 
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
			DomainPartitionInfo domainPartInfo = attrPartInfo.getSubspaceDomainPartitionInfo().get(partitionNum);
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
			myConn = this.dataSource.getConnection();
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
	
	/**
	 * bulk insert is needed when number of partitions are very large
	 * @param subspaceId
	 * @param replicaNum
	 * @param subspaceVectorList
	 * @param respNodeIdList
	 */
	public void bulkInsertIntoSubspacePartitionInfo( int subspaceId, int replicaNum,
			List<List<Integer>> subspaceVectorList, List<NodeIDType> respNodeIdList )
	{
		assert(subspaceVectorList.size() == respNodeIdList.size());
		
		ContextServiceLogger.getLogger().fine("bulkInsertIntoSubspacePartitionInfo called subspaceId "
				+subspaceId + " replicaNum "+replicaNum+" "+subspaceVectorList.size()+" "+respNodeIdList.size() );
		
		long t0 							= System.currentTimeMillis();
		Connection myConn   				= null;
		PreparedStatement prepStmt      	= null;
		
		String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"PartitionInfo";
		
		SubspaceInfo<NodeIDType> currSubInfo = subspaceInfoMap.
				get(subspaceId).get(replicaNum);
		//Vector<AttributePartitionInfo> domainPartInfo = currSubInfo.getDomainPartitionInfo();
		//Vector<String> attrSubspaceInfo = currSubInfo.getAttributesOfSubspace();
		HashMap<String, AttributePartitionInfo> attrSubspaceInfo = currSubInfo.getAttributesOfSubspace();
		
		// subspace vector denotes parition num for each attribute 
		// in this subspace and attrSubspaceInfo.size denotes total 
		// number of attributes. The size of both should be same
		// as both denote number of attributes in this subspace.
		if(attrSubspaceInfo.size() != subspaceVectorList.get(0).size())
		{
			assert(false);
		}
		
		String insertTableSQL = " INSERT INTO "+tableName 
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
		
		insertTableSQL = insertTableSQL + " ) VALUES ( "+"?" + 
				" , "+"?";
		
		attrIter = attrSubspaceInfo.keySet().iterator();
		while(attrIter.hasNext())
		{
			// just  that the loop moves on
			attrIter.next();
			// for lower and upper value of each attribute of this subspace
			insertTableSQL = insertTableSQL + " , "+"?"+" , "+ 
					"?";
		}
		insertTableSQL = insertTableSQL + " ) ";
		
		//ContextServiceLogger.getLogger().fine("insertTableSQL "+insertTableSQL);
		try
		{
			myConn = this.dataSource.getConnection();
			
			prepStmt = myConn.prepareStatement(insertTableSQL);
			for( int i=0; i<subspaceVectorList.size(); i++ )
			{
				List<Integer> subspaceVector = subspaceVectorList.get(i);
				NodeIDType respNodeId = respNodeIdList.get(i);
				prepStmt.setInt(1, subspaceVector.hashCode());
				prepStmt.setInt(2, Integer.parseInt(respNodeId.toString()));
				
				attrIter = attrSubspaceInfo.keySet().iterator();
				int counter =0;
				int parameterIndex = 2;
				while(attrIter.hasNext())
				{
					String attrName = attrIter.next();
					AttributePartitionInfo attrPartInfo = attrSubspaceInfo.get(attrName);
					int partitionNum = subspaceVector.get(counter);
					DomainPartitionInfo domainPartInfo = attrPartInfo.getSubspaceDomainPartitionInfo().get(partitionNum);
					// if it is a String then single quotes needs to be added
					
					AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
					String dataType = attrMetaInfo.getDataType();
					
					AttributeTypes.insertStringToDataTypeForMySQLPrepStmt
					(domainPartInfo.lowerbound, dataType, prepStmt, ++parameterIndex);
					
					AttributeTypes.insertStringToDataTypeForMySQLPrepStmt
					(domainPartInfo.upperbound, dataType, prepStmt, ++parameterIndex);
					
//					insertTableSQL = insertTableSQL + " , "+"?"+" , "+ 
//							"?";
					counter++;
				}
				prepStmt.addBatch();
			}
			// stmt = myConn.createStatement();
			// execute insert SQL statement
			prepStmt.executeBatch();
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
				if( prepStmt != null )
				{
					prepStmt.close();
				}
			} catch(SQLException sqex)
			{
				sqex.printStackTrace();
			}
		}
		
		ContextServiceLogger.getLogger().fine("bulkInsertIntoSubspacePartitionInfo completed "
				+ subspaceVectorList.size()+" "+respNodeIdList.size() );
		
		if( ContextServiceConfig.DELAY_PROFILER_ON )
		{
			DelayProfiler.updateDelay("insertIntoSubspacePartitionInfo", t0);
		}
	}
	
	/**
     * Stores GUID in a subspace. The decision to store a guid on this node
     * in this subspace is not made in this function.
     * @param subspaceNum
     * @param nodeGUID
     * @param attrValuePairs
     * @return
     * @throws JSONException
     */
    public void storeGUIDInPrimarySubspace(String tableName, String nodeGUID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep, int updateOrInsert, 
    		JSONObject oldValJSON ) throws JSONException
    {
    	if( updateOrInsert == HyperspaceMySQLDB.INSERT_REC )
    	{
    		this.performStoreGUIDInPrimarySubspaceInsert
    			(tableName, nodeGUID, atrToValueRep);
    	}
    	else if( updateOrInsert == HyperspaceMySQLDB.UPDATE_REC )
    	{
    		this.performStoreGUIDInPrimarySubspaceUpdate
    				(tableName, nodeGUID, atrToValueRep, oldValJSON);
    	}
    }
    
	
	/**
     * Stores GUID in a subspace. The decision to store a guid on this node
     * in this subspace is not made in this function.
     * @param subspaceNum
     * @param nodeGUID
     * @param attrValuePairs
     * @return
     * @throws JSONException
     */
    public void storeGUIDInSecondarySubspace(String tableName, String nodeGUID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep, int updateOrInsert
    		, JSONObject oldValJSON ) throws JSONException
    {
    	long start = System.currentTimeMillis();
    	if(ContextServiceConfig.DEBUG_MODE)
    	{
    		ContextServiceLogger.getLogger().fine("STARTED storeGUIDInSecondarySubspace "+nodeGUID);
    	}
    	if( updateOrInsert == HyperspaceMySQLDB.INSERT_REC )
    	{
    		this.performStoreGUIDInSecondarySubspaceInsert
    			(tableName, nodeGUID, atrToValueRep, oldValJSON);
    	}
    	else if( updateOrInsert == HyperspaceMySQLDB.UPDATE_REC )
    	{
    		this.performStoreGUIDInSecondarySubspaceUpdate
    				(tableName, nodeGUID, atrToValueRep);
    	}
    	if(ContextServiceConfig.DEBUG_MODE)
    	{
    		long end = System.currentTimeMillis();
    		ContextServiceLogger.getLogger().fine
    		("FINISHED storeGUIDInSecondarySubspace "+nodeGUID+" time "+(end-start));
    	}
    }
    
    public void deleteGUIDFromSubspaceRegion(String tableName, String nodeGUID)
	{
		long t0 = System.currentTimeMillis();
		String deleteCommand = "DELETE FROM "+tableName+" WHERE nodeGUID= X'"+nodeGUID+"'";
		Connection myConn 	= null;
		Statement stmt 		= null;
		
		try
		{
			myConn = this.dataSource.getConnection();
			stmt = myConn.createStatement();
			long start = System.currentTimeMillis();
			stmt.executeUpdate(deleteCommand);
			long end = System.currentTimeMillis();
			
			if(ContextServiceConfig.DEBUG_MODE)
        	{
        		System.out.println("TIME_DEBUG: deleteGUIDFromSubspaceRegion "+(end-start));
        	}
			
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
    
    /**
	 * Returns a list of regions/nodes that overlap with a query in a given subspace.
	 * @param subspaceNum
	 * @param qcomponents, takes matching attributes as input
	 * @return
	 */
	public HashMap<Integer, OverlappingInfoClass> 
		getOverlappingRegionsInSubspace( int subspaceId, int replicaNum, 
				Vector<ProcessingQueryComponent> matchingQueryComponents )
	{
		long t0 = System.currentTimeMillis();
		HashMap<Integer, OverlappingInfoClass> answerList 
						= new HashMap<Integer, OverlappingInfoClass>();
		
		String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"PartitionInfo";
		
		String selectTableSQL = "SELECT hashCode, respNodeID from "+tableName+" WHERE ";
		
		for( int i=0; i<matchingQueryComponents.size(); i++ )
		{
			ProcessingQueryComponent qcomponent = matchingQueryComponents.get(i);
			String attrName = qcomponent.getAttributeName();
			
			String lowerAttr = "lower"+attrName;
			String upperAttr = "upper"+attrName;
			
			AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
			String dataType = attrMetaInfo.getDataType();
			
			if(AttributeTypes.compareTwoValues(qcomponent.getLowerBound(),
					qcomponent.getUpperBound(), dataType))
			{
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
				
				// follwing the convention that the in (lowerVal, upperVal) range lowerVal is included in 
				// range and upperVal is not included in range. This convnetion is for data storage in mysql
				// queryMin and queryMax aare always both end points included.
				// means a query >= queryMin and query <= queryMax, but never query > queryMin and query < queryMax
				selectTableSQL = selectTableSQL +" ( "
						+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
						+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
						+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" ) ";
			}
			else // when lower value in query predicate is greater than upper value, meaning circular query, 
				// it is done mostly for generating uniform workload for experiments
			{
				// first case from lower to max value
				String queryMin  =  AttributeTypes.convertStringToDataTypeForMySQL(qcomponent.getLowerBound(), dataType) + "";
				String queryMax  =  AttributeTypes.convertStringToDataTypeForMySQL(attrMetaInfo.getMaxValue(), dataType) + "";
				
				selectTableSQL = selectTableSQL +"( ( "
						+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
						+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
						+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" ) OR ";
				
				// second case from minvalue to upper val
				queryMin  =  AttributeTypes.convertStringToDataTypeForMySQL(attrMetaInfo.getMinValue(), dataType) + "";
				queryMax  =  AttributeTypes.convertStringToDataTypeForMySQL(qcomponent.getUpperBound(), dataType) + "";
				selectTableSQL = selectTableSQL +"( "
						+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
						+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
						+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" )  )";
			}
			
			if( i != (matchingQueryComponents.size()-1) )
			{
				selectTableSQL = selectTableSQL + " AND ";
			}
		}
		
		Statement stmt 		= null;
		Connection myConn 	= null;
		try
		{
			myConn = this.dataSource.getConnection();
			stmt = myConn.createStatement();
			ContextServiceLogger.getLogger().fine("selectTableSQL "+selectTableSQL);
			long start = System.currentTimeMillis();
			ResultSet rs = stmt.executeQuery(selectTableSQL);
			long end = System.currentTimeMillis();
			
			if(ContextServiceConfig.DEBUG_MODE)
			{
				System.out.println("TIME_DEBUG: getOverlappingRegionsInSubspace time "+(end-start));
			}
			
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
	 * only need to update attributes in atToValRep,
	 *  as other attribtues are already there.
	 * @param tableName
	 * @param nodeGUID
	 * @param atrToValueRep
	 */
	private void performStoreGUIDInSecondarySubspaceUpdate(String tableName, String nodeGUID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep)
	{
		ContextServiceLogger.getLogger().fine("STARTED performStoreGUIDInSecondarySubspaceUpdate "+tableName
				+" nodeGUID "+nodeGUID);
    	
        Connection myConn      = null;
        Statement stmt         = null;
        
        String updateSqlQuery     	= "UPDATE "+tableName
                + " SET ";
        
        try
        {
        	Iterator<String> attrNameIter = atrToValueRep.keySet().iterator();
        	int i=0;
	        while( attrNameIter.hasNext() )
	        {
	            String attrName = attrNameIter.next();  
	            AttrValueRepresentationJSON attrValRep 
	            		= atrToValueRep.get(attrName);
	            
	            String newVal   = attrValRep.getActualAttrValue();
	            
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
	            }
	            else
	            {
	                //selectQuery = selectQuery + ", "+attrName+" ";
	                updateSqlQuery = updateSqlQuery +" , "+ attrName +" = "+newVal;
	            }
	            i++;
	        }
	        
	        //selectQuery = selectQuery + " FROM "+tableName+" WHERE nodeGUID = '"+nodeGUID+"'";
	        updateSqlQuery = updateSqlQuery + " WHERE nodeGUID = X'"+nodeGUID+"'";
            
            myConn = this.dataSource.getConnection();
            stmt = myConn.createStatement();
            
        	// if update fails then insert
    		//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE "+updateSqlQuery);
    		long start   = System.currentTimeMillis();
        	int rowCount = stmt.executeUpdate(updateSqlQuery);
        	long end     = System.currentTimeMillis();
        	
        	if(ContextServiceConfig.DEBUG_MODE)
        	{
        		System.out.println("TIME_DEBUG: performStoreGUIDInSecondarySubspaceUpdate "
        						+(end-start));
        	}
        	
        	//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE rowCount "+rowCount);
        	// update failed try insert
        	if(rowCount == 0)
        	{
        		ContextServiceLogger.getLogger().fine("ASSERTION FAIL");
        		// should not happen, rowCount should always be 1
        		assert(false);
        	}
            
        } catch ( Exception  | Error ex )
        {
            ex.printStackTrace();
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
	}
	
	private void performStoreGUIDInSecondarySubspaceInsert(String tableName, String nodeGUID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep, 
    		JSONObject oldValJSON)
	{
		ContextServiceLogger.getLogger().fine("STARTED performStoreGUIDInSubspaceInsert "
				+tableName+" nodeGUID "+nodeGUID);
    	
        Connection myConn      = null;
        Statement stmt         = null;
       
        // delayed insert performs better than just insert
        String insertQuery         = "INSERT INTO "+tableName+ " (";
        
        //JSONObject oldValueJSON = new JSONObject();
        try
        {
        	// insert happens for all attributes,
        	// updated attributes are taken from atrToValueRep and
        	// other attributes are taken from oldValJSON
        	Iterator<Integer> subapceIdIter = subspaceInfoMap.keySet().iterator();
    		
        	boolean first = true;
        	// just a way to iterate over attributes.
    		while( subapceIdIter.hasNext() )
    		{
    			int subspaceId = subapceIdIter.next();
    			// at least one replica and all replica have same default value for each attribute.
    			SubspaceInfo<NodeIDType> currSubspaceInfo 
    										= subspaceInfoMap.get(subspaceId).get(0);
    			HashMap<String, AttributePartitionInfo> attrSubspaceMap 
    										= currSubspaceInfo.getAttributesOfSubspace();
    			
    			Iterator<String> attrIter = attrSubspaceMap.keySet().iterator();
    			
    			while( attrIter.hasNext() )
    			{
    				String currAttrName = attrIter.next();
    				if(first)
    	            {
    	                insertQuery = insertQuery + currAttrName;
    	                first = false;
    	            }
    	            else
    	            {
    	                insertQuery = insertQuery +", "+currAttrName;
    	            }
    			}
    		}
    		insertQuery = insertQuery + ", nodeGUID) " + "VALUES"+ "(";
    		
    		
    		first = true;
    		subapceIdIter = subspaceInfoMap.keySet().iterator();
        	// just a way to iterate over attributes.
    		while( subapceIdIter.hasNext() )
    		{
    			int subspaceId = subapceIdIter.next();
    			// at least one replica and all replica have same default value for each attribute.
    			SubspaceInfo<NodeIDType> currSubspaceInfo 
    										= subspaceInfoMap.get(subspaceId).get(0);
    			HashMap<String, AttributePartitionInfo> attrSubspaceMap 
    										= currSubspaceInfo.getAttributesOfSubspace();
    			
    			Iterator<String> attrIter = attrSubspaceMap.keySet().iterator();
    			
    			while( attrIter.hasNext() )
    			{
    				String currAttrName = attrIter.next();
    				String currAttrValue = "";
    				if( atrToValueRep.containsKey(currAttrName) )
    				{
    					AttrValueRepresentationJSON attrValRep 
											= atrToValueRep.get(currAttrName);
    
    					currAttrValue = attrValRep.getActualAttrValue();
    				}
    				else
    				{
    					currAttrValue = oldValJSON.getString(currAttrName);
    				}
    				
    				AttributeMetaInfo attrMetaInfo 
    							= AttributeTypes.attributeMap.get(currAttrName);
    				
    			    String dataType = attrMetaInfo.getDataType();
    				
    			    currAttrValue = AttributeTypes.convertStringToDataTypeForMySQL
    						(currAttrValue, dataType)+"";
    			    
    				if(first)
    	            {
    					insertQuery = insertQuery + currAttrValue;
    	                first = false;
    	            }
    	            else
    	            {
    	            	insertQuery = insertQuery +" , "+currAttrValue;
    	            }
    			}
    		}
    		insertQuery = insertQuery +" , X'"+nodeGUID+"' )";
    		
    		myConn = this.dataSource.getConnection();
            stmt = myConn.createStatement();  
            
    		ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING INSERT "+insertQuery);
    		long start   = System.currentTimeMillis();
    		int rowCount = stmt.executeUpdate(insertQuery);
    		long end     = System.currentTimeMillis();
    		
    		if(ContextServiceConfig.DEBUG_MODE)
        	{
        		System.out.println("TIME_DEBUG: performStoreGUIDInSecondarySubspaceInsert insert  "+tableName
        				+" nodeGUID "+nodeGUID+" "+(end-start));
        	}
    		
    		ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING INSERT rowCount "+rowCount+" insertQuery "+insertQuery);
        	
        } catch ( Exception  | Error ex )
        {
            ex.printStackTrace();
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
	}
	
	/**
	 * Only need to update attributes in atToValRep,
	 * as other attribtues are already there.
	 * @param tableName
	 * @param nodeGUID
	 * @param atrToValueRep
	 */
	private void performStoreGUIDInPrimarySubspaceUpdate( String tableName, String nodeGUID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep, JSONObject oldValJSON )
	{
		ContextServiceLogger.getLogger().fine("performStoreGUIDInPrimarySubspaceUpdate "+tableName
				+" nodeGUID "+nodeGUID);
		
        Connection myConn      		= null;
        Statement stmt         		= null;
        
        String updateSqlQuery     	= "UPDATE "+tableName+ " SET ";
        
        try
        {
        	Iterator<String> attrNameIter = atrToValueRep.keySet().iterator();
        	int i = 0;
	        
        	while( attrNameIter.hasNext() )
	        {
	            String attrName = attrNameIter.next();  
	            AttrValueRepresentationJSON attrValRep 
	            		= atrToValueRep.get(attrName);
	            
	            String newVal   = attrValRep.getActualAttrValue();
	            
	            AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
				assert(attrMetaInfo != null);
	            String dataType = attrMetaInfo.getDataType();
				
				newVal = AttributeTypes.convertStringToDataTypeForMySQL
						(newVal, dataType)+"";	
                
	            if( i == 0 )
	            {
	                //selectQuery = selectQuery + attrName;
	                updateSqlQuery = updateSqlQuery + attrName +" = "+newVal;
	            }
	            else
	            {
	                //selectQuery = selectQuery + ", "+attrName+" ";
	                updateSqlQuery = updateSqlQuery +" , "+ attrName +" = "+newVal;
	            }
	            
	            //oldValueJSON.put(attrName, AttributeTypes.NOT_SET);
				// store the ACL info when privacy is enabled
	            String currColName 		= "ACL"+attrName;
	            
	            // if oldValJSON has the ACLAttName column then it means
	            // that it has the ACL info stored and we don't need to update.
	            // as ACL info is never updated. It is either deleted or inserted.
	            // because change in ACL will require re-computation of anonymized ID itself. 
	            if(!oldValJSON.has(currColName))
	            {
					JSONArray realIDMappingArray = null;
	                if( ContextServiceConfig.PRIVACY_ENABLED )
	                {
	                	realIDMappingArray = attrValRep.getRealIDMappingInfo();
	                	
	                	if( realIDMappingArray != null )
	                	{
	                		String jsonArrayString  = realIDMappingArray.toString();
	                		updateSqlQuery = updateSqlQuery +" , "+ currColName +" = '"+jsonArrayString+"'";
	                	}
	                }
	            }
                
	            i++;
	        }
	        
	        //selectQuery = selectQuery + " FROM "+tableName+" WHERE nodeGUID = '"+nodeGUID+"'";
	        updateSqlQuery = updateSqlQuery + " WHERE nodeGUID = X'"+nodeGUID+"'";
            
            myConn = this.dataSource.getConnection();
            stmt = myConn.createStatement();
            
        	// if update fails then insert
    		//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE "+updateSqlQuery);
    		long start   = System.currentTimeMillis();
        	int rowCount = stmt.executeUpdate(updateSqlQuery);
        	long end     = System.currentTimeMillis();
        	
        	if(ContextServiceConfig.DEBUG_MODE)
        	{
        		System.out.println("TIME_DEBUG: performStoreGUIDInPrimarySubspaceUpdate "
        						+(end-start));
        	}
        	
        	//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE rowCount "+rowCount);
        	// update failed try insert
        	if(rowCount == 0)
        	{
        		// should not happen, rowCount should always be 1
        		assert(false);
        	}
        } catch ( Exception  | Error ex )
        {
            ex.printStackTrace();
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
	}
	
	private void performStoreGUIDInPrimarySubspaceInsert( String tableName, String nodeGUID, 
    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep )
	{
		ContextServiceLogger.getLogger().fine("performStoreGUIDInPrimarySubspaceInsert "
				+tableName+" nodeGUID "+nodeGUID );
    	
        Connection myConn          = null;
        Statement stmt         	   = null;
        
        // delayed insert performs better than just insert
        String insertQuery         = "INSERT INTO "+tableName+ " (";
        
        try
        {
        	// insert happens for all attributes,
        	// updated attributes are taken from atrToValueRep and
        	// other attributes are taken from oldValJSON
        	//Iterator<Integer> subapceIdIter = subspaceInfoMap.keySet().iterator();
        	Iterator<String> attrIter = atrToValueRep.keySet().iterator();
        	
        	boolean first = true;
        	
        	// just a way to iterate over attributes.
    		while( attrIter.hasNext() )
    		{
				String currAttrName = attrIter.next();
				
				if(first)
	            {
	                insertQuery = insertQuery + currAttrName;
	                first = false;
	            }
	            else
	            {
	                insertQuery = insertQuery +", "+currAttrName;
	            }
				
				JSONArray realIDMappingArray = null;
                if( ContextServiceConfig.PRIVACY_ENABLED )
                {
                	AttrValueRepresentationJSON attrValRep 
                						= atrToValueRep.get(currAttrName);
                	
                	realIDMappingArray = attrValRep.getRealIDMappingInfo();
                	
                	if( realIDMappingArray != null )
                	{
                		String currColName 		= "ACL"+currAttrName;
                		//String jsonArrayString  = realIDMappingArray.toString();
                		insertQuery = insertQuery +" , "+currColName;
                		//updateSqlQuery = updateSqlQuery +" , "+ currColName +" = '"+jsonArrayString+"'";
                	}
                }
    		}
    		
    		insertQuery = insertQuery + ", nodeGUID) " + "VALUES"+ "(";
    		
    		
    		first = true;
    		attrIter = atrToValueRep.keySet().iterator();
        	// just a way to iterate over attributes.
    			
			while( attrIter.hasNext() )
			{
				String currAttrName = attrIter.next();
				String currAttrValue = "";
				
				AttrValueRepresentationJSON attrValRep 
									= atrToValueRep.get(currAttrName);

				currAttrValue = attrValRep.getActualAttrValue();
				
				
				AttributeMetaInfo attrMetaInfo 
							= AttributeTypes.attributeMap.get(currAttrName);
				
			    String dataType = attrMetaInfo.getDataType();
				
			    currAttrValue = AttributeTypes.convertStringToDataTypeForMySQL
						(currAttrValue, dataType)+"";
			    
				if(first)
	            {
					insertQuery = insertQuery + currAttrValue;
	                first = false;
	            }
	            else
	            {
	            	insertQuery = insertQuery +" , "+currAttrValue;
	            }
				
				JSONArray realIDMappingArray = null;
                if( ContextServiceConfig.PRIVACY_ENABLED )
                {        	
                	realIDMappingArray = attrValRep.getRealIDMappingInfo();
                	
                	if( realIDMappingArray != null )
                	{
                		insertQuery = insertQuery +" , '"+realIDMappingArray.toString()+"'";
                	}
                }
			}
    		
    		insertQuery = insertQuery +" , X'"+nodeGUID+"' )";
    		
    		myConn = this.dataSource.getConnection();
            stmt = myConn.createStatement();  
            
    		ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING INSERT "+insertQuery);
    		long start   = System.currentTimeMillis();
    		int rowCount = stmt.executeUpdate(insertQuery);
    		long end     = System.currentTimeMillis();
    		
    		if( ContextServiceConfig.DEBUG_MODE )
        	{
    			System.out.println("TIME_DEBUG: performStoreGUIDInPrimarySubspaceInsert "
        															+(end-start) );
        	}
    		
    		ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING INSERT rowCount "+rowCount
    					+" insertQuery "+insertQuery);	
        }
        catch ( Exception  | Error ex )
        {
        	ex.printStackTrace();
        }
        finally
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
	}
	
	
	private String getDataStorageString(String newTableCommand)
	{
		Iterator<Integer> subapceIdIter = subspaceInfoMap.keySet().iterator();
		
		while( subapceIdIter.hasNext() )
		{
			int subspaceId = subapceIdIter.next();
			// at least one replica and all replica have same default value for each attribute.
			SubspaceInfo<NodeIDType> currSubspaceInfo 
												= subspaceInfoMap.get(subspaceId).get(0);
			HashMap<String, AttributePartitionInfo> attrSubspaceMap 
												= currSubspaceInfo.getAttributesOfSubspace();
			
			Iterator<String> attrIter 			= attrSubspaceMap.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String attrName 				= attrIter.next();
				AttributePartitionInfo attrPartInfo 
												= attrSubspaceMap.get(attrName);
				AttributeMetaInfo attrMetaInfo 	  
												= attrPartInfo.getAttrMetaInfo();
				String dataType 				= attrMetaInfo.getDataType();
				String defaultVal 				= attrPartInfo.getDefaultValue();
				String mySQLDataType 			= AttributeTypes.mySQLDataType.get(dataType);
				
				
				newTableCommand = newTableCommand + ", "+attrName+" "+mySQLDataType
						+" DEFAULT "+AttributeTypes.convertStringToDataTypeForMySQL(defaultVal, dataType)
						+" , INDEX USING BTREE("+attrName+")";
				
				// FIXME: to be removed later on
//				if( ContextServiceConfig.PRIVACY_ENABLED )
//				{
//					//ACL<attrName> is ACL info for each attribute needed in privacy scheme.
//					// not sure if BLOB is right datatype. ALso not sure if this is most optimized way to do this.
//					// ACL info can be very large, CS can parse each element of JSONArray and store it row wise in a separate table.
//					// but that will require CS parsing, which is not needed for CS for any other purpose and 
//					// another drawback is after result Anonymized IDs are computed then there is another database 
//					// lookup to fetch ACL info of all those Anonymized IDs.
//					// So doing it like this now and see the performance.
//					// Benefit of this approach is that everything can be selected in one select query.
//					
//					newTableCommand 
//						= newTableCommand + ", "+attrName+" "+mySQLDataType+" DEFAULT "+AttributeTypes.convertStringToDataTypeForMySQL(defaultVal, dataType)
//						+" , INDEX USING BTREE("+attrName+") , ACL"+attrName+" BLOB";
//				}
//				else
//				{
//				}
			}
		}
		return newTableCommand;
	}
	
	
	private String getPrivacyStorageString( String newTableCommand )
	{
		Iterator<Integer> subapceIdIter = subspaceInfoMap.keySet().iterator();
		
		while( subapceIdIter.hasNext() )
		{
			int subspaceId = subapceIdIter.next();
			// at least one replica and all replica have same default value for each attribute.
			SubspaceInfo<NodeIDType> currSubspaceInfo = subspaceInfoMap.get(subspaceId).get(0);
			HashMap<String, AttributePartitionInfo> attrSubspaceMap 
													= currSubspaceInfo.getAttributesOfSubspace();
			
			Iterator<String> attrIter = attrSubspaceMap.keySet().iterator();
			
			while( attrIter.hasNext() )
			{
				String attrName = attrIter.next();
				//AttributePartitionInfo attrPartInfo = attrSubspaceMap.get(attrName);
				//AttributeMetaInfo attrMetaInfo = attrPartInfo.getAttrMetaInfo();
				//String dataType = attrMetaInfo.getDataType();
				//String defaultVal = attrPartInfo.getDefaultValue();
				//String mySQLDataType = AttributeTypes.mySQLDataType.get(dataType);
				
				
//				newTableCommand = newTableCommand + ", "+attrName+" "+mySQLDataType
//						+" DEFAULT "+AttributeTypes.convertStringToDataTypeForMySQL
//				(defaultVal, dataType)
//						+" , INDEX USING BTREE("+attrName+")";
				
				if( ContextServiceConfig.PRIVACY_ENABLED )
				{
					//ACL<attrName> is ACL info for each attribute needed in privacy scheme.
					// using TEXT datatype and will check how much performance of getGUIDFromPrimarySubspace is affected.
					// not sure if TEXT is right datatype. ALso not sure if this is most optimized way to do this.
					// this column stores JSONArray are String.
					
					newTableCommand = newTableCommand + " , ACL"+attrName+" TEXT";
				}
			}
		}
		return newTableCommand;
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
	
	
	// old storeGUIDInSubpace will be removed after overhaul of storeGUIDInSubspace fun
	/**
     * Stores GUID in a subspace. The decision to store a guid on this node
     * in this subspace is not made in this function.
     * @param subspaceNum
     * @param nodeGUID
     * @param attrValuePairs
     * @return
     * @throws JSONException
     */
//    public void storeGUIDInSubspace(String tableName, String nodeGUID, 
//    		HashMap<String, AttrValueRepresentationJSON> atrToValueRep, int updateOrInsert
//    		, JSONObject oldValJSON ) throws JSONException
//    {
//    	ContextServiceLogger.getLogger().fine("storeGUIDInSubspace "+tableName+" nodeGUID "+nodeGUID+
//    			" updateOrInsert "+updateOrInsert);
//    	
//        long t0 = System.currentTimeMillis();
//        Connection myConn      = null;
//        Statement stmt         = null;
//        
//        String updateSqlQuery     	= "UPDATE "+tableName
//                + " SET ";
//       
//        // delayed insert performs better than just insert
//        String insertQuery         = "INSERT INTO "+tableName+ " (";
//        
//        //JSONObject oldValueJSON = new JSONObject();
//        try
//        {
//        	Iterator<String> attrNameIter = atrToValueRep.keySet().iterator();
//        	int i=0;
//	        while( attrNameIter.hasNext() )
//	        {
//	            String attrName = attrNameIter.next();  
//	            AttrValueRepresentationJSON attrValRep 
//	            		= atrToValueRep.get(attrName);
//	            
//	            String newVal   = attrValRep.getActualAttrValue();
//	            
//	            AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
//				assert(attrMetaInfo != null);
//	            String dataType = attrMetaInfo.getDataType();
//				
//				newVal = AttributeTypes.convertStringToDataTypeForMySQL
//						(newVal, dataType)+"";	
//				
//	            //oldValueJSON.put(attrName, AttributeTypes.NOT_SET);
//	           
//	            if(i == 0)
//	            {
//	                //selectQuery = selectQuery + attrName;
//	                updateSqlQuery = updateSqlQuery + attrName +" = "+newVal;
//	                insertQuery = insertQuery + attrName;
//	            }
//	            else
//	            {
//	                //selectQuery = selectQuery + ", "+attrName+" ";
//	                updateSqlQuery = updateSqlQuery +" , "+ attrName +" = "+newVal;
//	                insertQuery = insertQuery +", "+attrName;
//	            }
//	            i++;
//	        }
//	        
//	        //selectQuery = selectQuery + " FROM "+tableName+" WHERE nodeGUID = '"+nodeGUID+"'";
//	        updateSqlQuery = updateSqlQuery + " WHERE nodeGUID = X'"+nodeGUID+"'";
//	        insertQuery = insertQuery + ", nodeGUID) " + "VALUES"+ "(";
//            //+ ",'"+nodeGUID+"' )
//	        //double oldValue = Double.MIN_VALUE;
//	        
//	        
//            i = 0;
//            //try insert, if fails then update
//            attrNameIter = atrToValueRep.keySet().iterator();
//            while( attrNameIter.hasNext() )
//            {
//                String attrName = attrNameIter.next();
//                AttrValueRepresentationJSON attrValRep 
//        							= atrToValueRep.get(attrName);
//                
//                String newValue = attrValRep.getActualAttrValue();
//                
//                AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
//    			
//                String dataType = attrMetaInfo.getDataType();
//    			
//                newValue = AttributeTypes.convertStringToDataTypeForMySQL
//    					(newValue, dataType)+"";
//               
//                if(i == 0)
//                {
//                    insertQuery = insertQuery + newValue;
//                }
//                else
//                {
//                    insertQuery = insertQuery +" , "+newValue;
//                }
//                
////                if(ContextServiceConfig.PRIVACY_ENABLED &&  
////                		(attrValRep.getRealIDMappingInfo() != null) )
////                {
////                	JSONArray encryptedRealIDArray = attrValRep.getRealIDMappingInfo();
////                	
////            		if( encryptedRealIDArray.length() > 0 )
////            		{
////            			String hexRep 
////            				= Utils.bytArrayToHex( encryptedRealIDArray.toString().getBytes() );
////            			// now add it in the query    	                
////    	                insertQuery = insertQuery +" , X'"+hexRep+"'";
////            		}
////                }    
//                i++;
//            }
//            insertQuery = insertQuery +" , X'"+nodeGUID+"' )";
//            
//            myConn = this.dataSource.getConnection();
//            stmt = myConn.createStatement();   
//            
//            if( updateOrInsert == HyperspaceMySQLDB.UPDATE_REC )
//            {
//            	// if update fails then insert
//            	try
//                {
//            		//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE "+updateSqlQuery);
//            		long start   = System.currentTimeMillis();
//                	int rowCount = stmt.executeUpdate(updateSqlQuery);
//                	long end     = System.currentTimeMillis();
//                	
//                	if(ContextServiceConfig.DEBUG_MODE)
//                	{
//                		System.out.println("TIME_DEBUG: storeGUIDInSubspace update "+(end-start));
//                	}
//                	
//                	//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE rowCount "+rowCount);
//                	// update failed try insert
//                	if(rowCount == 0)
//                	{
//                		//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE EXCP "+insertQuery);
//                		start   = System.currentTimeMillis();
//                		rowCount = stmt.executeUpdate(insertQuery);
//                		end     = System.currentTimeMillis();
//                		
//                		if(ContextServiceConfig.DEBUG_MODE)
//                    	{
//                    		System.out.println("TIME_DEBUG: storeGUIDInSubspace insert "+(end-start));
//                    	}
//                		//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE EXCP rowCount "+rowCount);
//                	}
//                } catch(SQLException sqlEx)
//                {
//                	try
//                	{
//	                	//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE EXCP "+insertQuery);
//                		long start   = System.currentTimeMillis();
//                		int rowCount = stmt.executeUpdate(insertQuery);
//                		long end     = System.currentTimeMillis();
//                		if(ContextServiceConfig.DEBUG_MODE)
//                    	{
//                    		System.out.println("TIME_DEBUG: storeGUIDInSubspace insert "+(end-start));
//                    	}
//                		//ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING UPDATE EXCP rowCount "+rowCount);
//                	}
//                	// insert failed because of another insert, which caused primary key violation
//                	catch( SQLException sqlEx2 )
//                	{
//                		long start   = System.currentTimeMillis();
//                		int rowCount = stmt.executeUpdate(updateSqlQuery);
//                		long end     = System.currentTimeMillis();
//                		
//                		if(ContextServiceConfig.DEBUG_MODE)
//                    	{
//                    		System.out.println("TIME_DEBUG: storeGUIDInSubspace update "+(end-start));
//                    	}
//                	}
//                }
//            }
//            else if(updateOrInsert == HyperspaceMySQLDB.INSERT_REC)
//            {
//            	try
//                {
//            		ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING INSERT "+insertQuery);
//            		long start   = System.currentTimeMillis();
//            		int rowCount = stmt.executeUpdate(insertQuery);
//            		long end     = System.currentTimeMillis();
//            		
//            		if(ContextServiceConfig.DEBUG_MODE)
//                	{
//                		System.out.println("TIME_DEBUG: storeGUIDInSubspace insert "+(end-start));
//                	}
//            		
//            		ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING INSERT rowCount "+rowCount+" insertQuery "+insertQuery);
//            		// duplicate insert always gives exception so no need to check rowCount and do update
//            		// it happends in exception code
//                } catch( SQLException sqlEx )
//                {
//                	// ContextServiceLogger.getLogger().fine("EXECUTING INSERT "+updateSqlQuery);
//                	long start   = System.currentTimeMillis();
//                	int rowCount = stmt.executeUpdate(updateSqlQuery);
//                	long end     = System.currentTimeMillis();
//                	
//                	if(ContextServiceConfig.DEBUG_MODE)
//                	{
//                		System.out.println("TIME_DEBUG: storeGUIDInSubspace update "+(end-start));
//                	}
//                	// ContextServiceLogger.getLogger().fine("EXECUTING INSERT rowCount "+rowCount);
//                }
//            }
//            // execute insert SQL statement
//            // statement.executeUpdate(sqlQuery);
//        } catch ( Exception  | Error ex )
//        {
//            ex.printStackTrace();
//        } finally
//        {
//            try
//            {
//                if ( stmt != null )
//                    stmt.close();
//                
//                if ( myConn != null )
//                    myConn.close();
//            }
//            catch(SQLException e)
//            {
//            	e.printStackTrace();
//            }
//        }
//        
//        if( ContextServiceConfig.DELAY_PROFILER_ON )
//        {
//            DelayProfiler.updateDelay("StoreGUIDInSubspace", t0);
//        }
//    }
}