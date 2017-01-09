package edu.umass.cs.contextservice.database.guidattributes;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.config.ContextServiceConfig.SQL_DB_TYPE;
import edu.umass.cs.contextservice.database.RegionMappingDataStorageDB;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource.DB_REQUEST_TYPE;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.dataformat.SearchReplyGUIDRepresentationJSON;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.utils.Utils;

/**
 * This class implements GUIDAttributeStorageInterface.
 * This class has moethids for table creation, updates and searches of 
 * context service.
 * @author adipc
 */
public class SQLGUIDStorage implements GUIDStorageInterface
{
	private final int myNodeID;
	//private final HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap;
	private final AbstractDataSource dataSource;
	
	public SQLGUIDStorage( int myNodeID, AbstractDataSource dataSource )
	{
		this.myNodeID = myNodeID;
		this.dataSource = dataSource;
	}
	
	@Override
	public void createDataStorageTables() 
	{
		Connection myConn  = null;
		Statement  stmt    = null;	
		try
		{
			myConn = dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt   = myConn.createStatement();
			
			//TODO: which indexing scheme is better, indexing two attribute once or 
			// creating a index over all attributes datastorage table of each subspace
			String tableName = RegionMappingDataStorageDB.ATTR_INDEX_TABLE_NAME;
			
			String newTableCommand = "create table "+tableName+" ( "
				      + " nodeGUID Binary(20) PRIMARY KEY";
			
			newTableCommand = getDataStorageString(newTableCommand);
			
			if(ContextServiceConfig.PRIVACY_ENABLED)
			{
				newTableCommand = getPrivacyStorageString(newTableCommand);
				// row format dynamic because we want TEXT columns to be stored completely off the row, 
				// only pointer should be stored in the row, otherwise default is storing 700 bytes for 
				// each TEXT in row.
				
				// sqlite doesn't support dynamic rows as it is an in-memory db.
				if( ContextServiceConfig.sqlDBType == SQL_DB_TYPE.SQLITE )
				{
					newTableCommand = newTableCommand +" )";
				}
				else
				{
					if(!ContextServiceConfig.IN_MEMORY_MYSQL)
					{
						newTableCommand = newTableCommand +" ) ROW_FORMAT=DYNAMIC";
					}
					else
					{
						newTableCommand = newTableCommand +" )";
					}
				}
			}
			else
			{
				newTableCommand = newTableCommand +" ) ";
			}
			
			if( (ContextServiceConfig.sqlDBType == SQL_DB_TYPE.MYSQL) 
									&& (ContextServiceConfig.IN_MEMORY_MYSQL) )
			{
				newTableCommand = newTableCommand +" ENGINE = MEMORY";
				
			}
			System.out.println("newTableCommand "+newTableCommand);
			stmt.executeUpdate(newTableCommand);
			
			
			tableName = RegionMappingDataStorageDB.GUID_HASH_TABLE_NAME;
			newTableCommand = "create table "+tableName+" ( "
				      + " nodeGUID Binary(20) PRIMARY KEY";
			
			newTableCommand = getDataStorageString(newTableCommand);
			
			newTableCommand = newTableCommand + " , "+RegionMappingDataStorageDB.unsetAttrsColName
						+" VARCHAR("+RegionMappingDataStorageDB.varcharSizeForunsetAttrsCol+") ";
			
			//FIXME: need to fix this for MySQL and SQLite cases.
			if( ContextServiceConfig.PRIVACY_ENABLED )
			{
				newTableCommand = getPrivacyStorageString(newTableCommand);
				//newTableCommand	= getPrivacyStorageString(newTableCommand);
				// row format dynamic because we want TEXT columns to be stored 
				// completely off the row, only pointer should be stored in the row, 
				// otherwise default is storing 700 bytes for each TEXT in row.
				
				if( ContextServiceConfig.sqlDBType == SQL_DB_TYPE.SQLITE )
				{
					newTableCommand = newTableCommand +" )";
				}
				else
				{
					if(!ContextServiceConfig.IN_MEMORY_MYSQL)
					{
						newTableCommand = newTableCommand +" ) ROW_FORMAT=DYNAMIC";
					}
					else
					{
						newTableCommand = newTableCommand +" )";
					}
				}
			}
			else
			{
				newTableCommand = newTableCommand +" )";
			}
			
			if( (ContextServiceConfig.sqlDBType == SQL_DB_TYPE.MYSQL) 
					&& (ContextServiceConfig.IN_MEMORY_MYSQL) )
			{
				newTableCommand = newTableCommand +" ENGINE = MEMORY";
			}
			
			stmt.executeUpdate(newTableCommand);
		}
		catch( SQLException mysqlEx )
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
	
	
//	private void createIndexesInSQLite(String tableName, 
//			HashMap<String, AttributePartitionInfo> subspaceAttributes, Statement  stmt)
//	{
//		Iterator<String> attrIter = subspaceAttributes.keySet().iterator();
//		
//		while( attrIter.hasNext() )
//		{
//			String attrName = attrIter.next();
//			
//			String indexName = tableName+attrName+"Index";
//			
//			String cmd = "CREATE INDEX "+indexName+" ON "+tableName+" ("+attrName+")";
//		}
//	}
	
	public int processSearchQueryUsingAttrIndex
			(HashMap<String, AttributeValueRange> queryAttrValRange, 
					JSONArray resultArray)
	{	
		String mysqlQuery = getMySQLQueryForProcessingSearchQuery(queryAttrValRange);
		
		assert(mysqlQuery != null);
		
		Connection myConn  = null;
		Statement stmt     = null;
		
		int resultSize = 0;
		try
		{
			myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.SELECT);
			// for row by row fetching, otherwise default is fetching whole result
			// set in memory. 
			// http://dev.mysql.com/doc/connector-j/en/connector-j-reference-implementation-notes.html
			if( ContextServiceConfig.rowByRowFetchingEnabled )
			{
				stmt   = myConn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, 
					java.sql.ResultSet.CONCUR_READ_ONLY);
				stmt.setFetchSize(ContextServiceConfig.MYSQL_CURSOR_FETCH_SIZE);
			}
			else
			{
				// fetches all result in memory once
				stmt   = myConn.createStatement();
			}
			
			ContextServiceLogger.getLogger().fine("processSearchQueryInSubspaceRegion: "
								+mysqlQuery);
			
			ResultSet rs = stmt.executeQuery(mysqlQuery);
			while( rs.next() )
			{	
				// it is actually a JSONArray in hexformat byte array representation.
				// reverse conversion is byte array to String and then string to JSONArray.
				// byte[] realIDEncryptedArray = rs.getBytes(ACLattr);
				// ValueTableInfo valobj = new ValueTableInfo(value, nodeGUID);
				// answerList.add(valobj);
				if(ContextServiceConfig.sendFullRepliesWithinCS)
				{
					byte[] nodeGUIDBytes = rs.getBytes("nodeGUID");
					
					String nodeGUID = Utils.byteArrayToHex(nodeGUIDBytes);
					
					//String anonymizedIDToGUIDMapping = null;
					JSONArray anonymizedIDToGuidArray = null;
					if( ContextServiceConfig.PRIVACY_ENABLED )
					{
						byte[] anonymizedIDToGUIDMappingBA 
							= rs.getBytes(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName);
						
						if(anonymizedIDToGUIDMappingBA != null)
						{
							if(anonymizedIDToGUIDMappingBA.length > 0)
							{
								anonymizedIDToGuidArray 
									= this.deserializeByteArrayToAnonymizedIDJSONArray
										(anonymizedIDToGUIDMappingBA);
							}
						}
					}
					
					if(anonymizedIDToGuidArray != null)
					{
						SearchReplyGUIDRepresentationJSON searchReplyRep 
							= new SearchReplyGUIDRepresentationJSON(nodeGUID, 
									anonymizedIDToGuidArray);
					
						resultArray.put(searchReplyRep.toJSONObject());
					}
					else
					{
						SearchReplyGUIDRepresentationJSON searchReplyRep 
							= new SearchReplyGUIDRepresentationJSON(nodeGUID);
				
						resultArray.put(searchReplyRep.toJSONObject());
					}
					
					resultSize++;
				}
				else
				{
					if(ContextServiceConfig.onlyResultCountEnable)
					{
						resultSize = rs.getInt("RESULT_SIZE");
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
		
		return resultSize;
	}
	
	
	public JSONObject getGUIDStoredUsingHashIndex( String guid )
	{
		Connection myConn 		= null;
		Statement stmt 			= null;
		
		String selectQuery 		= "SELECT * ";
		String tableName 		= RegionMappingDataStorageDB.GUID_HASH_TABLE_NAME;
		
		JSONObject oldValueJSON = new JSONObject();
		
		selectQuery = selectQuery + " FROM "+tableName+" WHERE nodeGUID = X'"+guid+"'";
		
		try
		{
			myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.SELECT);
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
					
					// doing translation here saves multiple strng to JSON translations later in code.
					if(colName.equals(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName))
					{
						byte[] colValBA = rs.getBytes(colName);
						
						if(colValBA != null)
						{
							if(colValBA.length > 0)
							{
								try
								{
									JSONArray jsonArr 
										= this.deserializeByteArrayToAnonymizedIDJSONArray(colValBA);
									oldValueJSON.put(colName, new JSONArray(jsonArr));
								} catch (JSONException e) 
								{
									e.printStackTrace();
								}
							}
						}
					} else if(colName.equals(RegionMappingDataStorageDB.unsetAttrsColName))
					{
						String colVal = rs.getString(colName);
						try
						{
							oldValueJSON.put(colName, new JSONObject(colVal));
						} catch (JSONException e) 
						{
							e.printStackTrace();
						}
					}
					else
					{
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
			}
			rs.close();	
			long end = System.currentTimeMillis();
			if(ContextServiceConfig.DEBUG_MODE)
			{
				System.out.println("TIME_DEBUG: getGUIDStoredInPrimarySubspace "
										+(end-start));
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
		
		return oldValueJSON;
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
    public void storeGUIDUsingHashIndex( String nodeGUID, 
    		JSONObject jsonToWrite, int updateOrInsert ) throws JSONException
    {
    	if( updateOrInsert == RegionMappingDataStorageDB.INSERT_REC )
    	{
    		this.performStoreGUIDInPrimarySubspaceInsert
    			(nodeGUID, jsonToWrite);
    	}
    	else if( updateOrInsert == RegionMappingDataStorageDB.UPDATE_REC )
    	{
    		this.performStoreGUIDInPrimarySubspaceUpdate
    				(nodeGUID, jsonToWrite);
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
    public void storeGUIDUsingAttrIndex( String tableName, String nodeGUID, 
    		JSONObject updatedAttrValJSON, int updateOrInsert ) throws JSONException
    {
    	long start = System.currentTimeMillis();
    	if(ContextServiceConfig.DEBUG_MODE)
    	{
    		ContextServiceLogger.getLogger().fine("STARTED storeGUIDInSecondarySubspace "+nodeGUID);
    	}
    	
    	if( updateOrInsert == RegionMappingDataStorageDB.INSERT_REC )
    	{
    		this.performStoreGUIDInSecondarySubspaceInsert
    			(tableName, nodeGUID, updatedAttrValJSON);
    		
    		if(ContextServiceConfig.DEBUG_MODE)
        	{
        		long end = System.currentTimeMillis();
        		ContextServiceLogger.getLogger().fine
        		("FINISHED storeGUIDInSecondarySubspace Insert "+nodeGUID
        				+" time "+(end-start));
        	}	
    	}
    	else if( updateOrInsert == RegionMappingDataStorageDB.UPDATE_REC )
    	{
    		this.performStoreGUIDInSecondarySubspaceUpdate
    				( tableName, nodeGUID, updatedAttrValJSON );
    		
    		if(ContextServiceConfig.DEBUG_MODE)
        	{
        		long end = System.currentTimeMillis();
        		ContextServiceLogger.getLogger().fine
        		("FINISHED storeGUIDInSecondarySubspace Update "+nodeGUID
        				+" time "+(end-start));
        	}
    	}
    }
    
    public void deleteGUIDFromTable(String tableName, String nodeGUID)
	{
		String deleteCommand = "DELETE FROM "+tableName+" WHERE nodeGUID= X'"+nodeGUID+"'";
		Connection myConn 	= null;
		Statement stmt 		= null;
		
		try
		{
			myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt = myConn.createStatement();
			long start = System.currentTimeMillis();
			stmt.executeUpdate(deleteCommand);
			long end = System.currentTimeMillis();
			
			if( ContextServiceConfig.DEBUG_MODE )
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
	private String getMySQLQueryForProcessingSearchQuery( HashMap<String, AttributeValueRange> 
			queryAttrValSpace )
	{		
		String tableName = RegionMappingDataStorageDB.ATTR_INDEX_TABLE_NAME;
		String mysqlQuery = "";	
		
		// if privacy is enabled then we also fetch 
		// anonymizedIDToGuidMapping set.
		if(ContextServiceConfig.PRIVACY_ENABLED)
		{
			mysqlQuery = "SELECT nodeGUID , "+RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName
					+" from "+tableName+" WHERE ( ";
		}
		else
		{
			if(ContextServiceConfig.onlyResultCountEnable)
			{
				mysqlQuery = "SELECT COUNT(nodeGUID) AS RESULT_SIZE from "+tableName+" WHERE ( ";
			}
			else
			{
				mysqlQuery = "SELECT nodeGUID from "+tableName+" WHERE ( ";
			}
		}
		
		Iterator<String> attrIter = queryAttrValSpace.keySet().iterator();
		
		int counter = 0;
		try
		{
			while(attrIter.hasNext())
			{
				String attrName = attrIter.next();
				//ProcessingQueryComponent pqc = queryComponents.get(attrName);
				AttributeValueRange attrValRange = queryAttrValSpace.get(attrName);
				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
				
				assert(attrMetaInfo != null);
				
				String dataType = attrMetaInfo.getDataType();
				
				// normal case of lower value being lesser than the upper value
				if(AttributeTypes.compareTwoValues(attrValRange.getLowerBound(), 
												attrValRange.getUpperBound(), dataType))
				{
					String queryMin  
						= AttributeTypes.convertStringToDataTypeForMySQL
											(attrValRange.getLowerBound(), dataType)+"";
					String queryMax  
						= AttributeTypes.convertStringToDataTypeForMySQL
											(attrValRange.getUpperBound(), dataType)+"";
					
					if( counter == (queryAttrValSpace.size()-1) )
					{
						// it is assumed that the strings in query(pqc.getLowerBound() or pqc.getUpperBound()) 
						// will have single or double quotes in them so we don't need to them separately in mysql query
						mysqlQuery = mysqlQuery + " ( "+attrName +" >= "+queryMin +" AND " 
								+attrName +" <= "+queryMax+" ) )";
					}
					else
					{
						mysqlQuery = mysqlQuery + " ( "+attrName +" >= "+queryMin +" AND " 
												+ attrName +" <= "+queryMax+" ) AND ";
					}
				}
				else
				{
					if(counter == (queryAttrValSpace.size()-1) )
					{
						String queryMin  
							= AttributeTypes.convertStringToDataTypeForMySQL
											(attrMetaInfo.getMinValue(), dataType)+"";
						String queryMax  
							= AttributeTypes.convertStringToDataTypeForMySQL
											(attrValRange.getUpperBound(), dataType)+"";
						
						mysqlQuery = mysqlQuery + " ( "
								+" ( "+attrName +" >= "+queryMin +" AND " 
								+ attrName +" <= "+queryMax+" ) OR ";
								
						queryMin  
							= AttributeTypes.convertStringToDataTypeForMySQL
													(attrValRange.getLowerBound(), dataType)+"";
						queryMax  
							= AttributeTypes.convertStringToDataTypeForMySQL
													(attrMetaInfo.getMaxValue(), dataType)+"";
						
						mysqlQuery = mysqlQuery +" ( "+attrName +" >= "+queryMin +" AND " 
								+ attrName +" <= "+queryMax+" ) ) )";
					}
					else
					{
						String queryMin  
							= AttributeTypes.convertStringToDataTypeForMySQL
												(attrMetaInfo.getMinValue(), dataType)+"";
						String queryMax  
							= AttributeTypes.convertStringToDataTypeForMySQL
												(attrValRange.getUpperBound(), dataType)+"";
						
						mysqlQuery = mysqlQuery + " ( "
								+ " ( "+attrName +" >= "+queryMin +" AND " 
								+ attrName +" <= "+queryMax+" ) OR ";
								
						queryMin  
							= AttributeTypes.convertStringToDataTypeForMySQL
										(attrValRange.getLowerBound(), dataType)+"";
						queryMax  
							= AttributeTypes.convertStringToDataTypeForMySQL
										(attrMetaInfo.getMaxValue(), dataType)+"";
						
						mysqlQuery = mysqlQuery +" ( "+attrName +" >= "+queryMin +" AND " 
								+attrName +" <= "+queryMax+" ) ) AND ";
					}
				}
				
				counter++;
			}
			return mysqlQuery;
		} catch(Exception | Error ex)
		{
			ex.printStackTrace();
		}
		return null;
	}
	
    
	/**
	 * only need to update attributes in atToValRep,
	 *  as other attributes are already there.
	 * @param tableName
	 * @param nodeGUID
	 * @param atrToValueRep
	 */
	private void performStoreGUIDInSecondarySubspaceUpdate( 
			String tableName, String nodeGUID, JSONObject toWriteJSON )
	{
		ContextServiceLogger.getLogger().fine("STARTED "
				+ " performStoreGUIDInSecondarySubspaceUpdate "+tableName
				+ " nodeGUID "+nodeGUID);
    	
        Connection myConn      			= null;
        Statement stmt         			= null;
        
        String updateSqlQuery     		= "UPDATE "+tableName
                + " SET ";
        
        try
        {
        	@SuppressWarnings("unchecked")
			Iterator<String> colIter 	= toWriteJSON.keys();
        	int i = 0;
	        while( colIter.hasNext() )
	        {
	            String colName = colIter.next();  
	            //AttrValueRepresentationJSON attrValRep 
	            //		= atrToValueRep.get(attrName);
	            String colValue = "";
	            
	            if( colName.equals(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName) )
	            {
	            	JSONArray jsonArr  = toWriteJSON.getJSONArray
	            					(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName);
	            	
	            	assert( jsonArr.length() > 0 );
	            	
	            	byte[] fullBArray = this.serializeAnonymizedIDJSONArrayToByteArray(jsonArr);
	            	
	            	colValue = "X'"+Utils.byteArrayToHex(fullBArray)+"'";
	            }
	            else
	            {
	            	String newVal   = toWriteJSON.getString(colName);
		            
		            AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(colName);
					assert(attrMetaInfo != null);
		            String dataType = attrMetaInfo.getDataType();
					
		            colValue = AttributeTypes.convertStringToDataTypeForMySQL
							(newVal, dataType)+"";	
	            }
	            
				
	            //oldValueJSON.put(attrName, AttributeTypes.NOT_SET);
	           
	            if(i == 0)
	            {
	                updateSqlQuery = updateSqlQuery + colName +" = "+colValue;
	            }
	            else
	            {
	                updateSqlQuery = updateSqlQuery +" , "+ colName +" = "+colValue;
	            }
	            i++;
	        }
	        
	        updateSqlQuery = updateSqlQuery + " WHERE nodeGUID = X'"+nodeGUID+"'";
            
            myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
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
	
	@SuppressWarnings("unchecked")
	private void performStoreGUIDInSecondarySubspaceInsert( String tableName, String nodeGUID, 
    		JSONObject toWriteJSON )
	{
		long s1 = System.currentTimeMillis();
		ContextServiceLogger.getLogger().fine( "STARTED performStoreGUIDInSubspaceInsert "
				+tableName+" nodeGUID "+nodeGUID );
    	
        Connection myConn      	   = null;
        Statement stmt         	   = null;
        
        String insertQuery         = "INSERT INTO "+tableName+ " (";
        
        try
        {
        	// insert happens for all attributes,
        	// updated attributes are taken from atrToValueRep and
        	// other attributes are taken from oldValJSON
			Iterator<String> colIter = toWriteJSON.keys();
    		
        	boolean first = true;
        	// just a way to iterate over attributes.
    		while( colIter.hasNext() )
    		{
    			String colName = colIter.next();
//    			String currAttrName = attrIter.next();
				if(first)
	            {
	                insertQuery = insertQuery + colName;
	                first = false;
	            }
	            else
	            {
	                insertQuery = insertQuery +", "+colName;
	            }
    		}
    		insertQuery = insertQuery + ", nodeGUID) " + "VALUES"+ "(";
    		
    		first = true;
    		colIter = toWriteJSON.keys();
    		long start1 = System.currentTimeMillis();
        	// just a way to iterate over attributes.
    		while( colIter.hasNext() )
    		{
    			String colName = colIter.next();
    			// at least one replica and all replica have same default value for each attribute.
    			
				String colValue = "";
				
				if(colName.equals(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName))
				{
					JSONArray jsonArr = toWriteJSON.getJSONArray
										(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName);
					assert(jsonArr != null);
					assert( jsonArr.length() > 0 );
	            	
	            	byte[] fullBArray = this.serializeAnonymizedIDJSONArrayToByteArray(jsonArr);
	            	
	            	colValue = "X'"+Utils.byteArrayToHex(fullBArray)+"'";
				}
				else
				{
					colValue = toWriteJSON.getString(colName);
    				
    				AttributeMetaInfo attrMetaInfo 
    							= AttributeTypes.attributeMap.get(colName);
    				assert(attrMetaInfo != null);
    			    String dataType = attrMetaInfo.getDataType();
    				
    			    colValue = AttributeTypes.convertStringToDataTypeForMySQL
    						(colValue, dataType)+"";
				}
			    
				if(first)
	            {
					insertQuery = insertQuery + colValue;
	                first = false;
	            }
	            else
	            {
	            	insertQuery = insertQuery +" , "+colValue;
	            }
    		}
    		long end1 = System.currentTimeMillis();
    		insertQuery = insertQuery +" , X'"+nodeGUID+"' )";
    		
    		long end2 = System.currentTimeMillis();
    		
    		
    		myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
            stmt = myConn.createStatement();  
            
    		ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING INSERT "+insertQuery);
    		long start   = System.currentTimeMillis();
    		int rowCount = stmt.executeUpdate(insertQuery);
    		long end     = System.currentTimeMillis();
    		
    		if(ContextServiceConfig.DEBUG_MODE)
        	{
        		System.out.println("TIME_DEBUG: performStoreGUIDInSecondarySubspaceInsert insert  "
        				+tableName
        				+" nodeGUID "+nodeGUID+" "+(end-start)+" "+(end1-start1)+" "+(end2-end1));
        	}
    		
    		ContextServiceLogger.getLogger().fine(this.myNodeID+" EXECUTING INSERT rowCount "
    					+rowCount+" insertQuery "+insertQuery);
        }
        catch ( Exception | Error ex )
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
        long e1 = System.currentTimeMillis();
        if(ContextServiceConfig.DEBUG_MODE)
    	{
    		System.out.println("TIME_DEBUG: Total performStoreGUIDInSecondarySubspaceInsert insert  "
    				+ (e1-s1)+"updatedAttrValJSON "+toWriteJSON.length());
    	}
	}
	
	/**
	 * Only need to update attributes in atToValRep,
	 * as other attribtues are already there.
	 * @param tableName
	 * @param nodeGUID
	 * @param atrToValueRep
	 */
	private void performStoreGUIDInPrimarySubspaceUpdate( String nodeGUID, 
    		JSONObject jsonToWrite )
	{
		ContextServiceLogger.getLogger().fine(
				"performStoreGUIDInPrimarySubspaceUpdate "
				+ RegionMappingDataStorageDB.GUID_HASH_TABLE_NAME
				+ " nodeGUID "+nodeGUID);
		
        Connection myConn      		= null;
        Statement stmt         		= null;
        String tableName 			= RegionMappingDataStorageDB.GUID_HASH_TABLE_NAME;
        
        String updateSqlQuery     	= "UPDATE "+tableName+ " SET ";
        
        try
        {
        	@SuppressWarnings("unchecked")
			Iterator<String> colNameIter = jsonToWrite.keys();
        	int i = 0;
	        
        	while( colNameIter.hasNext() )
	        {
	            String colName  = colNameIter.next();
	            String colValue = "";
	            
	            if( colName.equals(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName) )
	            {
	            	JSONArray anonymizedIDToGuidList = jsonToWrite.getJSONArray(
	            			RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName);
	            	
	            	// if it is null in no privacy case, then it should be even inserted 
	            	// in towritejson.
	            	assert( anonymizedIDToGuidList != null );
	            	assert( anonymizedIDToGuidList.length() > 0 );
	            	
	            	byte[] fullBArray = this.serializeAnonymizedIDJSONArrayToByteArray
	            				(anonymizedIDToGuidList);
	            	
	            	colValue = "X'"+Utils.byteArrayToHex(fullBArray)+"'";
	            }
	            else if( colName.equals(RegionMappingDataStorageDB.unsetAttrsColName) )
	            {
	            	JSONObject unsetAttrsJSON 
	            					= jsonToWrite.getJSONObject(RegionMappingDataStorageDB.unsetAttrsColName);
	            	
	            	assert(unsetAttrsJSON != null);
	            	
	            	colValue = "'"+unsetAttrsJSON.toString()+"'";
	            }
	            else // else case can only be of attribute names.
	            {
	            	assert( AttributeTypes.attributeMap.containsKey(colName) );
	            	
	            	String attrVal   = jsonToWrite.getString(colName);
		            
		            AttributeMetaInfo attrMetaInfo 
		            			= AttributeTypes.attributeMap.get(colName);
					assert(attrMetaInfo != null);
		            String dataType = attrMetaInfo.getDataType();
					
		            colValue = AttributeTypes.convertStringToDataTypeForMySQL
							(attrVal, dataType)+"";
	            }
	            
	            if( i == 0 )
	            {
	                updateSqlQuery = updateSqlQuery + colName +" = "+colValue;
	            }
	            else
	            {
	                updateSqlQuery = updateSqlQuery +" , "+ colName +" = "+colValue;
	            }
	            i++;
	        }
            
	        updateSqlQuery = updateSqlQuery + " WHERE nodeGUID = X'"+nodeGUID+"'";
            
            myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
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
            catch( SQLException e )
            {
            	e.printStackTrace();
            }
        }
	}
	
	
	@SuppressWarnings("unchecked")
	private void performStoreGUIDInPrimarySubspaceInsert
					( String nodeGUID, JSONObject jsonToWrite )
	{
		ContextServiceLogger.getLogger().fine("performStoreGUIDInPrimarySubspaceInsert "
				+RegionMappingDataStorageDB.GUID_HASH_TABLE_NAME+" nodeGUID "+nodeGUID );
    	
        Connection myConn          = null;
        Statement stmt         	   = null;
        
        String tableName = RegionMappingDataStorageDB.GUID_HASH_TABLE_NAME;
        
        // delayed insert performs better than just insert
        String insertQuery         = "INSERT INTO "+tableName+ " (";
        
        try
        {
			Iterator<String> colIter = jsonToWrite.keys();
        	
        	boolean first = true;
        	// just a way to iterate over attributes.
    		while( colIter.hasNext() )
    		{
				String colName = colIter.next();
				
				if( first )
	            {
	                insertQuery = insertQuery + colName;
	                first = false;
	            }
	            else
	            {
	                insertQuery = insertQuery +", "+colName;
	            }
    		}
    		
    		insertQuery = insertQuery + ", nodeGUID) " + "VALUES"+ "(";
    		
    		first = true;
    		colIter = jsonToWrite.keys();
    		
			while( colIter.hasNext() )
			{
				String colName = colIter.next();
				String colValue = "";
				
				if( colName.equals(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName) )
	            {
	            	JSONArray anonymizedIDToGuidList = jsonToWrite.getJSONArray(
	            			RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName);
	            	
	            	// if it is null in no privacy case, then it should be even inserted 
	            	// in towritejson.
	            	assert( anonymizedIDToGuidList != null );
	            	assert( anonymizedIDToGuidList.length() > 0 );
	            	
	            	byte[] fullBArray = this.serializeAnonymizedIDJSONArrayToByteArray(anonymizedIDToGuidList);
	            	
	            	colValue = "X'"+Utils.byteArrayToHex(fullBArray)+"'";
	            }
	            else if( colName.equals(RegionMappingDataStorageDB.unsetAttrsColName) )
	            {
	            	JSONObject unsetAttrsJSON 
	            					= jsonToWrite.getJSONObject(RegionMappingDataStorageDB.unsetAttrsColName);
	            	
	            	assert(unsetAttrsJSON != null);
	            	
	            	colValue = "'"+unsetAttrsJSON.toString()+"'";
	            }
	            else // else case can only be of attribute names.
	            {
	            	assert( AttributeTypes.attributeMap.containsKey(colName) );
	            	
	            	String attrVal   = jsonToWrite.getString(colName);
		            
		            AttributeMetaInfo attrMetaInfo 
		            			= AttributeTypes.attributeMap.get(colName);
					assert(attrMetaInfo != null);
		            String dataType = attrMetaInfo.getDataType();
					
		            colValue = AttributeTypes.convertStringToDataTypeForMySQL
							(attrVal, dataType)+"";
	            }
			    
				if( first )
	            {
					insertQuery = insertQuery + colValue;
	                first = false;
	            }
	            else
	            {
	            	insertQuery = insertQuery +" , "+colValue;
	            }
			}
    		
			insertQuery = insertQuery +" , X'"+nodeGUID+"' )";			

    		
    		myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
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
		Iterator<String> attrIter 			= AttributeTypes.attributeMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName 				= attrIter.next();
			
			AttributeMetaInfo attrMetaInfo 	= AttributeTypes.attributeMap.get(attrName);
			String dataType 				= attrMetaInfo.getDataType();
			String defaultVal 				= attrMetaInfo.getDefaultValue();
			String mySQLDataType 			= AttributeTypes.mySQLDataType.get(dataType);
			
			
			newTableCommand = newTableCommand + ", "+attrName+" "+mySQLDataType
					+" DEFAULT "+ AttributeTypes.convertStringToDataTypeForMySQL(defaultVal, dataType);
				
			if( ContextServiceConfig.sqlDBType == SQL_DB_TYPE.MYSQL )
			{
				newTableCommand = newTableCommand 
							+" , "+ "INDEX USING BTREE("+attrName+")";
			}
		}
		return newTableCommand;
	}
	
	
	private String getPrivacyStorageString( String newTableCommand )
	{
		if(!ContextServiceConfig.IN_MEMORY_MYSQL)
		{
			newTableCommand 
				= newTableCommand + " , "+RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName
					+" BLOB";
		}
		else
		{
			// in memory case
			newTableCommand 
				= newTableCommand + " , "+RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName
					+" Binary("+ContextServiceConfig.GUID_SYMM_KEY_ENC_LENGTH+")";
		}
		
		return newTableCommand;
	}
	
	private byte[] serializeAnonymizedIDJSONArrayToByteArray(JSONArray jsonArray)
	{
		assert(jsonArray.length() > 0);
		int bArrayLen = -1;
		List<byte[]> byteList = new LinkedList<byte[]>();
		for(int i=0; i<jsonArray.length(); i++)
		{
			try 
			{
				byte[] bArray = Utils.hexStringToByteArray(jsonArray.getString(i));
				if(bArrayLen == -1)
				{
					bArrayLen = bArray.length;
				}
				else
				{
					assert(bArrayLen == bArray.length);
				}
				byteList.add(bArray);
			} catch (JSONException e)
			{
				e.printStackTrace();
			}
		}
		assert(bArrayLen != -1);
		ByteBuffer buf = ByteBuffer.allocate(Integer.SIZE/8 + bArrayLen*byteList.size());
		buf.putInt(bArrayLen);
		for(int i=0; i<byteList.size(); i++)
		{
			byte[] bArray = byteList.get(i);
			buf.put(bArray);
		}
		buf.flip();
		return buf.array();
	}
	
	
	private JSONArray deserializeByteArrayToAnonymizedIDJSONArray(byte[] fullByteArray)
	{
		JSONArray jsonArr = new JSONArray();
		ByteBuffer buf = ByteBuffer.wrap(fullByteArray);
		int byteArrayLen = buf.getInt();
		int numberElements = (fullByteArray.length-(Integer.SIZE/8))/byteArrayLen;
		for(int i=0; i<numberElements; i++)
		{
			byte[] bArray = new byte[byteArrayLen];
			buf.get(bArray);
			jsonArr.put(Utils.byteArrayToHex(bArray));
		}
		return jsonArr;
	}
	
	/**
	 * check if query has functions.
	 * If there are then sometimes 
	 * we need to do extra processing
	 * like for geoJSON, which initially 
	 * is processed by bounding rectangle
	 * @return
	 */
//	private boolean ifQueryHasFunctions(Vector<QueryComponent> qcomponents)
//	{
//		boolean isFun = false;
//		for(int i=0;i<qcomponents.size();i++)
//		{
//			QueryComponent qc = qcomponents.get(i);
//			if(qc.getComponentType() == QueryComponent.FUNCTION_PREDICATE)
//			{
//				isFun = true;
//				break;
//			}
//		}
//		return isFun;
//	}
	
	
	/*@Override
	public void createTables() 
	{
		Connection myConn  = null;
		Statement  stmt    = null;	
		try
		{
			myConn = dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt   = myConn.createStatement();
			Iterator<Integer> subspaceIter = this.subspaceInfoMap.keySet().iterator();
			
			while( subspaceIter.hasNext() )
			{
				int subspaceId = subspaceIter.next();
				Vector<SubspaceInfo> replicasOfSubspace 
										= subspaceInfoMap.get(subspaceId);
				
				for(int i = 0; i<replicasOfSubspace.size(); i++)
				{
					SubspaceInfo subInfo = replicasOfSubspace.get(i);
					
					int replicaNum = subInfo.getReplicaNum();
					
					HashMap<String, AttributePartitionInfo> subspaceAttributes 
															= subInfo.getAttributesOfSubspace();
					
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
						String attrDataType  = subspaceAttributes.get(attrName).getAttrMetaInfo().getDataType();
						String mySQLDataType = AttributeTypes.mySQLDataType.get(attrDataType);
						// lower range of this attribute in this subspace
						String lowerAttrName = "lower"+attrName;
						String upperAttrName = "upper"+attrName;
						
						newTableCommand = newTableCommand + " , "+lowerAttrName+" "+mySQLDataType+" , "
											+upperAttrName+" "+mySQLDataType;
						
						if( ContextServiceConfig.sqlDBType == SQL_DB_TYPE.MYSQL )
						{
							newTableCommand = newTableCommand 
									+" , "+ "INDEX USING BTREE("+lowerAttrName+" , "+upperAttrName+")";
						}
					}
					
					
					newTableCommand = newTableCommand +" )";
					stmt.executeUpdate(newTableCommand);
					
//					if( ContextServiceConfig.sqlDBType == SQL_DB_TYPE.SQLITE )
//					{
//						createIndexesInSQLite();
//					}
					
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
						      + " nodeGUID Binary(20) PRIMARY KEY";
					
					newTableCommand = getDataStorageString(newTableCommand);
					
					if(ContextServiceConfig.PRIVACY_ENABLED)
					{
						newTableCommand = getPrivacyStorageString(newTableCommand);
						// row format dynamic because we want TEXT columns to be stored completely off the row, 
						// only pointer should be stored in the row, otherwise default is storing 700 bytes for 
						// each TEXT in row.
						newTableCommand = newTableCommand +" ) ROW_FORMAT=DYNAMIC";
					}
					else
					{
						newTableCommand = newTableCommand +" ) ";
					}
					
					stmt.executeUpdate(newTableCommand);
				}
			}
			
			String tableName = HyperspaceDB.PRIMARY_SUBSPACE_TABLE_NAME;
			String newTableCommand = "create table "+tableName+" ( "
				      + " nodeGUID Binary(20) PRIMARY KEY";
			
			newTableCommand = getDataStorageString(newTableCommand);
			
			newTableCommand = newTableCommand + " , "+HyperspaceDB.unsetAttrsColName
								+" VARCHAR("+HyperspaceDB.varcharSizeForunsetAttrsCol+") ";
			
			//FIXME: need to fix this for MySQL and SQLite cases.
			if( ContextServiceConfig.PRIVACY_ENABLED )
			{
				newTableCommand = getPrivacyStorageString(newTableCommand);
				//newTableCommand	= getPrivacyStorageString(newTableCommand);
				// row format dynamic because we want TEXT columns to be stored completely off the row, 
				// only pointer should be stored in the row, otherwise default is storing 700 bytes for each TEXT in row.
				newTableCommand = newTableCommand +" ) ROW_FORMAT=DYNAMIC ";
			}
			else
			{
				newTableCommand = newTableCommand +" )";
			}
			
			stmt.executeUpdate(newTableCommand);
		}
		catch( SQLException mysqlEx )
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
	}*/
	
	
	/**
	 * Returns a list of regions/nodes that overlap with a query in a given subspace.
	 * @param subspaceNum
	 * @param qcomponents, takes matching attributes as input
	 * @return
	 */
	/*public HashMap<Integer, RegionInfoClass> 
		getOverlappingRegionsInSubspace( int subspaceId, int replicaNum, 
				HashMap<String, ProcessingQueryComponent> matchingQueryComponents )
	{
		long t0 = System.currentTimeMillis();
		HashMap<Integer, RegionInfoClass> answerList 
						= new HashMap<Integer, RegionInfoClass>();
		
		String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"PartitionInfo";
		
		String selectTableSQL = "SELECT hashCode, respNodeID from "+tableName+" WHERE ";
		Iterator<String> attrIter = matchingQueryComponents.keySet().iterator();
		int count = 0;
		while(attrIter.hasNext())
		{
			ProcessingQueryComponent qcomponent = matchingQueryComponents.get(attrIter.next());
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
			
			if( count != (matchingQueryComponents.size()-1) )
			{
				selectTableSQL = selectTableSQL + " AND ";
			}
			count++;
		}
		
		Statement stmt 		= null;
		Connection myConn 	= null;
		try
		{
			myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.SELECT);
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
		    	//int hashCode		 = rs.getInt("hashCode");
		    	RegionInfoClass overlapObj = new RegionInfoClass();
		    	
		    	//overlapObj.hashCode = hashCode;
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
	}*/
}