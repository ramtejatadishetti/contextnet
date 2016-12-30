package edu.umass.cs.contextservice.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;


import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.datasource.MySQLDataSource;
import edu.umass.cs.contextservice.database.datasource.SQLiteDataSource;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource.DB_REQUEST_TYPE;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.dataformat.SearchReplyGUIDRepresentationJSON;
import edu.umass.cs.contextservice.queryparsing.QueryParser;
import edu.umass.cs.contextservice.regionmapper.helper.AttributeValueRange;
import edu.umass.cs.contextservice.regionmapper.helper.ValueSpaceInfo;
import edu.umass.cs.contextservice.utils.Utils;


public class QueryAllDB
{
	public static final int UPDATE_REC 								= 1;
	public static final int INSERT_REC 								= 2;
	
	// maximum query length of 1000bytes
	public static final int MAX_QUERY_LENGTH						= 1000;
	
	public static final String groupGUID 							= "groupGUID";
	public static final String userIP 								= "userIP";
	public static final String userPort 							= "userPort";
	
	private AbstractDataSource dataSource;
	
	public QueryAllDB( Integer myNodeID )
			throws Exception
	{
		if(ContextServiceConfig.sqlDBType == ContextServiceConfig.SQL_DB_TYPE.MYSQL)
		{
			this.dataSource = new MySQLDataSource(myNodeID);
		}
		else if(ContextServiceConfig.sqlDBType == ContextServiceConfig.SQL_DB_TYPE.SQLITE)
		{
			this.dataSource = new SQLiteDataSource(myNodeID);
		}
		else
		{
			assert(false);
		}
		createTables();
	}
	
	
	public void createTables() 
	{
		Connection myConn  = null;
		Statement  stmt    = null;
		
		try
		{
			myConn = dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt   = myConn.createStatement();
			
			String tableName = "primarySubspaceDataStorage";
			String newTableCommand = "create table "+tableName+" ( "
				      + " nodeGUID Binary(20) PRIMARY KEY";
			
			newTableCommand = getDataStorageString(newTableCommand);
			
			// row format dynamic because we want TEXT columns to be stored completely off the row, 
			// only pointer should be stored in the row, otherwise default is storing 700 bytes for each TEXT in row.
			if(ContextServiceConfig.sqlDBType == ContextServiceConfig.SQL_DB_TYPE.MYSQL)
			{
				newTableCommand = newTableCommand +" ) ROW_FORMAT=DYNAMIC ";
			}
			else if(ContextServiceConfig.sqlDBType == ContextServiceConfig.SQL_DB_TYPE.SQLITE)
			{
				newTableCommand = newTableCommand +" ) ";
			}
			else
			{
				assert(false);
			}
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
	
	
	private String getDataStorageString(String newTableCommand)
	{
		Iterator<String> attrIter 
						= AttributeTypes.attributeMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			
			AttributeMetaInfo attrMetaInfo  = AttributeTypes.attributeMap.get(attrName);
			String dataType 				= attrMetaInfo.getDataType();
			String defaultVal 				= attrMetaInfo.getMinValue();
			String mySQLDataType 			= AttributeTypes.mySQLDataType.get(dataType);
			
			if(ContextServiceConfig.sqlDBType == ContextServiceConfig.SQL_DB_TYPE.MYSQL)
			{
				newTableCommand = newTableCommand + ", "+attrName+" "+mySQLDataType
						+ " DEFAULT "
						+ AttributeTypes.convertStringToDataTypeForMySQL(defaultVal, dataType)
						+ " , INDEX USING BTREE("+attrName+")";
			}
			else if(ContextServiceConfig.sqlDBType == ContextServiceConfig.SQL_DB_TYPE.SQLITE)
			{
				newTableCommand = newTableCommand + ", "+attrName+" "+mySQLDataType
					+ " DEFAULT "
					+ AttributeTypes.convertStringToDataTypeForMySQL(defaultVal, dataType);
			}
			else
			{
				assert(false);
			}
		}
		return newTableCommand;
	}
	
	
	public int processSearchQueryInSubspaceRegion( String query, 
			JSONArray resultArray)
	{	
		String mysqlQuery = getMySQLQueryForProcessSearchQueryInSubspaceRegion
				(query);
		
		assert(mysqlQuery != null);
		
		Connection myConn  = null;
		Statement stmt     = null;
		//JSONArray jsoArray = new JSONArray();
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
				stmt   = myConn.createStatement();
			}
			
			ContextServiceLogger.getLogger().fine("processSearchQueryInSubspaceRegion: "
								+mysqlQuery);
			
			ResultSet rs = stmt.executeQuery(mysqlQuery);
			while( rs.next() )
			{
				// String nodeGUID = rs.getString("nodeGUID");	
				
				// it is actually a JSONArray in hexformat byte array representation.
				// reverse conversion is byte array to String and then string to JSONArray.
				// byte[] realIDEncryptedArray = rs.getBytes(ACLattr);
				// ValueTableInfo valobj = new ValueTableInfo(value, nodeGUID);
				// answerList.add(valobj);
				if(ContextServiceConfig.sendFullRepliesWithinCS)
				{
					byte[] nodeGUIDBytes = rs.getBytes("nodeGUID");
					
					String nodeGUID = Utils.byteArrayToHex(nodeGUIDBytes);
					
					String anonymizedIDToGUIDMapping = null;
					JSONArray anonymizedIDToGuidArray = null;
					if( ContextServiceConfig.PRIVACY_ENABLED )
					{
						anonymizedIDToGUIDMapping 
							= rs.getString(RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName);
						
						if(anonymizedIDToGUIDMapping != null)
						{
							if(anonymizedIDToGUIDMapping.length() > 0)
							{
								anonymizedIDToGuidArray 
									= new JSONArray(anonymizedIDToGUIDMapping);
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
										( String query)
	{
		ValueSpaceInfo qValSpace = QueryParser.parseQuery(query);
		
		String tableName = RegionMappingDataStorageDB.GUID_HASH_TABLE_NAME;
		String mysqlQuery = "";
		
		// if privacy is enabled then we also fetch 
		// anonymizedIDToGuidMapping set.
		if(ContextServiceConfig.PRIVACY_ENABLED)
		{
			mysqlQuery = "SELECT nodeGUID , "
						+RegionMappingDataStorageDB.anonymizedIDToGUIDMappingColName
						+" from "+tableName+" WHERE ( ";
		}
		else
		{
			if(ContextServiceConfig.onlyResultCountEnable)
			{
				mysqlQuery = "SELECT COUNT(nodeGUID) AS RESULT_SIZE from "
								+tableName+" WHERE ( ";
			}
			else
			{
				mysqlQuery = "SELECT nodeGUID from "+tableName+" WHERE ( ";
			}
		}
		
		
		Iterator<String> attrIter = qValSpace.getValueSpaceBoundary().keySet().iterator();
		int counter = 0;
		try
		{
			while(attrIter.hasNext())
			{
				String attrName = attrIter.next();
				AttributeValueRange pqc = qValSpace.getValueSpaceBoundary().get(attrName);
				 
				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
				
				assert(attrMetaInfo != null);
				
				String dataType = attrMetaInfo.getDataType();
				
				
				// normal case of lower value being lesser than the upper value
				if(AttributeTypes.compareTwoValues(pqc.getLowerBound(), pqc.getUpperBound(), dataType))
				{
					String queryMin  = AttributeTypes.convertStringToDataTypeForMySQL
									(pqc.getLowerBound(), dataType)+"";
					String queryMax  = AttributeTypes.convertStringToDataTypeForMySQL
									(pqc.getUpperBound(), dataType)+"";
					
					if( counter == (qValSpace.getValueSpaceBoundary().size()-1) )
					{
						// it is assumed that the strings in query(pqc.getLowerBound() or pqc.getUpperBound()) 
						// will have single or double quotes in them so we don't need to them separately in mysql query
						mysqlQuery = mysqlQuery + " ( "+attrName +" >= "+queryMin +" AND " 
								+attrName +" <= "+queryMax+" ) )";
					}
					else
					{
						mysqlQuery = mysqlQuery + " ( "+attrName +" >= "+queryMin +" AND " 
								+attrName +" <= "+queryMax+" ) AND ";
					}
				}
				else
				{
					if(counter == (qValSpace.getValueSpaceBoundary().size()-1) )
					{
						String queryMin  = AttributeTypes.convertStringToDataTypeForMySQL
										(attrMetaInfo.getMinValue(), dataType)+"";
						String queryMax  = AttributeTypes.convertStringToDataTypeForMySQL
										(pqc.getUpperBound(), dataType)+"";
						
						mysqlQuery = mysqlQuery + " ( "
								+" ( "+attrName +" >= "+queryMin +" AND " 
								+attrName +" <= "+queryMax+" ) OR ";
								
						queryMin  = AttributeTypes.convertStringToDataTypeForMySQL
										(pqc.getLowerBound(), dataType)+"";
						queryMax  = AttributeTypes.convertStringToDataTypeForMySQL
										(attrMetaInfo.getMaxValue(), dataType)+"";
						
						mysqlQuery = mysqlQuery +" ( "+attrName +" >= "+queryMin +" AND " 
								+attrName +" <= "+queryMax+" ) ) )";
					}
					else
					{
						String queryMin  
							= AttributeTypes.convertStringToDataTypeForMySQL
									(attrMetaInfo.getMinValue(), dataType)+"";
						String queryMax  
							= AttributeTypes.convertStringToDataTypeForMySQL
									(pqc.getUpperBound(), dataType)+"";
						
						mysqlQuery = mysqlQuery + " ( "
								+" ( "+attrName +" >= "+queryMin +" AND " 
								+ attrName +" <= "+queryMax+" ) OR ";
								
						queryMin  = AttributeTypes.convertStringToDataTypeForMySQL
										(pqc.getLowerBound(), dataType)+"";
						queryMax  = AttributeTypes.convertStringToDataTypeForMySQL
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
	
	public JSONObject getGUIDStoredInPrimarySubspace( String guid )
	{
		Connection myConn 		= null;
		Statement stmt 			= null;
		
		String selectQuery 		= "SELECT * ";
		String tableName 		= "primarySubspaceDataStorage";
		
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
    public void storeGUIDInPrimarySubspace(String tableName, String nodeGUID, 
    		JSONObject updatedAttrValJSON, int updateOrInsert) throws JSONException
    {
    	if( updateOrInsert == RegionMappingDataStorageDB.INSERT_REC )
    	{
    		this.performStoreGUIDInPrimarySubspaceInsert
    			(tableName, nodeGUID, updatedAttrValJSON);
    	}
    	else if( updateOrInsert == RegionMappingDataStorageDB.UPDATE_REC )
    	{
    		this.performStoreGUIDInPrimarySubspaceUpdate
    				(tableName, nodeGUID, updatedAttrValJSON);
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
    		JSONObject updatedAttrValJSON )
	{
		ContextServiceLogger.getLogger().fine("performStoreGUIDInPrimarySubspaceUpdate "+tableName
				+" nodeGUID "+nodeGUID);
		
        Connection myConn      		= null;
        Statement stmt         		= null;
        
        String updateSqlQuery     	= "UPDATE "+tableName+ " SET ";
        
        try
        {
        	@SuppressWarnings("unchecked")
			Iterator<String> attrNameIter = updatedAttrValJSON.keys();
        	int i = 0;
	        
        	while( attrNameIter.hasNext() )
	        {
	            String attrName = attrNameIter.next();
	            
	            String newVal   = updatedAttrValJSON.getString(attrName);
	            
	            AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
				assert(attrMetaInfo != null);
	            String dataType = attrMetaInfo.getDataType();
				
				newVal = AttributeTypes.convertStringToDataTypeForMySQL
						(newVal, dataType)+"";	
                
	            if( i == 0 )
	            {
	                updateSqlQuery = updateSqlQuery + attrName +" = "+newVal;
	            }
	            else
	            {
	                updateSqlQuery = updateSqlQuery +" , "+ attrName +" = "+newVal;
	            }
                
	            i++;
	        }
            
	        updateSqlQuery = updateSqlQuery + " WHERE nodeGUID = X'"+nodeGUID+"'";
            
            myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
            stmt = myConn.createStatement();
            
        	// if update fails then insert
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
	private void performStoreGUIDInPrimarySubspaceInsert( 
			String tableName, String nodeGUID, 
			JSONObject updatedAttrValJSON )
	{
		ContextServiceLogger.getLogger().fine("performStoreGUIDInPrimarySubspaceInsert "
				+tableName+" nodeGUID "+nodeGUID );
    	
        Connection myConn          = null;
        Statement stmt         	   = null;
        
        // delayed insert performs better than just insert
        String insertQuery         = "INSERT INTO "+tableName+ " (";
        
        try
        {
			Iterator<String> attrIter = updatedAttrValJSON.keys();
        	
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
    		}
    		
    		insertQuery = insertQuery + ", nodeGUID) " + "VALUES"+ "(";
    		
    		
    		first = true;
    		attrIter = updatedAttrValJSON.keys();
    		
			while( attrIter.hasNext() )
			{
				String currAttrName = attrIter.next();
				String currAttrValue = "";

				currAttrValue = updatedAttrValJSON.getString(currAttrName);
				
				AttributeMetaInfo attrMetaInfo 
							= AttributeTypes.attributeMap.get(currAttrName);
				
			    String dataType = attrMetaInfo.getDataType();
				
			    currAttrValue = AttributeTypes.convertStringToDataTypeForMySQL
						(currAttrValue, dataType)+"";
			    
				if( first )
	            {
					insertQuery = insertQuery + currAttrValue;
	                first = false;
	            }
	            else
	            {
	            	insertQuery = insertQuery +" , "+currAttrValue;
	            }
			}
			
			insertQuery = insertQuery +" , X'"+nodeGUID+"' )";
			
    		
    		myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
            stmt = myConn.createStatement();  
            
    		ContextServiceLogger.getLogger().fine(" EXECUTING INSERT "+insertQuery);
    		long start   = System.currentTimeMillis();
    		int rowCount = stmt.executeUpdate(insertQuery);
    		long end     = System.currentTimeMillis();
    		
    		if( ContextServiceConfig.DEBUG_MODE )
        	{
    			System.out.println("TIME_DEBUG: performStoreGUIDInPrimarySubspaceInsert "
        															+(end-start) );
        	}
    		
    		ContextServiceLogger.getLogger().fine(" EXECUTING INSERT rowCount "+rowCount
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
}