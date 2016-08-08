package edu.umass.cs.contextservice.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;


import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.dataformat.SearchReplyGUIDRepresentationJSON;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.utils.DelayProfiler;


public class QueryAllMySQLDB<NodeIDType>
{
	public static final int UPDATE_REC 								= 1;
	public static final int INSERT_REC 								= 2;
	
	// maximum query length of 1000bytes
	public static final int MAX_QUERY_LENGTH						= 1000;
	
	public static final String groupGUID 							= "groupGUID";
	public static final String userIP 								= "userIP";
	public static final String userPort 							= "userPort";
	
	private final DataSource<NodeIDType> mysqlDataSource;
	
	public QueryAllMySQLDB( NodeIDType myNodeID )
			throws Exception
	{
		this.mysqlDataSource = new DataSource<NodeIDType>(myNodeID);
		createTables();
	}
	
	
	public void createTables() 
	{
		Connection myConn  = null;
		Statement  stmt    = null;
		
		try
		{
			myConn = mysqlDataSource.getConnection();
			stmt   = myConn.createStatement();
			
			String tableName = "primarySubspaceDataStorage";
			String newTableCommand = "create table "+tableName+" ( "
				      + " nodeGUID Binary(20) PRIMARY KEY";
			
			newTableCommand = getDataStorageString(newTableCommand);
			
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
			
			newTableCommand = newTableCommand + ", "+attrName+" "+mySQLDataType
					+ " DEFAULT "
					+ AttributeTypes.convertStringToDataTypeForMySQL(defaultVal, dataType)
					+ " , INDEX USING BTREE("+attrName+")";
		}
		return newTableCommand;
	}
	
	public int processSearchQueryInSubspaceRegion( String query, 
			JSONArray resultArray)
	{
		long t0 = System.currentTimeMillis();
		
		String mysqlQuery = getMySQLQueryForProcessSearchQueryInSubspaceRegion
				(query);
		
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
			myConn = this.mysqlDataSource.getConnection();
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
						if(ContextServiceConfig.sendFullRepliesWithinCS)
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
					// String nodeGUID = rs.getString("nodeGUID");
					byte[] nodeGUIDBytes = rs.getBytes("nodeGUID");
					
					// it is actually a JSONArray in hexformat byte array representation.
					// reverse conversion is byte array to String and then string to JSONArray.
					// byte[] realIDEncryptedArray = rs.getBytes(ACLattr);
					// ValueTableInfo valobj = new ValueTableInfo(value, nodeGUID);
					// answerList.add(valobj);
					if(ContextServiceConfig.sendFullRepliesWithinCS)
					{
						String nodeGUID = Utils.bytArrayToHex(nodeGUIDBytes);
						
						String anonymizedIDToGUIDMapping = null;
						JSONArray anonymizedIDToGuidArray = null;
						if( ContextServiceConfig.PRIVACY_ENABLED )
						{
							anonymizedIDToGUIDMapping 
								= rs.getString(HyperspaceMySQLDB.anonymizedIDToGUIDMappingColName);
							
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
		QueryInfo<NodeIDType> qinfo = new QueryInfo<NodeIDType>(query);
		
		HashMap<String, ProcessingQueryComponent> pqComponents = qinfo.getProcessingQC();
		Vector<QueryComponent> qcomponents = qinfo.getQueryComponents();
		
		boolean isFun = ifQueryHasFunctions(qcomponents);
		
		String tableName = "primarySubspaceDataStorage";
		String mysqlQuery = "";
		
		if( isFun )
		{
			// get all fields as function might need to check them
			// for post processing
			mysqlQuery = "SELECT * from "+tableName+" WHERE ( ";
		}
		else
		{
			// if privacy is enabled then we also fetch 
			// anonymizedIDToGuidMapping set.
			if(ContextServiceConfig.PRIVACY_ENABLED)
			{
				mysqlQuery = "SELECT nodeGUID , "+HyperspaceMySQLDB.anonymizedIDToGUIDMappingColName
						+" from "+tableName+" WHERE ( ";
			}
			else
			{
				mysqlQuery = "SELECT nodeGUID from "+tableName+" WHERE ( ";
			}
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
	
	public JSONObject getGUIDStoredInPrimarySubspace( String guid )
	{
		long t0 = System.currentTimeMillis();
		Connection myConn 		= null;
		Statement stmt 			= null;
		
		String selectQuery 		= "SELECT * ";
		String tableName 		= "primarySubspaceDataStorage";
		
		JSONObject oldValueJSON = new JSONObject();
		
		selectQuery = selectQuery + " FROM "+tableName+" WHERE nodeGUID = X'"+guid+"'";
		
		try
		{
			myConn = this.mysqlDataSource.getConnection();
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
		
		if(ContextServiceConfig.DELAY_PROFILER_ON)
		{
			DelayProfiler.updateDelay("getGUIDStoredInPrimarySubspace", t0);
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
    	if( updateOrInsert == HyperspaceMySQLDB.INSERT_REC )
    	{
    		this.performStoreGUIDInPrimarySubspaceInsert
    			(tableName, nodeGUID, updatedAttrValJSON);
    	}
    	else if( updateOrInsert == HyperspaceMySQLDB.UPDATE_REC )
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
            
            myConn = this.mysqlDataSource.getConnection();
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
			
    		
    		myConn = this.mysqlDataSource.getConnection();
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