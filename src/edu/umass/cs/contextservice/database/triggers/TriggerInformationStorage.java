package edu.umass.cs.contextservice.database.triggers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.HyperspaceDB;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource;
import edu.umass.cs.contextservice.database.datasource.AbstractDataSource.DB_REQUEST_TYPE;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.schemes.HyperspaceHashing;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.utils.DelayProfiler;

/**
 * 
 * Implements the trigger storage table creation
 * and search and update trigger storage.
 * @author adipc
 */
public class TriggerInformationStorage implements 
										TriggerInformationStorageInterface
{
	//private final Integer myNodeID;
	//private final HashMap<Integer, Vector<SubspaceInfo>> subspaceInfoMap;
	private final AbstractDataSource dataSource;
	
	public TriggerInformationStorage( Integer myNodeID, 
			AbstractDataSource dataSource )
	{
		//this.myNodeID = myNodeID;
		//this.subspaceInfoMap = subspaceInfoMap;
		this.dataSource = dataSource;
	}
	
	
	@Override
	public void createTriggerStorageTables()
	{
		Connection myConn  = null;
		Statement  stmt    = null;
		
		try
		{
			myConn = dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt   =  myConn.createStatement();
			
			String tableName = HyperspaceDB.ATTR_INDEX_TRIGGER_TABLE_NAME;
			
			String newTableCommand = "create table "+tableName+" ( groupGUID BINARY(20) NOT NULL , "
					+ "userIP Binary(4) NOT NULL ,  userPort INTEGER NOT NULL , expiryTime BIGINT NOT NULL ";
			newTableCommand = getPartitionInfoStorageString(newTableCommand);
			
			if( ContextServiceConfig.disableUniqueQueryStorage )
			{
				newTableCommand = newTableCommand +" , INDEX USING BTREE(expiryTime), "
						+ "INDEX USING HASH(groupGUID) )";
			}
			else
			{
				newTableCommand = newTableCommand +" , PRIMARY KEY(groupGUID, userIP, userPort), INDEX USING BTREE(expiryTime), "
						+ "INDEX USING HASH(groupGUID) )";
			}
			
			stmt.executeUpdate(newTableCommand);
			
			
			
			if( ContextServiceConfig.TRIGGER_ENABLED 
								&& ContextServiceConfig.UniqueGroupGUIDEnabled )
			{
				// currently it is assumed that there are only conjunctive queries
				// DNF form queries can be added by inserting its multiple conjunctive components.
				//ContextServiceLogger.getLogger().fine( "HyperspaceMySQLDB "
				//		+ " TRIGGER_ENABLED "+ContextServiceConfig.TRIGGER_ENABLED );					
				//createTablesForTriggers(subInfo, stmt);
				
				// for storing the trigger data, which is search queries
				
				tableName = HyperspaceDB.HASH_INDEX_TRIGGER_TABLE_NAME;
				
				newTableCommand = "create table "+tableName+" ( groupGUID BINARY(20) NOT NULL , "
						+ "userIP Binary(4) NOT NULL ,  userPort INTEGER NOT NULL ";
				
				newTableCommand = newTableCommand +" , PRIMARY KEY(groupGUID, userIP, userPort) )";
				stmt.executeUpdate(newTableCommand);
			}
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
	 * Inserts trigger info on a query into the table
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoTriggerDataStorage( String userQuery, 
			String groupGUID, String userIP, int userPort, 
			long expiryTimeFromNow )
	{
		long t0 			= System.currentTimeMillis();
		Connection myConn   = null;
		Statement stmt      = null;
		
		String tableName = HyperspaceDB.ATTR_INDEX_TRIGGER_TABLE_NAME;
		
		QueryInfo processedQInfo = new QueryInfo(userQuery);
		HashMap<String, ProcessingQueryComponent> pqcMap = processedQInfo.getProcessingQC();
		
		String hexIP;
		try
		{
			hexIP = Utils.byteArrayToHex(InetAddress.getByName(userIP).getAddress());	
			
			String insertTableSQL = " INSERT INTO "+tableName 
					+" ( groupGUID, userIP, userPort , expiryTime ";
			
			Iterator<String> qattrIter = pqcMap.keySet().iterator();
			while( qattrIter.hasNext() )
			{
				String qattrName = qattrIter.next();
				String lowerAtt = "lower"+qattrName;
				String upperAtt = "upper"+qattrName;
				insertTableSQL = insertTableSQL + ", "+lowerAtt+" , "+upperAtt;
			}
			
			insertTableSQL = insertTableSQL + " ) VALUES ( X'"+groupGUID+"', "+
							 " X'"+hexIP+"', "+userPort+" , "+expiryTimeFromNow+" ";
			
			// assuming the order of iterator over attributes to be same in above and here
			qattrIter = pqcMap.keySet().iterator();
			while( qattrIter.hasNext() )
			{
				String qattrName = qattrIter.next();
				
				ProcessingQueryComponent pqc = pqcMap.get(qattrName);
					
				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(qattrName);
				String dataType = attrMetaInfo.getDataType();
				
				String lowerBound 
					= AttributeTypes.convertStringToDataTypeForMySQL(pqc.getLowerBound(), dataType)+"";
				String upperBound 
					= AttributeTypes.convertStringToDataTypeForMySQL(pqc.getUpperBound(), dataType)+"";
				
				insertTableSQL = insertTableSQL + " , "+lowerBound+" , "+ upperBound;
			}
			insertTableSQL = insertTableSQL + " ) ";
			
			myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt = myConn.createStatement();
			
			// execute insert SQL stetement
			stmt.executeUpdate(insertTableSQL);
			
		} catch(SQLException sqlex)
		{
			sqlex.printStackTrace();
		} catch (UnknownHostException e) 
		{
			e.printStackTrace();
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
	 * @throws InterruptedException 
	 */
	public void getTriggerDataInfo( JSONObject oldValJSON, JSONObject onlyUpdateAttrValJSON, 
		HashMap<String, GroupGUIDInfoClass> removedGroupGUIDMap, 
		HashMap<String, GroupGUIDInfoClass> addedGroupGUIDMap, 
		int requestType, JSONObject newUnsetAttrs,
		boolean firstTimeInsert )
					throws InterruptedException
	{
		assert(removedGroupGUIDMap != null);
		assert(addedGroupGUIDMap != null);
		// oldValJSON should contain all attribtues.
		// newUpdateVal contains only updated attr:val pairs
		//assert(oldValJSON.length() == AttributeTypes.attributeMap.size());
		long t0 = System.currentTimeMillis();
		
		
		if( requestType == ValueUpdateToSubspaceRegionMessage.REMOVE_ENTRY )
		{
			OldValueGroupGUIDs old = new OldValueGroupGUIDs
			(oldValJSON, onlyUpdateAttrValJSON, newUnsetAttrs, removedGroupGUIDMap,
					dataSource);
			old.run();
		}
		else if( requestType == ValueUpdateToSubspaceRegionMessage.ADD_ENTRY )
		{
			returnAddedGroupGUIDs( oldValJSON, 
					onlyUpdateAttrValJSON, addedGroupGUIDMap, newUnsetAttrs, firstTimeInsert);
		}
		else if( requestType == ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY )
		{
			// first time insert is done as udpate, as it results reply from one node.
			// so we don't need to check for old groups to which this new GUID was part of.
			if(firstTimeInsert)
			{
				returnAddedGroupGUIDs(oldValJSON, 
						onlyUpdateAttrValJSON, addedGroupGUIDMap, newUnsetAttrs, firstTimeInsert );
			}
			else
			{
				// both old and new value GUIDs stored at same nodes,
				// makes it possible to find which groupGUIDs needs to be triggered.
				// in parallel
//				OldValueGroupGUIDs<Integer> old = new OldValueGroupGUIDs<Integer>
//				(subspaceId, oldValJSON, onlyUpdateAttrValJSON, newUnsetAttrs, oldValGroupGUIDMap,
//						dataSource);
//				old.run();
////				Thread st = new Thread(old);
////				st.start();
//				returnAddedGroupGUIDs( subspaceId, oldValJSON, 
//						newJSONToWrite, newValGroupGUIDMap, newUnsetAttrs, firstTimeInsert );
////				st.join();
				
				
				HashMap<String, GroupGUIDInfoClass> oldSatisfyingGroups 
												= new HashMap<String, GroupGUIDInfoClass>();
				
				HashMap<String, GroupGUIDInfoClass> newSatisfyingGroups 
												= new HashMap<String, GroupGUIDInfoClass>();
				getOldAndNewValueSatisfyingGroups
					(oldValJSON, onlyUpdateAttrValJSON,  oldSatisfyingGroups, newSatisfyingGroups,
							newUnsetAttrs);
				
				// computing removed groups
				Iterator<String> groupGUIDIter = oldSatisfyingGroups.keySet().iterator();
				
				while( groupGUIDIter.hasNext() )
				{
					String oldGrpGUID = groupGUIDIter.next();
					
					// if newSatisfying GUID don;t have old value group GUID 
					// then it is a removedGUIDTrigger
					if( !newSatisfyingGroups.containsKey(oldGrpGUID) )
					{
						removedGroupGUIDMap.put(oldGrpGUID, oldSatisfyingGroups.get(oldGrpGUID));				
					}
				}
				
				// for added trigger
				groupGUIDIter = newSatisfyingGroups.keySet().iterator();
				
				while( groupGUIDIter.hasNext() )
				{
					String newGrpGUID = groupGUIDIter.next();
					
					// if oldSatisfyingGroups  don;t have new value group GUID 
					// then it is a addedGUIDTrigger
					if( !oldSatisfyingGroups.containsKey(newGrpGUID) )
					{
						addedGroupGUIDMap.put(newGrpGUID, newSatisfyingGroups.get(newGrpGUID));				
					}
				}
				
			}
		}
		
		if( ContextServiceConfig.DELAY_PROFILER_ON )
		{
			DelayProfiler.updateDelay("getTriggerInfo", t0);
		}
	}
	
	
	private void getOldAndNewValueSatisfyingGroups
					( JSONObject oldValJSON, JSONObject updateValJSON, 
				HashMap<String, GroupGUIDInfoClass> oldSatisfyingGroups, 
				HashMap<String, GroupGUIDInfoClass> newSatisfyingGroups
				, JSONObject newUnsetAttrs )
	{
		String tableName 			= HyperspaceDB.ATTR_INDEX_TRIGGER_TABLE_NAME;
		
		assert(oldValJSON != null);
		assert(oldValJSON.length() > 0);
		JSONObject oldUnsetAttrs 	= HyperspaceHashing.getUnsetAttrJSON(oldValJSON);
		
		// it can be empty but should not be null
		assert( oldUnsetAttrs != null );
		
		Connection myConn 			= null;
		Statement stmt 				= null;
		
		//FIXME: DONE: it could be changed to calculating tobe removed GUIDs right here.
		// in one single mysql query once can check to old group guid and new group guids
		// and return groupGUIDs which are in old value but no in new value.
		// but usually complex queries have more cost, so not sure if it would help.
		// but this optimization can be checked later on if number of group guids returned becomes 
		// an issue later on. 
		
		try
		{
			String queriesWithAttrs 
				= TriggerInformationStorage.getQueriesThatContainAttrsInUpdate(updateValJSON);
			//String newTableName = "projTable";
			
			//String createTempTable = "CREATE TEMPORARY TABLE "+
			//		newTableName+" AS ( "+queriesWithAttrs+" ) ";
			
			String oldGroupsQuery 
				= TriggerInformationStorage.getQueryToGetOldValueGroups(oldValJSON);
			
			String oldGroupQuery = "SELECT groupGUID, userIP, userPort FROM "+tableName
					+ " WHERE "
					+ " groupGUID IN ( "+queriesWithAttrs+" ) AND "
 				    + " groupGUID IN ( "+oldGroupsQuery+" ) ";
			
			ContextServiceLogger.getLogger().fine("returnOldValueGroupGUIDs getTriggerInfo "
												+oldGroupQuery);
			myConn 	     = dataSource.getConnection(DB_REQUEST_TYPE.SELECT);
			stmt   		 = myConn.createStatement();
			
			ResultSet rs = stmt.executeQuery(oldGroupQuery);
			
			while( rs.next() )
			{
				// FIXME: need to replace these with macros
				byte[] groupGUIDBytes = rs.getBytes("groupGUID");
				String groupGUIDString = Utils.byteArrayToHex(groupGUIDBytes);
				byte[] ipAddressBytes = rs.getBytes("userIP");
				String userIPString = InetAddress.getByAddress(ipAddressBytes).getHostAddress();
				int userPort = rs.getInt("userPort");
				
				GroupGUIDInfoClass groupGUIDInfo = new GroupGUIDInfoClass(
						groupGUIDString, userIPString, userPort);
				oldSatisfyingGroups.put(groupGUIDString, groupGUIDInfo);
			}
			rs.close();
			
			
			// for new value
			
//			String queriesWithAttrs = TriggerInformationStorage.getQueriesThatContainAttrsInUpdate
//					(newUpdateVal, subspaceId);
			
			String selectQuery = "SELECT groupGUID, userIP, userPort FROM "+tableName
					+" WHERE ";
			
			String newGroupsQuery = 
					getQueryToGetNewValueGroups
					( oldValJSON, updateValJSON, 
							newUnsetAttrs);
			
			selectQuery = selectQuery 
					+ " groupGUID IN ( "+queriesWithAttrs+" ) AND "
//					+ " groupGUID NOT IN ( "+oldGroupsQuery+" ) AND "
					+ " groupGUID IN ( "+newGroupsQuery+" ) ";
			
			rs = stmt.executeQuery(selectQuery);
			
			while( rs.next() )
			{
				// FIXME: need to replace these with macros
				byte[] groupGUIDBytes = rs.getBytes("groupGUID");
				String groupGUIDString = Utils.byteArrayToHex(groupGUIDBytes);
				byte[] ipAddressBytes = rs.getBytes("userIP");
				String userIPString = InetAddress.getByAddress(ipAddressBytes).getHostAddress();
				int userPort = rs.getInt("userPort");
				
				GroupGUIDInfoClass groupGUIDInfo = new GroupGUIDInfoClass(
						groupGUIDString, userIPString, userPort);
				newSatisfyingGroups.put(groupGUIDString, groupGUIDInfo);
			}
			rs.close();
			
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		catch (JSONException e)
		{
			e.printStackTrace();
		}
		catch (UnknownHostException e)
		{
			e.printStackTrace();
		}
		finally
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
	}
	
	/**
	 * Returns search queries that contain attributes of an update, 
	 * as only those search queries can be affected.
	 * This helps in reducing the size of the search queries that needs to be checked
	 * further if the GUID in update satisfies that or not.
	 * @param attrsInUpdate
	 * @return
	 */
	public static String getQueriesThatContainAttrsInUpdate( JSONObject attrsInUpdate )
	{
		String tableName 			= HyperspaceDB.ATTR_INDEX_TRIGGER_TABLE_NAME;
		String selectQuery 			= "SELECT groupGUID FROM "+tableName+" WHERE ";
		
		@SuppressWarnings("unchecked")
		Iterator<String> attrIter 	= attrsInUpdate.keys();
		boolean first = true;
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			AttributeMetaInfo attrMeta = AttributeTypes.attributeMap.get(attrName);
			String defaultVal = attrMeta.getDefaultValue();
			
			if( first )
			{
				if( attrMeta.isLowerValDefault() )
				{
					String lowerAttrName = "lower"+attrName;
					selectQuery = selectQuery + lowerAttrName +" > "+defaultVal;
				}
				else
				{
					String upperAttrName = "upper"+attrName;
					selectQuery = selectQuery + upperAttrName +" < "+defaultVal;
				}
				first = false;
			}
			else
			{
				if( attrMeta.isLowerValDefault() )
				{
					String lowerAttrName = "lower"+attrName;
					selectQuery = selectQuery +" AND "+ lowerAttrName +" > "+defaultVal;
				}
				else
				{
					String upperAttrName = "upper"+attrName;
					selectQuery = selectQuery +" AND "+ upperAttrName +" < "+defaultVal;
				}
			}
		}
		return selectQuery;
	}
	
	
	public static String getQueryToGetOldValueGroups(JSONObject oldValJSON) 
			throws JSONException
	{
		String tableName 			= HyperspaceDB.ATTR_INDEX_TRIGGER_TABLE_NAME;
		
		JSONObject oldUnsetAttrs 	= HyperspaceHashing.getUnsetAttrJSON(oldValJSON);
		
		assert( oldUnsetAttrs != null );
		
		
		Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
		//		attrSubspaceInfo.keySet().iterator();
		// for groups associated with old value
		boolean first = true;
		String selectQuery = "SELECT groupGUID FROM "+tableName+" WHERE ";
		
		while( attrIter.hasNext() )
		{
			String currAttrName = attrIter.next();
			
			AttributeMetaInfo attrMetaInfo 
					= AttributeTypes.attributeMap.get(currAttrName);
			
			String dataType = attrMetaInfo.getDataType();
			
			String minVal = attrMetaInfo.getMinValue();
			String maxVal = attrMetaInfo.getMaxValue();
			
			String attrValForMysql = "";
			
			if( oldUnsetAttrs.has(currAttrName) )
			{
				attrValForMysql = attrMetaInfo.getDefaultValue();
			}
			else
			{
				attrValForMysql = AttributeTypes.convertStringToDataTypeForMySQL
						(oldValJSON.getString(currAttrName), dataType)+"";
			}
			
			
			String lowerValCol = "lower"+currAttrName;
			String upperValCol = "upper"+currAttrName;
			//FIXED: for circular queries, this won't work.
			if( first )
			{
				// <= and >= both to handle the == case of the default value
				if(ContextServiceConfig.disableCircularQueryTrigger)
				{
					selectQuery = selectQuery+lowerValCol+" <= "+attrValForMysql
					+" AND "+upperValCol+" >= "+attrValForMysql;
					
				}
				else
				{
					selectQuery = selectQuery + " ( ( "+lowerValCol+" <= "+attrValForMysql
							+" AND "+upperValCol+" >= "+attrValForMysql+" ) OR "
									+ " ( ( "+lowerValCol+" > "+upperValCol+") AND "
											+ " ( ( " +minVal+" <= "+attrValForMysql
											+" AND "+upperValCol+" >= "+attrValForMysql+" ) "
									+ "OR ( "+ lowerValCol+" <= "+attrValForMysql
									+" AND "+maxVal+" >= "+attrValForMysql+" ) " + " ) "+ " ) )";
				}
				first = false;
			}
			else
			{
				// old query without circular predicates
//				selectQuery = selectQuery+" AND "+lowerValCol+" <= "+attrValForMysql
//						+" AND "+upperValCol+" >= "+attrValForMysql;
				
				if(ContextServiceConfig.disableCircularQueryTrigger)
				{
					selectQuery = selectQuery+" AND "+lowerValCol+" <= "+attrValForMysql
							+" AND "+upperValCol+" >= "+attrValForMysql;
				}
				else
				{
					selectQuery = selectQuery + " AND ( ( "+lowerValCol+" <= "+attrValForMysql
							+" AND "+upperValCol+" >= "+attrValForMysql+" ) OR "
									+ " ( ( "+lowerValCol+" > "+upperValCol+") AND "
											+ " ( ( " +minVal+" <= "+attrValForMysql
											+" AND "+upperValCol+" >= "+attrValForMysql+" ) "
									+ "OR ( "+ lowerValCol+" <= "+attrValForMysql
									+" AND "+maxVal+" >= "+attrValForMysql+" ) " + " ) "+ " ) )";
				}
			}
		}
		return selectQuery;
	}
	
	
	public static String getQueryToGetNewValueGroups
				( JSONObject oldValJSON, JSONObject newJSONToWrite, 
						JSONObject newUnsetAttrs )
								throws JSONException
	{
		String tableName 			= HyperspaceDB.ATTR_INDEX_TRIGGER_TABLE_NAME;

		Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
		// for groups associated with the new value
		try
		{
			boolean first = true;
			String selectQuery = "SELECT groupGUID FROM "
						+tableName+" WHERE ";
			while( attrIter.hasNext() )
			{
				String currAttrName = attrIter.next();
				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(currAttrName);
				
				String minVal = attrMetaInfo.getMinValue();
				String maxVal = attrMetaInfo.getMaxValue();
				
				String dataType = attrMetaInfo.getDataType();
				
				String attrValForMysql = attrMetaInfo.getDefaultValue();
				
				if( !newUnsetAttrs.has(currAttrName) )
				{
					if( newJSONToWrite.has(currAttrName) )
					{
						attrValForMysql =
						AttributeTypes.convertStringToDataTypeForMySQL
						(newJSONToWrite.getString(currAttrName), dataType)+"";
					}
					else if( oldValJSON.has(currAttrName) )
					{
						attrValForMysql =
								AttributeTypes.convertStringToDataTypeForMySQL
								(oldValJSON.getString(currAttrName), dataType)+"";	
					}
				}
				
				String lowerValCol = "lower"+currAttrName;
				String upperValCol = "upper"+currAttrName;
				//FIXED: will not work for circular queries
				if( first )
				{
					// <= and >= both to handle the == case of the default value
//					selectQuery = selectQuery + lowerValCol+" <= "+attrValForMysql
//							+" AND "+upperValCol+" >= "+attrValForMysql;
					
					if(ContextServiceConfig.disableCircularQueryTrigger)
					{
						selectQuery = selectQuery + lowerValCol+" <= "+attrValForMysql
								+" AND "+upperValCol+" >= "+attrValForMysql;
					}
					else
					{
						selectQuery = selectQuery + " ( ( "+lowerValCol+" <= "+attrValForMysql
								+" AND "+upperValCol+" >= "+attrValForMysql+" ) OR "
										+ " ( ( "+lowerValCol+" > "+upperValCol+") AND "
												+ " ( ( " +minVal+" <= "+attrValForMysql
												+" AND "+upperValCol+" >= "+attrValForMysql+" ) "
										+ "OR ( "+ lowerValCol+" <= "+attrValForMysql
										+" AND "+maxVal+" >= "+attrValForMysql+" ) " + " ) "+ " ) )";
					}
					
					first = false;
				}
				else
				{
//					selectQuery = selectQuery+" AND "+lowerValCol+" <= "+attrValForMysql
//							+" AND "+upperValCol+" >= "+attrValForMysql;
					
					if(ContextServiceConfig.disableCircularQueryTrigger)
					{
						selectQuery = selectQuery+" AND "+lowerValCol+" <= "+attrValForMysql
								+" AND "+upperValCol+" >= "+attrValForMysql;
					}
					else
					{
						selectQuery = selectQuery + " AND ( ( "+lowerValCol+" <= "+attrValForMysql
								+" AND "+upperValCol+" >= "+attrValForMysql+" ) OR "
										+ " ( ( "+lowerValCol+" > "+upperValCol+") AND "
												+ " ( ( " +minVal+" <= "+attrValForMysql
												+" AND "+upperValCol+" >= "+attrValForMysql+" ) "
										+ "OR ( "+ lowerValCol+" <= "+attrValForMysql
										+" AND "+maxVal+" >= "+attrValForMysql+" ) " + " ) "+ " ) )";
					}
				}
			}
			return selectQuery;
		}
		catch (JSONException e) 
		{
			e.printStackTrace();
		}
		assert(false);
		return "";
	}
	
	
	/**
	 * this function runs independently on every node 
	 * and deletes expired queries.
	 * @return
	 */
	public int deleteExpiredSearchQueries()
	{
		long currTime = System.currentTimeMillis();
		int rumRowsDeleted = -1;
		
		String tableName = HyperspaceDB.ATTR_INDEX_TRIGGER_TABLE_NAME;
		
		String deleteCommand = "DELETE FROM "+tableName+" WHERE expiryTime <= "+currTime;
		Connection myConn 	= null;
		Statement stmt 		= null;
		
		try
		{
			myConn = this.dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt = myConn.createStatement();
			rumRowsDeleted = stmt.executeUpdate(deleteCommand);
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
			DelayProfiler.updateDelay("deleteExpiredSearchQueries ", currTime);
		}
		
		return rumRowsDeleted;
	}
	
	
	private void returnAddedGroupGUIDs( JSONObject oldValJSON, JSONObject newUpdateVal, 
			HashMap<String, GroupGUIDInfoClass> newValGroupGUIDMap, JSONObject newUnsetAttrs, 
			boolean firstTimeInsert )
	{
		String tableName 			= HyperspaceDB.ATTR_INDEX_TRIGGER_TABLE_NAME;
		
		Connection myConn 			= null;
		Statement stmt 				= null;
		// for groups associated with the new value
		try
		{
			myConn 	     = this.dataSource.getConnection(DB_REQUEST_TYPE.SELECT);
			stmt   		 = myConn.createStatement();
			
			String selectQuery ="";
			if( firstTimeInsert )
			{	
				selectQuery = "SELECT groupGUID, userIP, userPort FROM "
						+tableName+" WHERE ";
				
				String newGroupsQuery = 
						getQueryToGetNewValueGroups
						( oldValJSON, newUpdateVal, 
								newUnsetAttrs);
				selectQuery = selectQuery + " groupGUID IN ( "+newGroupsQuery+" ) ";
			}
			else
			{
				String queriesWithAttrs = TriggerInformationStorage.getQueriesThatContainAttrsInUpdate
						(newUpdateVal);
				
				selectQuery = "SELECT groupGUID, userIP, userPort FROM "+tableName
						+" WHERE ";
				
				String newGroupsQuery = 
						getQueryToGetNewValueGroups
						( oldValJSON, newUpdateVal, 
								newUnsetAttrs);
				
				String oldGroupsQuery 
					= getQueryToGetOldValueGroups(oldValJSON);
				
				selectQuery = selectQuery 
						+ " groupGUID IN ( "+queriesWithAttrs+" ) AND "
						+ " groupGUID NOT IN ( "+oldGroupsQuery+" ) AND "
						+ " groupGUID IN ( "+newGroupsQuery+" ) ";
			}
		
			
			ResultSet rs = stmt.executeQuery(selectQuery);
			
			while( rs.next() )
			{
				byte[] groupGUIDBytes = rs.getBytes("groupGUID");
				String groupGUIDString = Utils.byteArrayToHex(groupGUIDBytes);
				byte[] ipAddressBytes = rs.getBytes("userIP");
				String userIPString = InetAddress.getByAddress(ipAddressBytes).getHostAddress();
				int userPort = rs.getInt("userPort");
				GroupGUIDInfoClass groupGUIDInfoClass = new GroupGUIDInfoClass(
						groupGUIDString, userIPString, userPort);
				newValGroupGUIDMap.put(groupGUIDString, groupGUIDInfoClass);
			}
			rs.close();
		} catch (SQLException e)
		{
			e.printStackTrace();
		} catch (JSONException e)
		{
			e.printStackTrace();
		} catch (UnknownHostException e)
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
	}
	
	
	public boolean checkAndInsertSearchQueryRecordFromPrimaryTriggerSubspace(String groupGUID, 
			String userIP, int userPort) 
					throws UnknownHostException
	{
		long t0 = System.currentTimeMillis();
		
		String tableName 			= HyperspaceDB.HASH_INDEX_TRIGGER_TABLE_NAME;
		
		Connection myConn 			= null;
		Statement stmt 				= null;
		
		String selectQuery 			= "SELECT * ";
		
		String ipInHex = Utils.byteArrayToHex(InetAddress.getByName(userIP).getAddress());
		
		selectQuery 				= selectQuery + " FROM "+tableName+" WHERE groupGUID = X'"+groupGUID
				+"'"+" AND userIP = X'"+ipInHex+"'"+" AND userPort = "+userPort;
		
		
		
		boolean found   = false;
		
		try
		{
			myConn 		 	= this.dataSource.getConnection(DB_REQUEST_TYPE.UPDATE);
			stmt 		 	= myConn.createStatement();
			ResultSet rs 	= stmt.executeQuery(selectQuery);	
			
			while( rs.next() )
			{
				found = true;
			}
			rs.close();
			
			if( !found )
			{
				String insertTableSQL = " INSERT INTO "+tableName 
						+" ( groupGUID, userIP, userPort ";
				
				insertTableSQL = insertTableSQL + " ) VALUES ( X'"+groupGUID+"', "+
								 " X'"+ipInHex+"', "+userPort+" ) ";
				
				stmt.executeUpdate(insertTableSQL);
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
			DelayProfiler.updateDelay("getSearchQueryRecordFromPrimaryTriggerSubspace", t0);
		}
		return found;
	}
	
	
	private String getPartitionInfoStorageString(String newTableCommand)
	{
		// query and default value mechanics
		//Attr specified in query but not set in GUID                  Do Not return GUID
		//Attr specified in query and  set in GUID                     Return GUID if possible

		//Attr not specified in query but  set in GUID                 Return GUID if possible 
		//Attr not specified in query and not set in GUID              Return GUID if possible as no privacy leak
		
		// creating for all attributes rather than just the attributes of the subspace for better matching
		Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
		
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
			String dataType = attrMetaInfo.getDataType();
			String minVal = attrMetaInfo.getMinValue();
			String maxVal = attrMetaInfo.getMaxValue();
			String defaultValue = attrMetaInfo.getDefaultValue();
				
			String mySQLDataType = AttributeTypes.mySQLDataType.get(dataType);			
				
			String lowerAttrName = "lower"+attrName;
			String upperAttrName = "upper"+attrName;
				
			String queryMinDefault = minVal;
			String queryMaxDefault = maxVal;
				
			// finding if default value is smaller or larger than min or max.
			// so that the query satisfies this case
			//Attr not specified in query and not set in GUID              Return GUID if possible as no privacy leak
			if(AttributeTypes.compareTwoValues(defaultValue, minVal, dataType))
			{
				// default value smaller than min
				queryMinDefault = defaultValue;
				queryMaxDefault = maxVal;
			}
			else if(AttributeTypes.compareTwoValues(maxVal, defaultValue, dataType))
			{
				// maxVal is smaller than defaultValue
				queryMinDefault = minVal;
				queryMaxDefault = defaultValue;
			}
			else
			{
				System.out.println("defaultValue "+defaultValue+" minVal "+minVal
							+" maxVal "+maxVal);
				// this should not happen
				assert(false);
			}
				
			// changed it to min max for lower and upper value instead of default 
			// because we want a query to match for attributes that are not specified 
			// in the query, as those basically are don't care.
			newTableCommand = newTableCommand + " , "+lowerAttrName+" "+mySQLDataType
					+ " DEFAULT "
					+ AttributeTypes.convertStringToDataTypeForMySQL(queryMinDefault, dataType)
					+ " , "+upperAttrName+" "+mySQLDataType+" DEFAULT "
					+ AttributeTypes.convertStringToDataTypeForMySQL(queryMaxDefault, dataType)
					+ " , INDEX USING BTREE("+lowerAttrName+" , "+upperAttrName+")";
		}
		return newTableCommand;
	}
}