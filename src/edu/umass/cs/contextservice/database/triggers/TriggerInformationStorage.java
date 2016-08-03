package edu.umass.cs.contextservice.database.triggers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.attributeInfo.AttributeMetaInfo;
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.DataSource;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.ValueUpdateToSubspaceRegionMessage;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
import edu.umass.cs.contextservice.schemes.HyperspaceHashing;
import edu.umass.cs.contextservice.utils.Utils;
import edu.umass.cs.utils.DelayProfiler;

/**
 * Implements the trigger storage table creation
 * and search and update trigger storage.
 * @author adipc
 *
 */
public class TriggerInformationStorage<NodeIDType> implements TriggerInformationStorageInterface<NodeIDType>
{
	private final NodeIDType myNodeID;
	private final HashMap<Integer, Vector<SubspaceInfo<NodeIDType>>> subspaceInfoMap;
	private final DataSource<NodeIDType> dataSource;
	
	public TriggerInformationStorage( NodeIDType myNodeID, 
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

					// currently it is assumed that there are only conjunctive queries
					// DNF form queries can be added by inserting its multiple conjunctive components.
					ContextServiceLogger.getLogger().fine( "HyperspaceMySQLDB "
								+ " TRIGGER_ENABLED "+ContextServiceConfig.TRIGGER_ENABLED );					
					createTablesForTriggers(subInfo, stmt);
				}
			}
			
			if( ContextServiceConfig.TRIGGER_ENABLED && ContextServiceConfig.UniqueGroupGUIDEnabled )
			{
				// currently it is assumed that there are only conjunctive queries
				// DNF form queries can be added by inserting its multiple conjunctive components.
				//ContextServiceLogger.getLogger().fine( "HyperspaceMySQLDB "
				//		+ " TRIGGER_ENABLED "+ContextServiceConfig.TRIGGER_ENABLED );					
				//createTablesForTriggers(subInfo, stmt);
				
				// for storing the trigger data, which is search queries
				
				String tableName = "primarySubspaceTriggerDataStorage";
				
				String newTableCommand = "create table "+tableName+" ( groupGUID BINARY(20) NOT NULL , "
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
	 * creates one dimensional subspaces and query storage tables for triggers
	 * @throws SQLException 
	 */
	private void createTablesForTriggers(SubspaceInfo<NodeIDType> subInfo, Statement  stmt) throws SQLException
	{
		int subspaceId = subInfo.getSubspaceId();
		//int replicaNum = subInfo.getReplicaNum();
		// creating for all attributes rather than just the attributes of the subspace for better mataching
		
		// at least one replica and all replica have same default value for each attribute.
		// FIXME: replicas may not have same default value for each attribute, because they can have 
		// different number of nodes. But it may not changes number of partitions. Need to check.
		// can be easily fixed by setting default value to partition 0 .but for now set to all partitions for load balancing/uniform.
		//HashMap<String, AttributePartitionInfo> attrSubspaceMap = subInfo.getAttributesOfSubspace();
		
		if( !subInfo.checkIfSubspaceHasMyID(myNodeID) )
		{
			return;
		}
		
		String tableName = "subspaceId"+subspaceId+"TriggerDataInfo";
		
		String newTableCommand = "create table "+tableName+" ( groupGUID BINARY(20) NOT NULL , "
				+ "userIP Binary(4) NOT NULL ,  userPort INTEGER NOT NULL , expiryTime BIGINT NOT NULL ";
		newTableCommand = getPartitionInfoStorageString(newTableCommand);
		
		newTableCommand = newTableCommand +" , PRIMARY KEY(groupGUID, userIP, userPort), INDEX USING BTREE(expiryTime) )";
		stmt.executeUpdate(newTableCommand);
	}
	
	private String getPartitionInfoStorageString(String newTableCommand)
	{
		// creating for all attributes rather than just the attributes of the subspace for better mataching
		Iterator<Integer> subapceIdIter = subspaceInfoMap.keySet().iterator();
		while(subapceIdIter.hasNext())
		{
			int subspaceId = subapceIdIter.next();
			// at least one replica and all replica have same default value for each attribute.
			SubspaceInfo<NodeIDType> currSubspaceInfo = subspaceInfoMap.get(subspaceId).get(0);
			HashMap<String, AttributePartitionInfo> attrSubspaceMap = currSubspaceInfo.getAttributesOfSubspace();
			
			Iterator<String> attrIter = attrSubspaceMap.keySet().iterator();
			while(attrIter.hasNext())
			{
				String attrName = attrIter.next();
				AttributePartitionInfo attrPartInfo = attrSubspaceMap.get(attrName);
				AttributeMetaInfo attrMetaInfo = attrPartInfo.getAttrMetaInfo();
				String dataType = attrMetaInfo.getDataType();
				String minVal = attrMetaInfo.getMinValue();
				String maxVal = attrMetaInfo.getMaxValue();
				String mySQLDataType = AttributeTypes.mySQLDataType.get(dataType);			
				
				String lowerAttrName = "lower"+attrName;
				String upperAttrName = "upper"+attrName;
				
				
				// changed it to min max for lower and upper value instead of default 
				// because we want a query to match for attributes that are not specified 
				// in the query, as those basically are don't care.
				newTableCommand = newTableCommand + " , "+lowerAttrName+" "+mySQLDataType
						+" DEFAULT "+AttributeTypes.convertStringToDataTypeForMySQL(minVal, dataType)
						+ " , "+upperAttrName+" "+mySQLDataType+" DEFAULT "
						+AttributeTypes.convertStringToDataTypeForMySQL(maxVal, dataType)
						+ " , INDEX USING BTREE("+lowerAttrName+" , "+upperAttrName+")";			
			}
		}
		return newTableCommand;
	}
	
	/**
	 * Inserts trigger info on a query into the table
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoSubspaceTriggerDataInfo( int subspaceId, String userQuery, 
			String groupGUID, String userIP, int userPort, 
			long expiryTimeFromNow )
	{
		long t0 			= System.currentTimeMillis();
		Connection myConn   = null;
		Statement stmt      = null;
		
		String tableName = "subspaceId"+subspaceId+"TriggerDataInfo";
		
		QueryInfo<NodeIDType> processedQInfo = new QueryInfo<NodeIDType>(userQuery);
		HashMap<String, ProcessingQueryComponent> pqcMap = processedQInfo.getProcessingQC();
		
		String hexIP;
		try
		{
			hexIP = Utils.bytArrayToHex(InetAddress.getByName(userIP).getAddress());	
			
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
			
			myConn = this.dataSource.getConnection();
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
	public void getTriggerDataInfo(int subspaceId,  
		JSONObject oldValJSON, JSONObject newJSONToWrite, 
		HashMap<String, GroupGUIDInfoClass> oldValGroupGUIDMap, 
		HashMap<String, GroupGUIDInfoClass> newValGroupGUIDMap, 
		int requestType, JSONObject newUnsetAttrs,
		boolean firstTimeInsert) 
					throws InterruptedException
	{
		assert(oldValGroupGUIDMap != null);
		assert(newValGroupGUIDMap != null);
		// oldValJSON should contain all attribtues.
		// newUpdateVal contains only updated attr:val pairs
		//assert(oldValJSON.length() == AttributeTypes.attributeMap.size());
		long t0 = System.currentTimeMillis();
		
		
		if( requestType == ValueUpdateToSubspaceRegionMessage.REMOVE_ENTRY )
		{
			OldValueGroupGUIDs<NodeIDType> old = new OldValueGroupGUIDs<NodeIDType>
			(subspaceId, oldValJSON, newJSONToWrite, newUnsetAttrs, oldValGroupGUIDMap,
					dataSource);
			old.run();
		}
		else if( requestType == ValueUpdateToSubspaceRegionMessage.ADD_ENTRY )
		{
			returnAddedGroupGUIDs( subspaceId, oldValJSON, 
					newJSONToWrite, newValGroupGUIDMap, newUnsetAttrs, firstTimeInsert);
		}
		else if( requestType == ValueUpdateToSubspaceRegionMessage.UPDATE_ENTRY )
		{
			// first time insert is done as udpate, as it results reply from one node.
			// so we don't need to check for old groups to which this new GUID was part of.
			if(firstTimeInsert)
			{
				returnAddedGroupGUIDs( subspaceId, oldValJSON, 
						newJSONToWrite, newValGroupGUIDMap, newUnsetAttrs, firstTimeInsert );
			}
			else
			{
				// both old and new value GUIDs stored at same nodes,
				// makes it possible to find which groupGUIDs needs to be triggered.
				// in parallel
				OldValueGroupGUIDs<NodeIDType> old = new OldValueGroupGUIDs<NodeIDType>
				(subspaceId, oldValJSON, newJSONToWrite, newUnsetAttrs, oldValGroupGUIDMap,
						dataSource);
				Thread st = new Thread(old);
				st.start();
				returnAddedGroupGUIDs( subspaceId, oldValJSON, 
						newJSONToWrite, newValGroupGUIDMap, newUnsetAttrs, firstTimeInsert );
				st.join();
			}
		}
		
		if( ContextServiceConfig.DELAY_PROFILER_ON )
		{
			DelayProfiler.updateDelay("getTriggerInfo", t0);
		}
	}
	
	public static String getQueryToGetOldValueGroups(JSONObject oldValJSON, int subspaceId) throws JSONException
	{
		String tableName 			= "subspaceId"+subspaceId+"TriggerDataInfo";
		
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
			//FIXME: for circular queries, this won't work.
			if( first )
			{
				// <= and >= both to handle the == case of the default value
				selectQuery = selectQuery + lowerValCol+" <= "+attrValForMysql
						+" AND "+upperValCol+" >= "+attrValForMysql;
				first = false;
			}
			else
			{
				selectQuery = selectQuery+" AND "+lowerValCol+" <= "+attrValForMysql
						+" AND "+upperValCol+" >= "+attrValForMysql;
			}
		}
		return selectQuery;
	}
	
	public static String getQueryToGetNewValueGroups
				( JSONObject oldValJSON, JSONObject newJSONToWrite, 
						JSONObject newUnsetAttrs,int subspaceId ) throws JSONException
	{
		String tableName 			= "subspaceId"+subspaceId+"TriggerDataInfo";

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
				//FIXME: will not work for cicular queries
				if( first )
				{
					// <= and >= both to handle the == case of the default value
					selectQuery = selectQuery + lowerValCol+" <= "+attrValForMysql
							+" AND "+upperValCol+" >= "+attrValForMysql;
					first = false;
				}
				else
				{
					selectQuery = selectQuery+" AND "+lowerValCol+" <= "+attrValForMysql
							+" AND "+upperValCol+" >= "+attrValForMysql;
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
	public int deleteExpiredSearchQueries( int subspaceId )
	{
		long currTime = System.currentTimeMillis();
		int rumRowsDeleted = -1;
		
		String tableName = "subspaceId"+subspaceId+"TriggerDataInfo";
		String deleteCommand = "DELETE FROM "+tableName+" WHERE expiryTime <= "+currTime;
		Connection myConn 	= null;
		Statement stmt 		= null;
		
		try
		{
			myConn = this.dataSource.getConnection();
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
	
	
	private void returnAddedGroupGUIDs( int subspaceId, 
			JSONObject oldValJSON, JSONObject newUpdateVal, 
			HashMap<String, GroupGUIDInfoClass> newValGroupGUIDMap, JSONObject newUnsetAttrs, 
			boolean firstTimeInsert )
	{
		String tableName 			= "subspaceId"+subspaceId+"TriggerDataInfo";
		
		Connection myConn 			= null;
		Statement stmt 				= null;
		// for groups associated with the new value
		try
		{
			String selectQuery = "SELECT groupGUID, userIP, userPort FROM "+tableName+" WHERE ";
			if( firstTimeInsert )
			{
				String newGroupsQuery = 
						getQueryToGetNewValueGroups
						( oldValJSON, newUpdateVal, 
								newUnsetAttrs, subspaceId );
				selectQuery = selectQuery + " groupGUID IN ( "+newGroupsQuery+" ) ";
			}
			else
			{
				String newGroupsQuery = 
						getQueryToGetNewValueGroups
						( oldValJSON, newUpdateVal, 
								newUnsetAttrs, subspaceId );
				
				String oldGroupsQuery 
					= getQueryToGetOldValueGroups(oldValJSON, subspaceId);
				
				selectQuery = selectQuery + " groupGUID NOT IN ( "+oldGroupsQuery
						+" ) AND groupGUID IN ( "+newGroupsQuery+" ) ";
			}
		
			myConn 	     = this.dataSource.getConnection();
			stmt   		 = myConn.createStatement();
			ResultSet rs = stmt.executeQuery(selectQuery);
			
			while( rs.next() )
			{
				byte[] groupGUIDBytes = rs.getBytes("groupGUID");
				String groupGUIDString = Utils.bytArrayToHex(groupGUIDBytes);
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
		
		String tableName 			= "primarySubspaceTriggerDataStorage";
		
		Connection myConn 			= null;
		Statement stmt 				= null;
		
		String selectQuery 			= "SELECT * ";
		
		String ipInHex = Utils.bytArrayToHex(InetAddress.getByName(userIP).getAddress());
		
		selectQuery 				= selectQuery + " FROM "+tableName+" WHERE groupGUID = X'"+groupGUID
				+"'"+" AND userIP = X'"+ipInHex+"'"+" AND userPort = "+userPort;
		
		
		
		boolean found   = false;
		
		try
		{
			myConn 		 	= this.dataSource.getConnection();
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
}