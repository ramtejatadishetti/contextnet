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
import edu.umass.cs.contextservice.attributeInfo.AttributeTypes.DomainPartitionInfo;
import edu.umass.cs.contextservice.config.ContextServiceConfig;
import edu.umass.cs.contextservice.database.DataSource;
import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.hyperspace.storage.AttributePartitionInfo;
import edu.umass.cs.contextservice.hyperspace.storage.SubspaceInfo;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.messages.UpdateTriggerMessage;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;
import edu.umass.cs.contextservice.queryparsing.QueryInfo;
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
					
					//int replicaNum = subInfo.getReplicaNum();
					
					//HashMap<String, AttributePartitionInfo> subspaceAttributes = subInfo.getAttributesOfSubspace();

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
		int replicaNum = subInfo.getReplicaNum();
		// creating for all attributes rather than just the attributes of the subspace for better mataching
		
		// at least one replica and all replica have same default value for each attribute.
		// FIXME: replicas may not have same default value for each attribute, because they can have 
		// different number of nodes. But it may not changes number of partitions. Need to check.
		// can be easily fixed by setting default value to partition 0 .but for now set to all partitions for load balancing/uniform.
		HashMap<String, AttributePartitionInfo> attrSubspaceMap = subInfo.getAttributesOfSubspace();
		
		Iterator<String> attrIter = attrSubspaceMap.keySet().iterator();
		while( attrIter.hasNext() )
		{
			String attrName = attrIter.next();
			AttributePartitionInfo attrPartInfo = attrSubspaceMap.get(attrName);
			AttributeMetaInfo attrMetaInfo = attrPartInfo.getAttrMetaInfo();
			String dataType = attrMetaInfo.getDataType();
			String defaultVal = attrMetaInfo.getDefaultValue();
			String mySQLDataType = AttributeTypes.mySQLDataType.get(dataType);
			
			// partition info storage info
			String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"Attr"+attrName+"TriggerPartitionInfo";
			
			String newTableCommand = "create table "+tableName+" ( hashCode INTEGER PRIMARY KEY , "
				      + "   respNodeID INTEGER ";
			
			String lowerAttrName = "lower"+attrName;
			String upperAttrName = "upper"+attrName;
			
			newTableCommand = newTableCommand + " , "+lowerAttrName+" "+mySQLDataType
					+" DEFAULT "+AttributeTypes.convertStringToDataTypeForMySQL(defaultVal, dataType)
					+ " , "+upperAttrName+" "+mySQLDataType+" DEFAULT "
					+AttributeTypes.convertStringToDataTypeForMySQL(defaultVal, dataType)
					+ " , INDEX USING BTREE("+lowerAttrName+" , "+upperAttrName+")";
			
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
			
			// creating separate query storage tables;
			// creating trigger guid storage
			tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"Attr"+attrName+"TriggerDataInfo";
			
			newTableCommand = "create table "+tableName+" ( groupGUID BINARY(20) NOT NULL , "
					+ "userIP Binary(4) NOT NULL ,  userPort INTEGER NOT NULL , expiryTime BIGINT NOT NULL ";
			newTableCommand = getPartitionInfoStorageString(newTableCommand);
			
			newTableCommand = newTableCommand +" , PRIMARY KEY(groupGUID, userIP, userPort), INDEX USING BTREE(expiryTime) )";
			stmt.executeUpdate(newTableCommand);
		}
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
				//String defaultVal = attrPartInfo.getDefaultValue();
				String minVal = attrMetaInfo.getMinValue();
				String maxVal = attrMetaInfo.getMaxValue();
				String mySQLDataType = AttributeTypes.mySQLDataType.get(dataType);
//				newTableCommand = newTableCommand + ", "+attrName+" "+mySQLDataType+" DEFAULT "+AttributeTypes.convertStringToDataTypeForMySQL(defaultVal, dataType)
//						+" , INDEX USING BTREE("+attrName+")";
				
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
	 * Returns a list of nodes that overlap with a query in a trigger 
	 * partitions single subspaces
	 * @param subspaceNum
	 * @param qcomponents, takes matching attributes as input
	 * @return
	 */
	public HashMap<Integer, OverlappingInfoClass> 
		getOverlappingPartitionsInTriggers(int subspaceId, int replicaNum, String attrName, 
				ProcessingQueryComponent matchingQueryComponent)
	{
		long t0 = System.currentTimeMillis();
		HashMap<Integer, OverlappingInfoClass> answerList 
						= new HashMap<Integer, OverlappingInfoClass>();
		
		String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"Attr"+attrName+"TriggerPartitionInfo";
		
		String selectTableSQL = "SELECT hashCode, respNodeID from "+tableName+" WHERE ";
		
		String lowerAttr = "lower"+attrName;
		String upperAttr = "upper"+attrName;
		
		AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
		String dataType = attrMetaInfo.getDataType();
		
		if( AttributeTypes.compareTwoValues(matchingQueryComponent.getLowerBound(),
				matchingQueryComponent.getUpperBound(), dataType) )
		{
			String queryMin  
			=  AttributeTypes.convertStringToDataTypeForMySQL(matchingQueryComponent.getLowerBound(), dataType) + "";
			String queryMax  
			=  AttributeTypes.convertStringToDataTypeForMySQL(matchingQueryComponent.getUpperBound(), dataType) + "";
			
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
			String queryMin  
			=  AttributeTypes.convertStringToDataTypeForMySQL(matchingQueryComponent.getLowerBound(), dataType) + "";
			String queryMax  
			=  AttributeTypes.convertStringToDataTypeForMySQL(attrMetaInfo.getMaxValue(), dataType) + "";
			
			selectTableSQL = selectTableSQL +"( ( "
					+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
					+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
					+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" ) OR ";
			
			// second case from minvalue to upper val
			queryMin  
			=  AttributeTypes.convertStringToDataTypeForMySQL(attrMetaInfo.getMinValue(), dataType) + "";
			queryMax  
			=  AttributeTypes.convertStringToDataTypeForMySQL(matchingQueryComponent.getUpperBound(), dataType) + "";
			
			selectTableSQL = selectTableSQL +"( "
					+ "( "+lowerAttr+" <= "+queryMin +" AND "+upperAttr+" > "+queryMin+" ) OR "
					+ "( "+lowerAttr+" <= "+queryMax +" AND "+upperAttr+" > "+queryMax+" ) OR "
					+ "( "+lowerAttr+" >= "+queryMin +" AND "+upperAttr+" <= "+queryMax+" ) "+" )  )";
		}
		
		Statement stmt 		= null;
		Connection myConn 	= null;
		try
		{
			myConn = this.dataSource.getConnection();
			stmt = myConn.createStatement();
			ContextServiceLogger.getLogger().fine("selectTableSQL "+selectTableSQL);
			ResultSet rs = stmt.executeQuery(selectTableSQL);
		    while( rs.next() )
		    {
		    	//Retrieve by column name
		    	int respNodeID  	 = rs.getInt("respNodeID");
		    	//int hashCode		 = rs.getInt("hashCode");
		    	OverlappingInfoClass overlapObj = new OverlappingInfoClass();
		    	
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
			DelayProfiler.updateDelay("getOverlappingPartitionsInTriggers", t0);
		}
		return answerList;
	}

	
	/**
	 * Inserts a subspace region denoted by subspace vector, 
	 * integer denotes partition num in partition info 
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoTriggerPartitionInfo(int subspaceId, int replicaNum, String attrName, 
			int partitionNum, NodeIDType respNodeId)
	{
		long t0 			= System.currentTimeMillis();
		Connection myConn   = null;
		Statement stmt      = null;
		
		String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"Attr"+attrName+"TriggerPartitionInfo";
		
		SubspaceInfo<NodeIDType> currSubInfo = subspaceInfoMap.
				get(subspaceId).get(replicaNum);
	
		
		HashMap<String, AttributePartitionInfo> attrSubspaceInfo = currSubInfo.getAttributesOfSubspace();
		
		//String insertTableSQL = "SET unique_checks=0; INSERT INTO "+tableName 
		String insertTableSQL = "INSERT INTO "+tableName 
				+" ( hashCode, respNodeID ";
		
		String lowerAtt = "lower"+attrName;
		String upperAtt = "upper"+attrName;
		
		insertTableSQL = insertTableSQL + ", "+lowerAtt+" , "+upperAtt;
		
		insertTableSQL = insertTableSQL + " ) VALUES ( "+partitionNum + 
				" , "+respNodeId;
		
		AttributePartitionInfo attrPartInfo = attrSubspaceInfo.get(attrName);
		DomainPartitionInfo domainPartInfo = attrPartInfo.getTriggerDomainPartitionInfo().get(partitionNum);
		// if it is a String then single quotes needs to be added
			
		AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(attrName);
		String dataType = attrMetaInfo.getDataType();
			
		String lowerBound  = AttributeTypes.convertStringToDataTypeForMySQL(domainPartInfo.lowerbound, dataType)+"";
		String upperBound  = AttributeTypes.convertStringToDataTypeForMySQL(domainPartInfo.upperbound, dataType)+"";
			
		insertTableSQL = insertTableSQL + " , "+lowerBound+" , "+ 
					upperBound;
		
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
			DelayProfiler.updateDelay("insertIntoTriggerPartitionInfo", t0);
		}
	}
	
	
	/**
	 * Inserts trigger info on a query into the table
	 * @param subspaceNum
	 * @param subspaceVector
	 */
	public void insertIntoSubspaceTriggerDataInfo( int subspaceId, int replicaNum, 
			String attrName, String userQuery, String groupGUID, String userIP, int userPort, 
			long expiryTimeFromNow )
	{
		long t0 			= System.currentTimeMillis();
		Connection myConn   = null;
		Statement stmt      = null;
		
		String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"Attr"+attrName+"TriggerDataInfo";
		
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
	public void getTriggerDataInfo(int subspaceId, int replicaNum, String attrName, 
		JSONObject oldValJSON, JSONObject newUpdateVal, HashMap<String, JSONObject> oldValGroupGUIDMap, 
			HashMap<String, JSONObject> newValGroupGUIDMap, int oldOrNewOrBoth, JSONObject newUnsetAttrs) throws InterruptedException
	{
		assert(oldValGroupGUIDMap != null);
		assert(newValGroupGUIDMap != null);
		// oldValJSON should contain all attribtues.
		// newUpdateVal contains only updated attr:val pairs
		//assert(oldValJSON.length() == AttributeTypes.attributeMap.size());
		
		long t0 = System.currentTimeMillis();
		
		
		if( oldOrNewOrBoth == UpdateTriggerMessage.OLD_VALUE )
		{
			OldValueGroupGUIDs<NodeIDType> old = new OldValueGroupGUIDs<NodeIDType>
			(subspaceId, replicaNum, attrName, oldValJSON, oldValGroupGUIDMap,
					dataSource);
			old.run();
			//returnOldValueGroupGUIDs(subspaceId, replicaNum, attrName, oldValJSON, oldValGroupGUIDMap);
		}
		else if( oldOrNewOrBoth == UpdateTriggerMessage.NEW_VALUE )
		{
			returnNewValueGroupGUIDs( subspaceId, replicaNum, attrName, oldValJSON, 
					newUpdateVal, newValGroupGUIDMap, newUnsetAttrs);
		}
		else if( oldOrNewOrBoth == UpdateTriggerMessage.BOTH )
		{
			// both old and new value GUIDs stored at same nodes,
			// makes it possible to find which groupGUIDs needs to be triggered.
			// in parallel
			OldValueGroupGUIDs<NodeIDType> old = new OldValueGroupGUIDs<NodeIDType>
					(subspaceId, replicaNum, attrName, 
					oldValJSON, oldValGroupGUIDMap, dataSource);
			Thread st = new Thread(old);
			st.start();			
//			returnOldValueGroupGUIDs(subspaceId, replicaNum, attrName, oldValJSON, oldValGroupGUIDMap);
			returnNewValueGroupGUIDs( subspaceId, replicaNum, attrName, oldValJSON, 
					newUpdateVal, newValGroupGUIDMap, newUnsetAttrs );
			st.join();
		}
		
		if( ContextServiceConfig.DELAY_PROFILER_ON )
		{
			DelayProfiler.updateDelay("getTriggerInfo", t0);
		}
	}
	
	
	/**
	 * this function runs independently on every node 
	 * and deletes expired queries.
	 * @return
	 */
	public int deleteExpiredSearchQueries( int subspaceId, int replicaNum, String attrName )
	{
		long currTime = System.currentTimeMillis();
		int rumRowsDeleted = -1;
		
		String tableName = "subspaceId"+subspaceId+"RepNum"+replicaNum+"Attr"+attrName+"TriggerDataInfo";
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
	
	
	private void returnNewValueGroupGUIDs( int subspaceId, int replicaNum, String attrName, 
			JSONObject oldValJSON, JSONObject newUpdateVal, 
			HashMap<String, JSONObject> newValGroupGUIDMap, JSONObject newUnsetAttrs )
	{
		String tableName 			= "subspaceId"+subspaceId+"RepNum"
							+replicaNum+"Attr"+attrName+"TriggerDataInfo";
		
		Connection myConn 			= null;
		Statement stmt 				= null;
		
		// there is always at least one replica
//		SubspaceInfo<NodeIDType> currSubInfo = subspaceInfoMap.get(subspaceId).get(0);
//		
//		HashMap<String, AttributePartitionInfo> attrSubspaceInfo 
//												= currSubInfo.getAttributesOfSubspace();
		
		Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
		// for groups associated with the new value
		try
		{
			boolean first = true;
			String selectQuery = "SELECT groupGUID, userIP, userPort FROM "+tableName+" WHERE ";
			while( attrIter.hasNext() )
			{
				String currAttrName = attrIter.next();
				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(currAttrName);
				
				String dataType = attrMetaInfo.getDataType();
				
				String attrValForMysql = attrMetaInfo.getDefaultValue();
				
				if( !newUnsetAttrs.has(currAttrName) )
				{
					if( newUpdateVal.has(currAttrName) )
					{
						attrValForMysql =
						AttributeTypes.convertStringToDataTypeForMySQL
						(newUpdateVal.getString(currAttrName), dataType)+"";
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
		
			myConn 	     = this.dataSource.getConnection();
			stmt   		 = myConn.createStatement();
			ResultSet rs = stmt.executeQuery(selectQuery);
			
			while( rs.next() )
			{
				JSONObject tableRow = new JSONObject();
				byte[] groupGUIDBytes = rs.getBytes("groupGUID");
				String groupGUIDString = Utils.bytArrayToHex(groupGUIDBytes);
				byte[] ipAddressBytes = rs.getBytes("userIP");
				String userIPStirng = InetAddress.getByAddress(ipAddressBytes).getHostAddress();
				//tableRow.put( "userQuery", rs.getString("userQuery") );
				tableRow.put( "groupGUID", groupGUIDString );
				tableRow.put( "userIP", userIPStirng );
				tableRow.put( "userPort", rs.getInt("userPort") );
				newValGroupGUIDMap.put(groupGUIDString, tableRow);
			}
			//ContextServiceLogger.getLogger().fine("NodeId "+this.myNodeID+" getGUIDRecordFromPrimarySubspace guid "
			//		+ ""+GUID+" oldValueJSON size "+oldValueJSON.length()+"oldValueJSON "+oldValueJSON);
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
	
	
	public boolean getSearchQueryRecordFromPrimaryTriggerSubspace(String groupGUID, 
			String userIP, int userPort) throws UnknownHostException
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