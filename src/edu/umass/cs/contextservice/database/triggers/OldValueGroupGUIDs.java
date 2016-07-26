package edu.umass.cs.contextservice.database.triggers;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.database.DataSource;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.schemes.HyperspaceHashing;
import edu.umass.cs.contextservice.utils.Utils;

/**
 * 
 * Class created to parallelize old and new guids fetching
 * @author adipc
 */
public class OldValueGroupGUIDs<NodeIDType> implements Runnable
{
	private int subspaceId;
	//private int replicaNum;
	//private String attrName;
	private JSONObject oldValJSON;
	private JSONObject updateValJSON;
	private JSONObject newUnsetAttrs;
	private HashMap<String, GroupGUIDInfoClass> oldValGroupGUIDMap;
	private final DataSource<NodeIDType> dataSource;
	
	
	public OldValueGroupGUIDs(int subspaceId, JSONObject oldValJSON, 
			JSONObject updateValJSON, JSONObject newUnsetAttrs,
			HashMap<String, GroupGUIDInfoClass> oldValGroupGUIDMap,
			DataSource<NodeIDType> dataSource )
	{
		this.subspaceId = subspaceId;
		this.oldValJSON = oldValJSON;
		this.updateValJSON = updateValJSON;
		this.newUnsetAttrs = newUnsetAttrs;
		this.oldValGroupGUIDMap = oldValGroupGUIDMap;
		this.dataSource = dataSource;
	}
	@Override
	public void run() 
	{
		returnRemovedGroupGUIDs();
	}
	
	private void returnRemovedGroupGUIDs()
	{
		String tableName 			= "subspaceId"+subspaceId+"TriggerDataInfo";
		
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
		
		
		// there is always at least one replica
		//SubspaceInfo<NodeIDType> currSubInfo = subspaceInfoMap.get(subspaceId).get(replicaNum);
		
//		HashMap<String, AttributePartitionInfo> attrSubspaceInfo 
//												= currSubInfo.getAttributesOfSubspace();
		
		//Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
		//		attrSubspaceInfo.keySet().iterator();
		// for groups associated with old value
		try
		{
			//boolean first = true;
			//String selectQuery = "SELECT groupGUID, userIP, userPort FROM "+tableName+" WHERE ";
			String oldGroupsQuery 
				= TriggerInformationStorage.getQueryToGetOldValueGroups(oldValJSON, subspaceId);
			
			String newGroupsQuery = TriggerInformationStorage.getQueryToGetNewValueGroups
					( oldValJSON, updateValJSON, 
							newUnsetAttrs, subspaceId );
			
			String removedGroupQuery = "SELECT groupGUID, userIP, userPort FROM "
					+ tableName+" WHERE "
				    + " groupGUID IN ( "+oldGroupsQuery+" ) AND groupGUID NOT IN ( "
					+ newGroupsQuery+" ) ";
			
//			while( attrIter.hasNext() )
//			{
//				String currAttrName = attrIter.next();
//				
//				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(currAttrName);
//				
//				String dataType = attrMetaInfo.getDataType();
//				
//				String attrValForMysql = "";
//				
//				if( oldUnsetAttrs.has(currAttrName) )
//				{
//					attrValForMysql = attrMetaInfo.getDefaultValue();
//				}
//				else
//				{
//					attrValForMysql = AttributeTypes.convertStringToDataTypeForMySQL
//							(oldValJSON.getString(currAttrName), dataType)+"";
//				}	
//				
//				String lowerValCol = "lower"+currAttrName;
//				String upperValCol = "upper"+currAttrName;
//				//FIXME: for circular queries, this won't work.
//				if( first )
//				{
//					// <= and >= both to handle the == case of the default value
//					selectQuery = selectQuery + lowerValCol+" <= "+attrValForMysql
//							+" AND "+upperValCol+" >= "+attrValForMysql;
//					first = false;
//				}
//				else
//				{
//					selectQuery = selectQuery+" AND "+lowerValCol+" <= "+attrValForMysql
//							+" AND "+upperValCol+" >= "+attrValForMysql;
//				}
//			}
			
			//oldValGroupGUIDs = new JSONArray();
			ContextServiceLogger.getLogger().fine("returnOldValueGroupGUIDs getTriggerInfo "
												+removedGroupQuery);
			myConn 	     = dataSource.getConnection();
			stmt   		 = myConn.createStatement();
			ResultSet rs = stmt.executeQuery(removedGroupQuery);
			
			while( rs.next() )
			{
				//JSONObject tableRow = new JSONObject();
				// FIXME: need to replace these with macros
				byte[] groupGUIDBytes = rs.getBytes("groupGUID");
				String groupGUIDString = Utils.bytArrayToHex(groupGUIDBytes);
				byte[] ipAddressBytes = rs.getBytes("userIP");
				String userIPString = InetAddress.getByAddress(ipAddressBytes).getHostAddress();
				int userPort = rs.getInt("userPort");
				
				//tableRow.put( "userQuery", rs.getString("userQuery") );
//				tableRow.put( "groupGUID", groupGUIDString );
//				tableRow.put( "userIP", userIPStirng );
//				tableRow.put( "userPort", rs.getInt("userPort") );
				GroupGUIDInfoClass groupGUIDInfo = new GroupGUIDInfoClass(
						groupGUIDString, userIPString, userPort);
				oldValGroupGUIDMap.put(groupGUIDString, groupGUIDInfo);
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
}