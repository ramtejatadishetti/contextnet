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
import edu.umass.cs.contextservice.database.DataSource;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.schemes.HyperspaceHashing;
import edu.umass.cs.contextservice.utils.Utils;

/**
 * Class created to parallelize old and new guids fetching
 * @author adipc
 *
 */
public class OldValueGroupGUIDs<NodeIDType> implements Runnable
{
	private int subspaceId;
	private int replicaNum;
	private String attrName;
	private JSONObject oldValJSON;
	private HashMap<String, JSONObject> oldValGroupGUIDMap;
	private final DataSource<NodeIDType> dataSource;
	
	public OldValueGroupGUIDs(int subspaceId, int replicaNum, String attrName, 
			JSONObject oldValJSON, HashMap<String, JSONObject> oldValGroupGUIDMap,
			DataSource<NodeIDType> dataSource )
	{
		this.subspaceId = subspaceId;
		this.replicaNum = replicaNum;
		this.attrName = attrName;
		this.oldValJSON = oldValJSON;
		this.oldValGroupGUIDMap = oldValGroupGUIDMap;
		this.dataSource = dataSource;
	}
	@Override
	public void run() 
	{
		returnOldValueGroupGUIDs(subspaceId, replicaNum, attrName, 
				oldValJSON, oldValGroupGUIDMap);
	}
	
	private void returnOldValueGroupGUIDs( int subspaceId, int replicaNum, String attrName, 
			JSONObject oldValJSON, HashMap<String, JSONObject> oldValGroupGUIDMap )
	{
		String tableName 			= "subspaceId"+subspaceId+"RepNum"+replicaNum
										+"Attr"+attrName+"TriggerDataInfo";
		
		JSONObject oldUnsetAttrs 	= HyperspaceHashing.getUnsetAttrJSON(oldValJSON);
		
		assert( oldUnsetAttrs != null );
		
		Connection myConn 			= null;
		Statement stmt 				= null;
		
		//FIXME: it could be changed to calculating tobe removed GUIDs right here.
		// in one single mysql query once can check to old group guid and new group guids
		// and return groupGUIDs which are in old value but no in new value.
		// but usually complex queries have more cost, so not sure if it would help.
		// but this optimization can be checked later on if number of group guids returned becomes 
		// an issue later on. 
		
		
		// there is always at least one replica
		//SubspaceInfo<NodeIDType> currSubInfo = subspaceInfoMap.get(subspaceId).get(replicaNum);
		
//		HashMap<String, AttributePartitionInfo> attrSubspaceInfo 
//												= currSubInfo.getAttributesOfSubspace();
		
		Iterator<String> attrIter = AttributeTypes.attributeMap.keySet().iterator();
		//		attrSubspaceInfo.keySet().iterator();
		// for groups associated with old value
		try
		{
			boolean first = true;
			String selectQuery = "SELECT groupGUID, userIP, userPort FROM "+tableName+" WHERE ";
			
			while( attrIter.hasNext() )
			{
				String currAttrName = attrIter.next();
				
				AttributeMetaInfo attrMetaInfo = AttributeTypes.attributeMap.get(currAttrName);
				
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
			
			//oldValGroupGUIDs = new JSONArray();
			ContextServiceLogger.getLogger().fine("getTriggerInfo "+selectQuery);
			myConn 	     = dataSource.getConnection();
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
				oldValGroupGUIDMap.put(groupGUIDString, tableRow);
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
}