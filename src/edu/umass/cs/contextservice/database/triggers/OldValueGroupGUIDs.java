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

import edu.umass.cs.contextservice.database.datasource.AbstractDataSource;
import edu.umass.cs.contextservice.logging.ContextServiceLogger;
import edu.umass.cs.contextservice.schemes.HyperspaceHashing;
import edu.umass.cs.contextservice.utils.Utils;

/**
 * 
 * Class created to parallelize old and new guids fetching
 * @author adipc
 */
public class OldValueGroupGUIDs implements Runnable
{
	private int subspaceId;
	private JSONObject oldValJSON;
	private JSONObject updateValJSON;
	private JSONObject newUnsetAttrs;
	private HashMap<String, GroupGUIDInfoClass> oldValGroupGUIDMap;
	private final AbstractDataSource dataSource;
	
	
	public OldValueGroupGUIDs(int subspaceId, JSONObject oldValJSON, 
			JSONObject updateValJSON, JSONObject newUnsetAttrs,
			HashMap<String, GroupGUIDInfoClass> oldValGroupGUIDMap,
			AbstractDataSource dataSource )
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
		
		try
		{
			String queriesWithAttrs 
				= TriggerInformationStorage.getQueriesThatContainAttrsInUpdate
					(updateValJSON, subspaceId);
			//String newTableName = "projTable";
			
			//String createTempTable = "CREATE TEMPORARY TABLE "+
			//		newTableName+" AS ( "+queriesWithAttrs+" ) ";
			
			String oldGroupsQuery 
				= TriggerInformationStorage.getQueryToGetOldValueGroups
					(oldValJSON, subspaceId);
			
			String newGroupsQuery = TriggerInformationStorage.getQueryToGetNewValueGroups
					( oldValJSON, updateValJSON, 
							newUnsetAttrs, subspaceId );
			
			String removedGroupQuery = "SELECT groupGUID, userIP, userPort FROM "+tableName
					+ " WHERE "
					+ " groupGUID IN ( "+queriesWithAttrs+" ) AND "
 				    + " groupGUID IN ( "+oldGroupsQuery+" ) AND "
 				    + " groupGUID NOT IN ( "+ newGroupsQuery+" ) ";
			
			ContextServiceLogger.getLogger().fine("returnOldValueGroupGUIDs getTriggerInfo "
												+removedGroupQuery);
			myConn 	     = dataSource.getConnection();
			stmt   		 = myConn.createStatement();
			
			ResultSet rs = stmt.executeQuery(removedGroupQuery);
			
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