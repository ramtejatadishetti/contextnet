package edu.umass.cs.contextservice.database.triggers;

import java.net.UnknownHostException;
import java.util.HashMap;

import org.json.JSONObject;

/**
 * This interface defines the trigger information storage 
 * tables and its search and update calls.
 * @author adipc
 */
public interface TriggerInformationStorageInterface<NodeIDType>
{
	/**
	 * Creates trigger tables
	 */
	public void createTables();
	
	public void insertIntoSubspaceTriggerDataInfo( int subspaceId, 
			String userQuery, String groupGUID, String userIP, int userPort, 
			long expiryTimeFromNow );
	
	public void getTriggerDataInfo( int subspaceId,  
			JSONObject oldValJSON, JSONObject newUpdateVal, 
			HashMap<String, GroupGUIDInfoClass> oldValGroupGUIDMap, 
			HashMap<String, GroupGUIDInfoClass> newValGroupGUIDMap, 
			int requestType, JSONObject newUnsetAttrs,
			boolean firstTimeInsert )
						throws InterruptedException;
	
	public int deleteExpiredSearchQueries( int subspaceId);
	
	public boolean checkAndInsertSearchQueryRecordFromPrimaryTriggerSubspace( String groupGUID, 
			String userIP, int userPort )
					throws UnknownHostException;
}