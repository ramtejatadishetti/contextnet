package edu.umass.cs.contextservice.database.triggers;

import java.net.UnknownHostException;
import java.util.HashMap;

import org.json.JSONObject;

/**
 * This interface defines the trigger information storage 
 * tables and its search and update calls.
 * @author adipc
 */
public interface TriggerInformationStorageInterface
{
	/**
	 * Creates trigger tables
	 */
	public void createTriggerStorageTables();
	
	public void insertIntoTriggerDataStorage( String userQuery, 
			String groupGUID, String userIP, int userPort, 
			long expiryTimeFromNow );
	
	public void getTriggerDataInfo( JSONObject oldValJSON, JSONObject updateAttrJSON, 
			HashMap<String, GroupGUIDInfoClass> oldValGroupGUIDMap, 
			HashMap<String, GroupGUIDInfoClass> newValGroupGUIDMap, 
			int requestType, JSONObject newUnsetAttrs,
			boolean firstTimeInsert )
						throws InterruptedException;
	
	public int deleteExpiredSearchQueries();
	
	public boolean checkAndInsertSearchQueryRecordFromPrimaryTriggerSubspace( String groupGUID, 
			String userIP, int userPort )
					throws UnknownHostException;
}