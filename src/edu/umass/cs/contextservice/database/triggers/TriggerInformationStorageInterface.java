
package edu.umass.cs.contextservice.database.triggers;

import java.net.UnknownHostException;
import java.util.HashMap;

import org.json.JSONObject;

import edu.umass.cs.contextservice.database.records.OverlappingInfoClass;
import edu.umass.cs.contextservice.queryparsing.ProcessingQueryComponent;

/**
 * This interface defines the trigger information storage 
 * tables and its search and update calls.
 * @author adipc
 *
 */
public interface TriggerInformationStorageInterface<NodeIDType>
{
	/**
	 * creates trigger tables
	 */
	public void createTables();
	
	
	/**
	 * 
	 * @param subspaceId
	 * @param replicaNum
	 * @param attrName
	 * @param matchingQueryComponent
	 * @return
	 */
	public HashMap<Integer, OverlappingInfoClass> 
		getOverlappingPartitionsInTriggers(int subspaceId, int replicaNum, String attrName, 
			ProcessingQueryComponent matchingQueryComponent);
	
	public void insertIntoTriggerPartitionInfo(int subspaceId, int replicaNum, String attrName, 
			int partitionNum, NodeIDType respNodeId);
	
	public void insertIntoSubspaceTriggerDataInfo( int subspaceId, int replicaNum, 
			String attrName, String userQuery, String groupGUID, String userIP, int userPort, 
			long expiryTimeFromNow );
	
	public void getTriggerDataInfo(int subspaceId, int replicaNum, String attrName, 
			JSONObject oldValJSON, JSONObject newUpdateVal, HashMap<String, JSONObject> oldValGroupGUIDMap, 
				HashMap<String, JSONObject> newValGroupGUIDMap, int oldOrNewOrBoth) throws InterruptedException;
	
	public int deleteExpiredSearchQueries( int subspaceId, 
			int replicaNum, String attrName );
	
	public boolean getSearchQueryRecordFromPrimaryTriggerSubspace(String groupGUID, 
			String userIP, int userPort) throws UnknownHostException;
}