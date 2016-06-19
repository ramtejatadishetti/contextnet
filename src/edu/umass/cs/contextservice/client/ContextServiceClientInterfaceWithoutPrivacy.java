package edu.umass.cs.contextservice.client;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.gnsclient.client.GuidEntry;

/**
 * This interface defines context service calls without privacy
 * @author adipc
 *
 */
public interface ContextServiceClientInterfaceWithoutPrivacy 
{
	/**
	 * send update to context service.
	 * If blocking is set to true then the call blocks until the update is complete.
	 * If false then the call just returns after forwarding update to the context service.
	 * @param GUID
	 * @param attrValuePairs
	 * @param versionNum
	 * @param blocking
	 */
	public void sendUpdate(String GUID, GuidEntry myGuidEntry, 
			JSONObject attrValuePairs, long versionNum);
	
	//blocking call
	/**
	 * Context service search API call.
	 * result is returned in resultGUIDMap.
	 * resultGUIDMap map is also internally updated on refresh trigger.
	 * expiry time denotes the time for which this query will be active.
	 * @param searchQuery
	 * @param resultGUIDMap
	 * @param expiryTime
	 */
	public int sendSearchQuery(String searchQuery, JSONArray replyArray, 
			long expiryTime);
	
	// blocking call
	/**
	 * performs a get object, based on GUID
	 * @param GUID
	 * @return
	 */
	public JSONObject sendGetRequest(String GUID);
	
	/**
	 * Returns query update triggers as a JSONArray,
	 * Each JSONObject in the trigger is a RefreshTrigger meesage in
	 * JSONObject form
	 * @return
	 */
	public  void getQueryUpdateTriggers(JSONArray triggerArray);
}