package edu.umass.cs.contextservice.client;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * 
 * implements the method to be used in secure 
 * context service client, which implements the privacy scheme.
 * One thing to note: everything guid, public key and private key is represented as byt[]
 * to half the space requirements. Keys are of 100s of bytes so storing them as string 
 * double the space 200s of bytes.
 * @author adipc
 */
public interface SecureContextClientInterface
{
	// 20 bytes
	public static final int SIZE_OF_ANONYMIZED_ID			= 20;
	/**
	 * @param myGUIDInfo contains a user's GUID, public key, and private key
	 * @param ACLInfo Each element is JSONObject, each JSON contains attrName, ACL member's GUID and public key
	 * @param attrValuePairs attr value pairs for update
	 * @param versionNum 
	 * @param blocking true then the update will block until CS confirms completion.
	 */
	public void sendUpdateSecure(GUIDEntryStoringClass myGUIDInfo, JSONArray ACLArray, JSONArray anonymizedIDs, 
			JSONObject attrValuePairs, long versionNum, boolean blocking);
	
	/**
	 * computes anonymized Ids for a user
	 * @param myGUIDInfo the calling user's guid info
	 * @param ACLArray the calling user's ACL
	 * @return
	 */
	public JSONArray computeAnonymizedIDs(GUIDEntryStoringClass myGUIDInfo, JSONArray ACLArray);
	
	//FIXME: semantics needs to be decided, after secure update/insert is implemented
	public int sendSearchQuerySecure(GUIDEntryStoringClass myGUIDInfo, String searchQuery, JSONArray replyArray, long expiryTime);
	
	//FIXME: semantics needs to be decided, after secure update/insert is implemented
	public JSONObject sendGetRequestSecure(GUIDEntryStoringClass myGUIDInfo, String GUID);
}