package edu.umass.cs.contextservice.client;

import java.util.HashMap;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.gnsclient.client.GuidEntry;

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
	public void sendUpdateSecure(GuidEntry myGUIDInfo, 
			HashMap<String, List<ACLEntry>> aclmap, List<AnonymizedIDEntry> anonymizedIDList, 
			JSONObject attrValuePairs, long versionNum, boolean blocking);
	
	/**
	 * computes anonymized Ids for a user
	 * @param myGUIDInfo the calling user's guid info
	 * @param ACLArray the calling user's ACL
	 * @return
	 */
	public List<AnonymizedIDEntry> computeAnonymizedIDs(HashMap<String, List<ACLEntry>> aclMap);
	
	//FIXME: semantics needs to be decided, after secure update/insert is implemented
	public int sendSearchQuerySecure(GuidEntry myGUIDInfo, String searchQuery, JSONArray replyArray, long expiryTime);
	
	//FIXME: semantics needs to be decided, after secure update/insert is implemented
	public JSONObject sendGetRequestSecure(GuidEntry myGUIDInfo, String GUID) throws Exception;
}