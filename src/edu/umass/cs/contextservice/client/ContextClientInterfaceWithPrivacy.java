package edu.umass.cs.contextservice.client;

import java.util.HashMap;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.gnsclient.client.util.GuidEntry;

/**
 * Implements the method to be used in secure 
 * context service client, which implements the privacy scheme.
 * One thing to note: everything guid, public key and private key is represented as byt[]
 * to half the space requirements. Keys are of 100s of bytes so storing them as string 
 * double the space 200s of bytes.
 * @author adipc
 */
public interface ContextClientInterfaceWithPrivacy
{
	/**
	 * @param myGUIDInfo contains a user's GUID, public key, and private key
	 * @param ACLInfo Each element is JSONObject, each JSON contains attrName, ACL member's GUID and public key
	 * @param attrValuePairs attr value pairs for update
	 * @param versionNum 
	 * @param blocking true then the update will block until CS confirms completion.
	 */
	public void sendUpdateSecure(String GUID, GuidEntry myGUIDInfo, 
			JSONObject attrValuePairs, long versionNum,
			HashMap<String, List<ACLEntry>> aclmap, 
			List<AnonymizedIDEntry> anonymizedIDList );
	
	/**
	 * computes anonymized Ids for a user
	 * @param myGUIDInfo the calling user's guid info
	 * @param ACLArray the calling user's ACL
	 * @return
	 * @throws JSONException 
	 */
	public List<AnonymizedIDEntry> computeAnonymizedIDs(
			GuidEntry myGuidEntry, HashMap<String, List<ACLEntry>> aclMap, 
			boolean useSymmetricKeys) throws JSONException;
	
	//FIXME: semantics needs to be decided, after secure update/insert is implemented
	public int sendSearchQuerySecure(String searchQuery, JSONArray replyArray, 
			long expiryTime, GuidEntry myGUIDInfo);
	
	//FIXME: semantics needs to be decided, after secure update/insert is implemented
	public JSONObject sendGetRequestSecure(String GUID, GuidEntry myGUIDInfo) throws Exception;
}