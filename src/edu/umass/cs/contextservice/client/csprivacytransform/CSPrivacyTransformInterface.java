package edu.umass.cs.contextservice.client.csprivacytransform;

import java.util.HashMap;
import java.util.List;

import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.AnonymizedIDEntry;
import edu.umass.cs.contextservice.client.common.ACLEntry;

/**
 * 
 * Defines the interface for CS privacy transform
 * @author adipc
 */
public interface CSPrivacyTransformInterface 
{
	/**
	 * This function transforms an update into privacy preserving updates to CS.
	 * A single update to a GUID can result in multiple anonymized Ids of that guid being updated,
	 * which is returned as a list.
	 * @param targetGuid
	 * @param attrValuePairs
	 * @param aclMap
	 * @param anonymizedIDList
	 * @param versionNum
	 * @return
	 */
	public List<CSTransformedMessage> transformUpdateForCSPrivacy(String targetGuid, 
			JSONObject attrValuePairs, HashMap<String, List<ACLEntry>> aclMap, 
			List<AnonymizedIDEntry> anonymizedIDList);
	
	/**
	 * untransforms the result of anonymized IDs obtained in a search reply.
	 * returns a lsit of GUIDs
	 * @param encryptedRealIDArray
	 * @return
	 */
	public List<String> unTransformSearchReply(List<CSTransformedMessage> csTransformedList);
}