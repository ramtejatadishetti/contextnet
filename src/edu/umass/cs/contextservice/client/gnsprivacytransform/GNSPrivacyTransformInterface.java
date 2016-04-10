package edu.umass.cs.contextservice.client.gnsprivacytransform;

import java.util.HashMap;
import java.util.List;

import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.gnsclient.client.GuidEntry;

/**
 * This interface specifies the GNS privacy transform interface.
 * @author adipc
 *
 */
public interface GNSPrivacyTransformInterface 
{
	/**
	 * Takes <attr, value> pairs and aclMap as input and 
	 * returns <attr, encryptedValueJSON>.
	 * @param attrValuePair
	 * @param aclMap
	 * @return
	 */
	public GNSTransformedUpdateMessage transformUpdateForGNSPrivacy(
			JSONObject attrValuePair, HashMap<String, List<ACLEntry>> aclMap);
	
	/**
	 * untransforms the encrypted value, and returns a plain text attr-value pair.
	 * @param encryptedAttrValuePair
	 * @param myGuidEntry
	 * @return
	 */
	public JSONObject unTransformGetReply(GNSTransformedUpdateMessage gnsTransformedMessage, 
			GuidEntry myGuidEntry);
}