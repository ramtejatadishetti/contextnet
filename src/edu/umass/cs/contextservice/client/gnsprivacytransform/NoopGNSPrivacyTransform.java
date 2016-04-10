package edu.umass.cs.contextservice.client.gnsprivacytransform;

import java.util.HashMap;
import java.util.List;

import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.gnsclient.client.GuidEntry;

/**
 * No operation transform doesn't do anything 
 * just copies the input values in output.
 * @author adipc
 *
 */
public class NoopGNSPrivacyTransform implements GNSPrivacyTransformInterface
{
	@Override
	public GNSTransformedMessage transformUpdateForGNSPrivacy(JSONObject attrValuePair,
			HashMap<String, List<ACLEntry>> aclMap) 
	{
		GNSTransformedMessage gnsTransformedMessage 
						= new GNSTransformedMessage(attrValuePair);
		return gnsTransformedMessage;
	}

	@Override
	public JSONObject unTransformGetReply(GNSTransformedMessage gnsTransformedMessage, 
			GuidEntry myGuidEntry) 
	{
		return gnsTransformedMessage.getEncryptedAttrValuePair();
	}
}