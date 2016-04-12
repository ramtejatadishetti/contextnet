package edu.umass.cs.contextservice.client.gnsprivacytransform;

import java.util.HashMap;
import java.util.List;

import org.json.JSONObject;

import edu.umass.cs.contextservice.client.common.ACLEntry;
import edu.umass.cs.gnsclient.client.GuidEntry;

public class EncryptionBasedGNSPrivacyTransform implements GNSPrivacyTransformInterface
{
	public static final String algorithm 			= "DESede";
	
	public EncryptionBasedGNSPrivacyTransform()
	{
	}
	
	@Override
	public GNSTransformedMessage transformUpdateForGNSPrivacy(JSONObject attrValuePair,
			HashMap<String, List<ACLEntry>> aclMap)
	{
		//Key symKey = KeyGenerator.getInstance(algorithm).generateKey();
		
		return null;
	}
	
	@Override
	public JSONObject unTransformGetReply(GNSTransformedMessage gnsTransformedMessage, GuidEntry myGuidEntry) 
	{
		return null;
	}
}