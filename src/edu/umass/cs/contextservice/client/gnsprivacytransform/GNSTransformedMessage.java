package edu.umass.cs.contextservice.client.gnsprivacytransform;

import org.json.JSONObject;

public class GNSTransformedMessage 
{
	// key is attrName, value is EncryptedValueJSON.
	private final JSONObject encryptedAttrValuePair;
	
	public GNSTransformedMessage(JSONObject encryptedAttrValuePair)
	{
		this.encryptedAttrValuePair = encryptedAttrValuePair;
	}
	
	public JSONObject getEncryptedAttrValuePair()
	{
		return this.encryptedAttrValuePair;
	}
}
