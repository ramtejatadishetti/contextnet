package edu.umass.cs.contextservice.client.csprivacytransform;

import org.json.JSONObject;

/**
 * This message stores  the anonymized ID, attrValuePairs,  
 * the mapping information to real GUID for each attribute 
 * associated to this anonymized ID.
 * @author adipc
 *
 */
public class CSTransformedMessage 
{	
	private final byte[] anonymizedID; 
	
	// attribute value pairs for this anonymizedID
	private final JSONObject attrValuePairs;
	

	//key to this JSON is the attributes associated with this anonymized ID
	// value is the encryption of read GUID with ACL member public keys.
	private final JSONObject realIDMappingInfo;
	
	
	public CSTransformedMessage(byte[] anonymizedID, JSONObject attrValuePairs, 
			JSONObject realIDMappingInfo)
	{
		this.anonymizedID = anonymizedID;
		this.attrValuePairs = attrValuePairs;
		this.realIDMappingInfo = realIDMappingInfo;
	}
	
	public byte[] getAnonymizedID()
	{
		return this.anonymizedID;
	}
	
	public JSONObject getAttrValuePairJSON()
	{
		return this.attrValuePairs;
	}
	
	public JSONObject getRealIDMappingInfoJSON()
	{
		return this.realIDMappingInfo;
	}
}