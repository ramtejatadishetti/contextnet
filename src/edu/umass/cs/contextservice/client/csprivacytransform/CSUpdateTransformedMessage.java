package edu.umass.cs.contextservice.client.csprivacytransform;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.umass.cs.contextservice.utils.Utils;

/**
 * This message stores  the anonymized ID, attrValuePairs,  
 * the mapping information to real GUID for each attribute 
 * associated to this anonymized ID.
 * @author adipc
 */
public class CSUpdateTransformedMessage 
{
	private final byte[] anonymizedID; 
	private final JSONObject csAttrValPairs;

	private final JSONArray anonymizedIDToGuidMapping;
	
	public CSUpdateTransformedMessage( byte[] anonymizedID, 
			JSONObject csAttrValPairs, JSONArray anonymizedIDToGuidMapping )
	{
		this.anonymizedID = anonymizedID;
		this.csAttrValPairs = csAttrValPairs;
		this.anonymizedIDToGuidMapping = anonymizedIDToGuidMapping;
	}
	
	public byte[] getAnonymizedID()
	{
		return this.anonymizedID;
	}
	
	public JSONObject getAttrValJSON()
	{
		return this.csAttrValPairs;
	}
	
	public JSONArray getAnonymizedIDToGuidMapping()
	{
		return anonymizedIDToGuidMapping;
	}
	
	public String toString()
	{
		String str="anonymized ID "+Utils.bytArrayToHex(anonymizedID)+" csAttrValPairs "
					+csAttrValPairs;
		
		if( anonymizedIDToGuidMapping != null )
		{
			str = str+ "anonymizedIDToGuidMapping "+anonymizedIDToGuidMapping.toString();
		}
		return str;
	}
}